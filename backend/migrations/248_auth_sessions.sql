-- The database is the session authority. Old JWT/Redis-only sessions require
-- sign-in after this migration; no synthetic epoch or legacy-token bypass.
ALTER TABLE users ADD COLUMN IF NOT EXISTS session_epoch BIGINT NOT NULL DEFAULT 1;
ALTER TABLE users ADD CONSTRAINT users_session_epoch_positive CHECK (session_epoch > 0);

CREATE TABLE auth_sessions (
    sid VARCHAR(128) PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_epoch BIGINT NOT NULL CHECK (session_epoch > 0),
    token_version BIGINT NOT NULL,
    binding_hash TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    CHECK (expires_at > created_at)
);
CREATE INDEX auth_sessions_user ON auth_sessions(user_id, created_at DESC);
CREATE INDEX auth_sessions_expiry ON auth_sessions(expires_at);

CREATE TABLE auth_refresh_tokens (
    token_hash CHAR(64) PRIMARY KEY CHECK (token_hash ~ '^[0-9a-f]{64}$'),
    sid VARCHAR(128) NOT NULL REFERENCES auth_sessions(sid) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    replaced_by_hash CHAR(64),
    CHECK ((consumed_at IS NULL) = (replaced_by_hash IS NULL)),
    CHECK (replaced_by_hash IS NULL OR replaced_by_hash ~ '^[0-9a-f]{64}$')
);
CREATE INDEX auth_refresh_tokens_session ON auth_refresh_tokens(sid);
CREATE INDEX auth_refresh_tokens_expiry ON auth_refresh_tokens(expires_at);

CREATE FUNCTION advance_user_session_epoch() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.email IS DISTINCT FROM NEW.email
       OR OLD.password_hash IS DISTINCT FROM NEW.password_hash
       OR OLD.role IS DISTINCT FROM NEW.role
       OR OLD.status IS DISTINCT FROM NEW.status
       OR OLD.deleted_at IS DISTINCT FROM NEW.deleted_at
       OR ((OLD.totp_enabled OR NEW.totp_enabled)
           AND OLD.totp_secret_encrypted IS DISTINCT FROM NEW.totp_secret_encrypted)
       OR OLD.totp_enabled IS DISTINCT FROM NEW.totp_enabled THEN
        NEW.session_epoch := OLD.session_epoch + 1;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER users_session_security_change BEFORE UPDATE ON users
FOR EACH ROW EXECUTE FUNCTION advance_user_session_epoch();

-- Provider claims/metadata can be refreshed during ordinary login. Only changes
-- to the authenticating identity or its ownership invalidate existing sessions.
CREATE FUNCTION advance_identity_session_epoch() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.user_id IS NOT DISTINCT FROM NEW.user_id
       AND OLD.provider_type IS NOT DISTINCT FROM NEW.provider_type
       AND OLD.provider_key IS NOT DISTINCT FROM NEW.provider_key
       AND OLD.provider_subject IS NOT DISTINCT FROM NEW.provider_subject
       AND OLD.issuer IS NOT DISTINCT FROM NEW.issuer
       AND (OLD.verified_at IS NULL) = (NEW.verified_at IS NULL) THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        UPDATE users SET session_epoch = session_epoch + 1 WHERE id = OLD.user_id;
        RETURN OLD;
    END IF;
    -- Lock both owners in a stable order during identity reassignment.
    IF TG_OP = 'UPDATE' AND OLD.user_id <> NEW.user_id THEN
        PERFORM id FROM users WHERE id IN (OLD.user_id, NEW.user_id) ORDER BY id FOR UPDATE;
        UPDATE users SET session_epoch = session_epoch + 1 WHERE id = OLD.user_id;
    END IF;
    UPDATE users SET session_epoch = session_epoch + 1 WHERE id = NEW.user_id;
    RETURN NEW;
END;
$$;
CREATE TRIGGER auth_identity_session_security_change AFTER INSERT OR UPDATE OR DELETE ON auth_identities
FOR EACH ROW EXECUTE FUNCTION advance_identity_session_epoch();

CREATE FUNCTION advance_passkey_session_epoch() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.user_id IS NOT DISTINCT FROM NEW.user_id
       AND OLD.credential_id IS NOT DISTINCT FROM NEW.credential_id
       AND OLD.credential_data->'publicKey' IS NOT DISTINCT FROM NEW.credential_data->'publicKey'
       AND OLD.credential_data->'id' IS NOT DISTINCT FROM NEW.credential_data->'id' THEN
        -- Assertion counters, backup flags, display names and last-used metadata
        -- change during ordinary authentication and do not replace credentials.
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        UPDATE users SET session_epoch = session_epoch + 1 WHERE id = OLD.user_id;
        RETURN OLD;
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.user_id <> NEW.user_id THEN
        PERFORM id FROM users WHERE id IN (OLD.user_id, NEW.user_id) ORDER BY id FOR UPDATE;
        UPDATE users SET session_epoch = session_epoch + 1 WHERE id = OLD.user_id;
    END IF;
    UPDATE users SET session_epoch = session_epoch + 1 WHERE id = NEW.user_id;
    RETURN NEW;
END;
$$;
CREATE TRIGGER passkey_session_security_change AFTER INSERT OR UPDATE OR DELETE ON passkey_credentials
FOR EACH ROW EXECUTE FUNCTION advance_passkey_session_epoch();

CREATE FUNCTION advance_identity_channel_session_epoch() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    old_owner BIGINT;
    new_owner BIGINT;
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.identity_id IS NOT DISTINCT FROM NEW.identity_id
       AND OLD.provider_type IS NOT DISTINCT FROM NEW.provider_type
       AND OLD.provider_key IS NOT DISTINCT FROM NEW.provider_key
       AND OLD.channel IS NOT DISTINCT FROM NEW.channel
       AND OLD.channel_app_id IS NOT DISTINCT FROM NEW.channel_app_id
       AND OLD.channel_subject IS NOT DISTINCT FROM NEW.channel_subject THEN
        RETURN NEW;
    END IF;
    IF TG_OP <> 'INSERT' THEN
        SELECT user_id INTO old_owner FROM auth_identities WHERE id = OLD.identity_id;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        SELECT user_id INTO new_owner FROM auth_identities WHERE id = NEW.identity_id;
    END IF;
    -- An identity cascade may already have removed the parent; its own trigger
    -- advances the epoch. A user cascade can remove the owner altogether.
    PERFORM id FROM users WHERE id IN (old_owner,new_owner) ORDER BY id FOR UPDATE;
    UPDATE users SET session_epoch = session_epoch + 1 WHERE id IN (old_owner,new_owner);
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER auth_identity_channel_session_security_change AFTER INSERT OR UPDATE OR DELETE ON auth_identity_channels
FOR EACH ROW EXECUTE FUNCTION advance_identity_channel_session_epoch();

CREATE FUNCTION advance_passkey_handle_session_epoch() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.user_id IS NOT DISTINCT FROM NEW.user_id
       AND OLD.user_handle IS NOT DISTINCT FROM NEW.user_handle THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        UPDATE users SET session_epoch = session_epoch + 1 WHERE id = OLD.user_id;
        RETURN OLD;
    END IF;
    PERFORM id FROM users WHERE id IN (OLD.user_id,NEW.user_id) ORDER BY id FOR UPDATE;
    UPDATE users SET session_epoch = session_epoch + 1 WHERE id IN (OLD.user_id,NEW.user_id);
    RETURN NEW;
END;
$$;
CREATE TRIGGER passkey_handle_session_security_change AFTER UPDATE OR DELETE ON passkey_user_handles
FOR EACH ROW EXECUTE FUNCTION advance_passkey_handle_session_epoch();
