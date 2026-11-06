-- AutoDesign editing-template publication and review state.
-- Binary artifacts stay behind a replaceable object-store interface; only
-- immutable object metadata and review state are persisted in PostgreSQL.

CREATE TABLE IF NOT EXISTS editing_templates (
    id UUID PRIMARY KEY,
    owner_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title VARCHAR(80) NOT NULL,
    description VARCHAR(1000) NOT NULL DEFAULT '',
    editing_brief TEXT NOT NULL DEFAULT '',
    aspect_ratio VARCHAR(32) NOT NULL DEFAULT '',
    duration_ms BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'draft',
    rejection_reason TEXT NOT NULL DEFAULT '',
    moderation_action VARCHAR(64) NOT NULL DEFAULT 'allow',
    submitted_at TIMESTAMPTZ,
    reviewed_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    rejected_at TIMESTAMPTZ,
    unlisted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT editing_templates_title_not_blank CHECK (length(btrim(title)) > 0),
    CONSTRAINT editing_templates_duration_nonnegative CHECK (duration_ms >= 0),
    CONSTRAINT editing_templates_status_check
        CHECK (status IN ('draft', 'pending_review', 'approved', 'rejected', 'unlisted'))
);

CREATE INDEX IF NOT EXISTS idx_editing_templates_owner_updated
    ON editing_templates(owner_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_editing_templates_review_queue
    ON editing_templates(status, submitted_at ASC, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_editing_templates_public_catalog
    ON editing_templates(published_at DESC, id)
    WHERE status = 'approved';

CREATE TABLE IF NOT EXISTS editing_template_artifacts (
    id BIGSERIAL PRIMARY KEY,
    template_id UUID NOT NULL REFERENCES editing_templates(id) ON DELETE CASCADE,
    kind VARCHAR(32) NOT NULL,
    storage_key TEXT NOT NULL UNIQUE,
    original_filename TEXT NOT NULL,
    content_type VARCHAR(160) NOT NULL,
    byte_size BIGINT NOT NULL,
    sha256 VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT editing_template_artifacts_kind_check
        CHECK (kind IN ('project', 'cover', 'preview')),
    CONSTRAINT editing_template_artifacts_byte_size_positive CHECK (byte_size > 0),
    CONSTRAINT editing_template_artifacts_template_kind_unique UNIQUE (template_id, kind)
);

CREATE INDEX IF NOT EXISTS idx_editing_template_artifacts_template
    ON editing_template_artifacts(template_id);

CREATE TABLE IF NOT EXISTS editing_template_reviews (
    id BIGSERIAL PRIMARY KEY,
    template_id UUID NOT NULL REFERENCES editing_templates(id) ON DELETE CASCADE,
    actor_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
    action VARCHAR(32) NOT NULL,
    from_status VARCHAR(32) NOT NULL,
    to_status VARCHAR(32) NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT editing_template_reviews_action_check
        CHECK (action IN ('submit', 'approve', 'reject', 'unlist'))
);

CREATE INDEX IF NOT EXISTS idx_editing_template_reviews_template_created
    ON editing_template_reviews(template_id, created_at DESC, id DESC);
