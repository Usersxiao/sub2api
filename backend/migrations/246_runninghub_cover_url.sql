-- Add cover_url to runninghub_workflows so the admin backend can store the
-- workflow's thumbnail image (pulled from RunningHub on import, overridable).
ALTER TABLE runninghub_workflows
    ADD COLUMN IF NOT EXISTS cover_url TEXT NOT NULL DEFAULT '';
