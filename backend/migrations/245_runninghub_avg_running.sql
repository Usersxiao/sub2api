-- Add avg_running_seconds to runninghub_workflows so CreateWorkflow can store
-- the value pulled from /api/webapp/detail and surface it in the admin UI.
ALTER TABLE runninghub_workflows
    ADD COLUMN IF NOT EXISTS avg_running_seconds INT NOT NULL DEFAULT 0;
