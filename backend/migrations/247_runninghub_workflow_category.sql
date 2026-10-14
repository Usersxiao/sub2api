-- Add category field to runninghub_workflows for grouping/filtering in the user-facing panel.
ALTER TABLE runninghub_workflows
  ADD COLUMN IF NOT EXISTS category VARCHAR(100) NOT NULL DEFAULT '';
