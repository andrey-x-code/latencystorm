ALTER TABLE agents ADD COLUMN external_id text UNIQUE;
ALTER TABLE assignments ADD COLUMN safe_after timestamptz;
UPDATE agents SET state = 'offline';
