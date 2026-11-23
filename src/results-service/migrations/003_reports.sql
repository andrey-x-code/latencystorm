ALTER TABLE result_batches ADD COLUMN received_at timestamptz;
ALTER TABLE run_manifests ADD COLUMN report_state jsonb;

CREATE TABLE rejected_results (
    partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    run_id uuid,
    reason text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (partition, kafka_offset)
);

CREATE INDEX rejected_results_run ON rejected_results (run_id, received_at);
