CREATE TABLE run_manifests (
    run_id uuid PRIMARY KEY,
    payload jsonb NOT NULL,
    finalization jsonb
);

CREATE TABLE result_batches (
    run_id uuid NOT NULL REFERENCES run_manifests(run_id),
    assignment_id uuid NOT NULL,
    sequence numeric(20, 0) NOT NULL CHECK (sequence > 0),
    payload jsonb NOT NULL,
    PRIMARY KEY (run_id, assignment_id, sequence)
);
