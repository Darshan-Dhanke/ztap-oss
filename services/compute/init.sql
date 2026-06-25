-- Sample data for the dedicated "compute" Postgres that the connection proxy
-- suspends and resumes. This node is intentionally separate from the platform
-- Postgres (which is always-on), so the proxy can really stop/start it to
-- demonstrate scale-to-zero without disrupting the rest of the stack.
CREATE TABLE IF NOT EXISTS metrics (
    id         bigserial PRIMARY KEY,
    name       text NOT NULL,
    value      numeric(12,2) NOT NULL,
    recorded_at timestamptz DEFAULT now()
);

INSERT INTO metrics (name, value) VALUES
    ('cpu_pct', 12.50),
    ('mem_pct', 48.20),
    ('req_per_s', 1043.00);
