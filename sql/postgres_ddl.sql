CREATE TABLE IF NOT EXISTS public.vitals_events (
  event_id UUID PRIMARY KEY,
  patient_id TEXT NOT NULL,
  loinc_code TEXT NOT NULL,
  code_display TEXT,
  value_num DOUBLE PRECISION,
  unit TEXT,
  effective_ts TIMESTAMP NOT NULL,
  source TEXT,
  raw TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);