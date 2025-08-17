from fastapi import FastAPI
import os, json, uuid, random
import psycopg2

app = FastAPI()

DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_NAME = os.environ["DB_NAME"]
DB_HOST = "/cloudsql/" + os.environ["INSTANCE_CONNECTION_NAME"]

SQL_QUERY = """
INSERT INTO public.vitals_events (
  event_id, patient_id, loinc_code, code_display,
  value_num, unit, effective_ts, source, raw
) VALUES (%s,%s,%s,%s,%s,%s,NOW(),%s,%s)
"""

def insert_row():
    with psycopg2.connect(user=DB_USER, password=DB_PASS, dbname=DB_NAME, host=DB_HOST) as conn:
        with conn.cursor() as cur:
            cur.execute(
                SQL_QUERY,
                (
                    str(uuid.uuid4()),
                    random.choice(["P001","P002","P003"]),
                    "8867-4",
                    "Heart rate",
                    round(random.uniform(60,100), 1),
                    "beats/min",
                    "synthetic",
                    json.dumps({"note": "fastapi demo"}),
                ),
            )

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/ingest")
def ingest():
    insert_row()
    return {"result": "inserted"}