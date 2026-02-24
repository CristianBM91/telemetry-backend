import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise Exception("DATABASE_URL no definida")

def get_conn():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS devices (
        id SERIAL PRIMARY KEY,
        device_id TEXT UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS measurements (
        id SERIAL PRIMARY KEY,
        device_id INTEGER REFERENCES devices(id),
        timestamp BIGINT,
        timestamp_iso TIMESTAMP,
        accel_x DOUBLE PRECISION,
        accel_y DOUBLE PRECISION,
        accel_z DOUBLE PRECISION,
        gyro_x DOUBLE PRECISION,
        gyro_y DOUBLE PRECISION,
        gyro_z DOUBLE PRECISION,
        mag_x DOUBLE PRECISION,
        mag_y DOUBLE PRECISION,
        mag_z DOUBLE PRECISION,
        light DOUBLE PRECISION,
        lat DOUBLE PRECISION,
        long DOUBLE PRECISION,
        speed DOUBLE PRECISION,
        mic_level DOUBLE PRECISION,
        pressure DOUBLE PRECISION
    );
    """)

    conn.commit()
    cur.close()
    conn.close()


@app.on_event("startup")
def startup():
    init_db()


def get_or_create_device(device_external_id: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM devices WHERE device_id = %s;", (device_external_id,))
    result = cur.fetchone()

    if result:
        device_id = result[0]
    else:
        cur.execute(
            "INSERT INTO devices (device_id) VALUES (%s) RETURNING id;",
            (device_external_id,)
        )
        device_id = cur.fetchone()[0]
        conn.commit()

    cur.close()
    conn.close()
    return device_id


@app.post("/ingest")
def ingest(payload: Dict[str, Any]):

    try:
        device_external_id = payload["metadata"]["deviceId"]
        records = payload["records"]
    except KeyError:
        raise HTTPException(status_code=400, detail="Formato JSON inválido")

    device_id = get_or_create_device(device_external_id)

    conn = get_conn()
    cur = conn.cursor()

    rows = []

    for record in records:
        data = record["data"]

        rows.append((
            device_id,
            record.get("timestamp"),
            record.get("timestampISO"),
            data["accelerometer"]["x"],
            data["accelerometer"]["y"],
            data["accelerometer"]["z"],
            data["gyroscope"]["x"],
            data["gyroscope"]["y"],
            data["gyroscope"]["z"],
            data["magnetometer"]["x"],
            data["magnetometer"]["y"],
            data["magnetometer"]["z"],
            data.get("light"),
            data["location"]["lat"],
            data["location"]["long"],
            data["location"]["speed"],
            data.get("micLevel"),
            data["barometer"]["pressure"]
        ))

    execute_batch(cur, """
        INSERT INTO measurements (
            device_id, timestamp, timestamp_iso,
            accel_x, accel_y, accel_z,
            gyro_x, gyro_y, gyro_z,
            mag_x, mag_y, mag_z,
            light, lat, long, speed,
            mic_level, pressure
        )
        VALUES (
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,%s,
            %s,%s
        );
    """, rows)

    conn.commit()
    cur.close()
    conn.close()

    return {
        "status": "ok",
        "inserted": len(rows),
        "device": device_external_id
    }


@app.get("/health")
def health():
    return {"status": "alive"}


@app.get("/devices")
def list_devices():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT device_id, created_at FROM devices;")
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return [{"device_id": r[0], "created_at": r[1]} for r in rows]


@app.get("/last/{device_id}")
def get_last_measurement(device_id: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            timestamp_iso,
            accel_x, accel_y, accel_z,
            gyro_x, gyro_y, gyro_z,
            mag_x, mag_y, mag_z,
            light,
            latitude, longitude, speed,
            pressure
        FROM measurements
        WHERE device_id = %s
        ORDER BY timestamp DESC
        LIMIT 1;
    """, (device_id,))

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return {"error": "No data found"}

    return {
        "timestamp": row[0],
        "accelerometer": {
            "x": row[1],
            "y": row[2],
            "z": row[3],
        },
        "gyroscope": {
            "x": row[4],
            "y": row[5],
            "z": row[6],
        },
        "magnetometer": {
            "x": row[7],
            "y": row[8],
            "z": row[9],
        },
        "light": row[10],
        "location": {
            "lat": row[11],
            "long": row[12],
            "speed": row[13],
        },
        "pressure": row[14]
    }