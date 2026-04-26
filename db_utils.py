# db_utils.py
import sqlite3
import pandas as pd
import yaml

def load_config(path="config_erik.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def get_connection(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def load_table(conn, table):
    return pd.read_sql(f"SELECT * FROM {table}", conn)


def get_trip_export_query():
    return """
    SELECT
        t.trip_id,
        tp.people_id,
        p.name AS person_name,
        t.date AS trip_date,
        t.start_time AS trip_start_time,
        t.end_time AS trip_end_time,
        tpl.place_id,
        pl.name AS place_name,
        pl.lat,
        pl.lon,
        COALESCE(tp.time, tpl.time, t.date || ' ' || t.start_time) AS event_time
    FROM trips t
    LEFT JOIN trip_people tp ON t.trip_id = tp.trip_id
    LEFT JOIN people p ON tp.people_id = p.people_id
    LEFT JOIN trip_places tpl ON t.trip_id = tpl.trip_id
    LEFT JOIN places pl ON tpl.place_id = pl.place_id
    """
