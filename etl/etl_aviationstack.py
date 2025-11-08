"""
Simple ETL for AviationStack Flights endpoint.
- Reads API key from environment variable AVIATIONSTACK_API_KEY
- Filters for multiple airlines: Garuda Indonesia (GA), ANA (NH), Emirates (EK), Turkish Airlines (TK)
- Writes `data/flights.parquet` and `data/aviationstack.db` SQLite table `fact_flights`


Adjust `max_pages` and `limit` either via env vars or CLI args.
"""
import os
import time
import requests
import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv


load_dotenv()


API_KEY = os.getenv('AVIATIONSTACK_API_KEY')
if not API_KEY:
    raise RuntimeError('AVIATIONSTACK_API_KEY environment variable not set. See .env.example')


BASE_URL = 'http://api.aviationstack.com/v1/flights'
OUT_DIR = Path('data')
OUT_DIR.mkdir(exist_ok=True)
PARQUET_PATH = OUT_DIR / 'flights.parquet'
SQLITE_PATH = OUT_DIR / 'aviationstack.db'

def call_api(params):
    params['access_key'] = API_KEY
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def flatten_flight(f):
    def get(d, *keys):
        x = d
        try:
            for k in keys:
                x = x[k]
            return x
        except Exception:
            return None

    row = {}
    row['flight_date'] = f.get('flight_date')
    row['flight_status'] = f.get('flight_status')


    # airline
    row['airline_name'] = get(f, 'airline', 'name')
    row['airline_iata'] = get(f, 'airline', 'iata')
    row['airline_icao'] = get(f, 'airline', 'icao')


    # flight
    row['flight_number'] = get(f, 'flight', 'number')
    row['flight_iata'] = get(f, 'flight', 'iata')
    row['flight_icao'] = get(f, 'flight', 'icao')


    # departure
    row['dep_airport'] = get(f, 'departure', 'airport')
    row['dep_iata'] = get(f, 'departure', 'iata')
    row['dep_icao'] = get(f, 'departure', 'icao')
    row['dep_timezone'] = get(f, 'departure', 'timezone')
    row['dep_terminal'] = get(f, 'departure', 'terminal')
    row['dep_gate'] = get(f, 'departure', 'gate')
    row['dep_delay'] = get(f, 'departure', 'delay')
    row['dep_scheduled'] = get(f, 'departure', 'scheduled')
    row['dep_estimated'] = get(f, 'departure', 'estimated')
    row['dep_actual'] = get(f, 'departure', 'actual')

    # arrival
    row['arr_airport'] = get(f, 'arrival', 'airport')
    row['arr_iata'] = get(f, 'arrival', 'iata')
    row['arr_icao'] = get(f, 'arrival', 'icao')
    row['arr_timezone'] = get(f, 'arrival', 'timezone')
    row['arr_terminal'] = get(f, 'arrival', 'terminal')
    row['arr_gate'] = get(f, 'arrival', 'gate')
    row['arr_baggage'] = get(f, 'arrival', 'baggage')
    row['arr_delay'] = get(f, 'arrival', 'delay')
    row['arr_scheduled'] = get(f, 'arrival', 'scheduled')
    row['arr_estimated'] = get(f, 'arrival', 'estimated')
    row['arr_actual'] = get(f, 'arrival', 'actual')


    # aircraft
    row['aircraft_registration'] = get(f, 'aircraft', 'registration')
    row['aircraft_iata'] = get(f, 'aircraft', 'iata')
    row['aircraft_icao'] = get(f, 'aircraft', 'icao')
    row['aircraft_icao24'] = get(f, 'aircraft', 'icao24')


    # live
    row['live_updated'] = get(f, 'live', 'updated')
    row['live_latitude'] = get(f, 'live', 'latitude')
    row['live_longitude'] = get(f, 'live', 'longitude')
    row['live_altitude'] = get(f, 'live', 'altitude')
    row['live_direction'] = get(f, 'live', 'direction')
    row['live_speed_horizontal'] = get(f, 'live', 'speed_horizontal')
    row['live_speed_vertical'] = get(f, 'live', 'speed_vertical')
    row['live_is_ground'] = get(f, 'live', 'is_ground')


    return row

def fetch_and_store(max_pages=5, limit=100, sleep_sec=1):
    # Airlines to fetch: Garuda Indonesia, ANA, Emirates, Turkish Airlines
    airlines = [
        {'iata': 'GA', 'name': 'Garuda Indonesia'},
        {'iata': 'NH', 'name': 'ANA'},
        {'iata': 'EK', 'name': 'Emirates'},
        {'iata': 'TK', 'name': 'Turkish Airlines'}
    ]
    
    all_rows = []
    
    for airline in airlines:
        print(f"\nFetching data for {airline['name']} ({airline['iata']})...")
        offset = 0
        airline_rows = []
        
        for page in range(max_pages):
            params = {'limit': limit, 'offset': offset, 'airline_iata': airline['iata']}
            js = call_api(params)
            data = js.get('data', [])
            for f in data:
                airline_rows.append(flatten_flight(f))
            count = len(data)
            print(f"  Page {page+1}: {count} records (offset={offset})")
            if count < limit:
                break
            offset += limit
            time.sleep(sleep_sec)
        
        all_rows.extend(airline_rows)
        print(f"  Total records for {airline['name']}: {len(airline_rows)}")

    df = pd.DataFrame(all_rows)

    # Convert datetime-like columns
    dt_cols = ['dep_scheduled','dep_estimated','dep_actual','arr_scheduled','arr_estimated','arr_actual','live_updated']
    for c in dt_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors='coerce')

    # Compute derived columns
    df['flight_date'] = pd.to_datetime(df['flight_date'], errors='coerce').dt.date
    df['dep_delay'] = pd.to_numeric(df['dep_delay'], errors='coerce')
    df['arr_delay'] = pd.to_numeric(df['arr_delay'], errors='coerce')
    df['is_on_time'] = df['arr_delay'].apply(lambda x: 1 if pd.notna(x) and x <= 15 else 0)


    # Save
    df.to_parquet(PARQUET_PATH, index=False)
    print(f"Saved {PARQUET_PATH}")


    conn = sqlite3.connect(SQLITE_PATH)
    df.to_sql('fact_flights', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Saved to {SQLITE_PATH} table fact_flights")

if __name__ == '__main__':
    max_pages = int(os.getenv('ETL_MAX_PAGES', 5))
    limit = int(os.getenv('ETL_LIMIT', 100))
    fetch_and_store(max_pages=max_pages, limit=limit)