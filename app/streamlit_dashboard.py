import os
from pathlib import Path
import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv


load_dotenv()
DATA_DIR = Path('data')
PARQUET_PATH = DATA_DIR / 'flights.parquet'
SQLITE_PATH = DATA_DIR / 'aviationstack.db'


st.set_page_config(page_title='Aviation BI Dashboard', layout='wide')


@st.cache_data
def load_data():
    if PARQUET_PATH.exists():
        df = pd.read_parquet(PARQUET_PATH)
    elif SQLITE_PATH.exists():
        conn = sqlite3.connect(SQLITE_PATH)
        df = pd.read_sql('SELECT * FROM fact_flights', conn, parse_dates=['dep_scheduled','dep_estimated','dep_actual','arr_scheduled','arr_estimated','arr_actual','live_updated'])
        conn.close()
    else:
        st.error('No data found. Run ETL first to create data/flights.parquet or data/aviationstack.db')
        st.stop()
    return df

df = load_data()


# Sidebar filters
st.sidebar.header('Filters')
min_date = pd.to_datetime(df['flight_date']).min().date()
max_date = pd.to_datetime(df['flight_date']).max().date()
date_range = st.sidebar.date_input('Flight date range', value=(min_date, max_date), min_value=min_date, max_value=max_date)


airlines = sorted(df['airline_name'].dropna().unique())
sel_airline = st.sidebar.selectbox('Airline', options=['All'] + airlines)


dep_iatas = sorted(df['dep_iata'].dropna().unique())
sel_dep = st.sidebar.selectbox('Departure Airport (IATA)', options=['All'] + list(dep_iatas))


arr_iatas = sorted(df['arr_iata'].dropna().unique())
sel_arr = st.sidebar.selectbox('Arrival Airport (IATA)', options=['All'] + list(arr_iatas))


delay_threshold = st.sidebar.slider('Delay threshold (mins) to count as delayed', 0, 240, 15)


# Filtering
start_date, end_date = date_range
mask = (pd.to_datetime(df['flight_date']) >= pd.to_datetime(start_date)) & (pd.to_datetime(df['flight_date']) <= pd.to_datetime(end_date))
filtered = df[mask].copy()
if sel_airline != 'All':
    filtered = filtered[filtered['airline_name'] == sel_airline]
if sel_dep != 'All':
    filtered = filtered[filtered['dep_iata'] == sel_dep]
if sel_arr != 'All':
    filtered = filtered[filtered['arr_iata'] == sel_arr]

# Executive KPIs
st.title('✈️ Aviation BI Dashboard')
col1, col2, col3, col4 = st.columns(4)

total_flights = len(filtered)
cancelled_pct = 100 * filtered['flight_status'].str.lower().eq('cancelled').sum() / (total_flights if total_flights else 1)
avg_arr_delay = filtered['arr_delay'].dropna()
avg_arr_delay = avg_arr_delay.mean() if not avg_arr_delay.empty else 0
on_time_pct = 100 * (filtered['arr_delay'].fillna(0) <= delay_threshold).sum() / (total_flights if total_flights else 1)


col1.metric('Total Flights', f"{total_flights:,}")
col2.metric('Cancelled %', f"{cancelled_pct:.2f}%")
col3.metric('Avg Arrival Delay (min)', f"{avg_arr_delay:.1f}")
col4.metric(f'On-time % (≤{delay_threshold}m)', f"{on_time_pct:.1f}%")


# Charts
st.markdown('## Flight Volume and Punctuality')
vol_fig = px.histogram(filtered, x='flight_date', title='Flights per Day', nbins=30)
st.plotly_chart(vol_fig, use_container_width=True)


st.markdown('### Delay by Airline')
delay_by_airline = filtered.groupby('airline_name')['arr_delay'].mean().reset_index().sort_values('arr_delay', ascending=False).dropna()
if not delay_by_airline.empty:
    fig_airline_delay = px.bar(delay_by_airline, x='arr_delay', y='airline_name', orientation='h', title='Average Arrival Delay by Airline')
st.plotly_chart(fig_airline_delay, use_container_width=True)