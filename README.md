# AviationStack BI Dashboard — GitHub-ready Project

This repository contains a ready-to-run BI project using the AviationStack Flights API (no airport enrichment). It includes:

* ETL script to pull flights and store as Parquet / SQLite
* Streamlit dashboard that reads ETL output and provides KPIs, charts, drilldowns
* Dockerfile, requirements, and run instructions

---

## File tree

```
aviationstack-bi/
├── README.md
├── .env.example
├── .gitignore
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── etl/
│   └── etl_aviationstack.py
├── app/
│   └── streamlit_dashboard.py
└── data/
    └── (flights.parquet will be written here)
```
---

## How to run locally

1. Clone the repo.
2. Copy `.env.example` → `.env` and set `AVIATIONSTACK_API_KEY`.
3. Create a Python virtualenv and install requirements: `python -m venv venv && source venv/bin/activate && pip install -r requirements.txt`.
4. Run ETL (small sample):

```bash
python etl/etl_aviationstack.py
```

This writes `data/flights.parquet` and `data/aviationstack.db`.

5. Run Streamlit:

```bash
streamlit run app/streamlit_dashboard.py
```

Open `http://localhost:8501`.

---

## Notes and caveats

* **API limits:** AviationStack API has rate limits & pagination. Adjust `ETL_MAX_PAGES` and `ETL_LIMIT` based on your API plan.
* **Data completeness:** Free plans might return limited or delayed data. For production use, consider a data warehouse & incremental loads.
* **Security:** Do not commit your `.env` or API key to source control. Use CI secrets for deployment.

---