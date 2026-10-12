from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pyarrow.parquet as pq

DEFAULT_ARGS = {
    'owner': 'Afiq Hilmy',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

def extract_currency_data(**context):
    """Fetch a rolling 30-day history of exchange rates with MYR as the base currency."""
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    url = f"https://api.frankfurter.app/{start_date}..{end_date}"
    params = {
        "from": "MYR",
        "to": "USD,GBP,SGD,JPY,AUD,IDR,THB"
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    if "rates" not in data:
        raise ValueError(f"Unexpected API response structure: {data}")
    
    base_currency = data['base']
    rates_by_date = data['rates']
    
    # Flatten the date-keyed nested dictionary into individual rows
    rows = []
    for rate_date, currencies in rates_by_date.items():
        for target_currency, rate in currencies.items():
            rows.append({
                'rate_date': rate_date,
                'base_currency': base_currency,
                'target_currency': target_currency,
                'exchange_rate': rate
            })
            
    df = pd.DataFrame(rows)
    
    # Save to staging parquet file in /tmp/
    df.to_parquet("/tmp/currency_cleaned.parquet", index=False)
    print(f"Extracted {len(df)} historical MYR exchange rate records from {start_date} to {end_date}.")

def load_currency_data(**context):
    """Load MYR-based rates into MySQL (finance_db) with smart duplicate filtering."""
    table = pq.read_table("/tmp/currency_cleaned.parquet")
    df = table.to_pandas()
    
    engine = create_engine("mysql+pymysql://airflow_user:Afiq-2004@localhost:3306/finance_db")
    
    # Check existing records to prevent composite key collisions (rate_date + target_currency)
    try:
        existing_df = pd.read_sql("SELECT rate_date, target_currency FROM currency_rates", con=engine)
    except Exception:
        existing_df = pd.DataFrame(columns=['rate_date', 'target_currency'])
        
    if not existing_df.empty and not df.empty:
        df['rate_date'] = pd.to_datetime(df['rate_date']).dt.date
        existing_df['rate_date'] = pd.to_datetime(existing_df['rate_date']).dt.date
        
        # Apply anti-join filter to keep only brand-new daily entries
        merged = df.merge(existing_df, on=['rate_date', 'target_currency'], how='left', indicator=True)
        df_filtered = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    else:
        df_filtered = df
        
    if not df_filtered.empty:
        df_filtered.to_sql('currency_rates', con=engine, if_exists='append', index=False)
        print(f"Successfully loaded {len(df_filtered)} new MYR exchange rate rows into finance_db.")
    else:
        print("No new exchange rates to insert. All daily records already exist.")

def export_currency_csv(**context):
    """Export the complete finance_db table into a flat CSV file for local inspection."""
    engine = create_engine("mysql+pymysql://airflow_user:Afiq-2004@localhost:3306/finance_db")
    df = pd.read_sql("SELECT * FROM currency_rates ORDER BY rate_date DESC", con=engine)
    
    output_path = "/home/lenovo/airflow/dags/currency_rates_export.csv"
    df.to_csv(output_path, index=False)
    print(f"Exported {len(df)} rows to flat file: {output_path}")

# Define the automated DAG schedule
with DAG(
    dag_id='currency_exchange_etl_pipeline',
    default_args=DEFAULT_ARGS,
    schedule='@daily',
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='extract_currency_data',
        python_callable=extract_currency_data,
    )

    t2 = PythonOperator(
        task_id='load_currency_data',
        python_callable=load_currency_data,
    )

    t3 = PythonOperator(
        task_id='export_currency_csv',
        python_callable=export_currency_csv,
    )

    t1 >> t2 >> t3