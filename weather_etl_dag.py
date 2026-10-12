import json
import requests
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, explode, arrays_zip

# -------------------------------------------------------------------
# 1. EXTRACT TASK
# -------------------------------------------------------------------
def extract_weather_data():
    import requests
import pandas as pd

def extract_weather_data():
    """Extract strictly historical weather data (no forecasts) for Kuala Lumpur."""
    url = "https://api.open-meteo.com/v1/forecast"
    
    params = {
        "latitude": 3.139,
        "longitude": 101.686,
        "hourly": "temperature_2m,rain,precipitation,cloud_cover,visibility,uv_index",
        "timezone": "Asia/Singapore",
        "past_days": 7,
        "forecast_days": 0
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    # Guard clause: ensure we received a dictionary back from the API
    if not isinstance(data, dict):
        raise ValueError(f"Expected dictionary from API, but got {type(data)}. Response: {data}")
    
    # Check if Open-Meteo returned an error message
    if data.get("error"):
        raise ValueError(f"Open-Meteo API Error: {data.get('reason', 'Unknown error')}")
    
    # Extract the hourly block safely
    hourly = data['hourly']
    df = pd.DataFrame({
        'weather_timestamp': hourly['time'],
        'temperature': hourly['temperature_2m'],
        'rain': hourly['rain'],
        'precipitation': hourly['precipitation'],
        'cloud_cover': hourly['cloud_cover'],
        'visibility': hourly['visibility'],
        'uv_index': hourly['uv_index']
    })
    
    # Save to staging parquet file
    df.to_parquet("/tmp/weather_cleaned.parquet", index=False)
    print(f"Extracted {len(df)} historical weather rows successfully.")

# -------------------------------------------------------------------
# 2. TRANSFORM TASK
# -------------------------------------------------------------------
def transform_weather_data():
    """Process raw JSON using PySpark to clean, unpack, and structure the expanded weather metrics."""
    spark = SparkSession.builder \
        .appName("MalaysiaWeatherETL") \
        .master("local[*]") \
        .getOrCreate()
    
    # Read raw JSON
    raw_df = spark.read.option("multiline", "true").json('/tmp/weather_raw.json')
    
    # Explode parallel arrays into tabular rows matching your new URL parameters
    hourly_df = raw_df.select(
        explode(
            arrays_zip(
                "hourly.time",
                "hourly.temperature_2m",
                "hourly.rain",
                "hourly.precipitation",
                "hourly.cloud_cover",
                "hourly.visibility",
                "hourly.uv_index"
            )
        ).alias("data")
    ).select(
        col("data.time").alias("weather_timestamp"),
        col("data.temperature_2m").cast("float").alias("temperature"),
        col("data.rain").cast("float").alias("rain"),
        col("data.precipitation").cast("float").alias("precipitation"),
        col("data.cloud_cover").cast("float").alias("cloud_cover"),
        col("data.visibility").cast("float").alias("visibility"),
        col("data.uv_index").cast("float").alias("uv_index")
    )
    
    # Filter out null timestamps and cast format
    cleaned_df = hourly_df.filter(col("weather_timestamp").isNotNull()) \
        .withColumn("weather_timestamp", to_timestamp("weather_timestamp"))
    
    # Save cleaned result as Parquet
    cleaned_df.write.mode("overwrite").parquet("/tmp/weather_cleaned.parquet")
    spark.stop()
    print("Successfully transformed expanded weather data with PySpark.")

# -------------------------------------------------------------------
# 3. LOAD TASK
# -------------------------------------------------------------------
def load_weather_data():
    """Load transformed Parquet data into local MySQL database."""
    # Read parquet into pandas
    table = pq.read_table("/tmp/weather_cleaned.parquet")
    df = table.to_pandas()
       # Connect to MySQL (Update 'your_password' with your actual local MySQL root password)
    engine = create_engine("mysql+pymysql://airflow_user:Afiq-2004@localhost:3306/weather_db")
    
   # Fetch existing timestamps from MySQL
    try:
        existing_df = pd.read_sql("SELECT weather_timestamp FROM kl_weather", con=engine)
    except Exception:
        existing_df = pd.DataFrame(columns=['weather_timestamp'])

    # Filter out rows that already exist in the database
    if not existing_df.empty and not df.empty:
        df['weather_timestamp'] = pd.to_datetime(df['weather_timestamp'])
        existing_df['weather_timestamp'] = pd.to_datetime(existing_df['weather_timestamp'])

        df_filtered = df[~df['weather_timestamp'].isin(existing_df['weather_timestamp'])]
    else:
        df_filtered = df

    # Append only new unique records
    if not df_filtered.empty:
        df_filtered.to_sql('kl_weather', con=engine, if_exists='append', index=False)
        print(f"Successfully loaded {len(df_filtered)} new rows into MySQL.")
    else:
        print("No new unique records to insert. Skipping.")

# -------------------------------------------------------------------
# 4. AIRFLOW DAG DEFINITION
# -------------------------------------------------------------------
default_args = {
    'owner': 'Afiq Hilmy',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='kl_weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Malaysia weather, specifically Kuala Lumpur data using PySpark and MySQL',
    schedule='@daily',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data,
    )

    transform_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data,
    )

    load_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data,
    )

    # Set task dependency sequence
    extract_task >> transform_task >> load_task
