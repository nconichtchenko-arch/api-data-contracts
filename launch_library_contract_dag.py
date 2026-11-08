from datetime import datetime, timedelta
import json
import requests
import yaml
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------------- configs
# Пути к контракту и валидатору (будут доступны внутри контейнера Airflow)
PROJECT_DIR = Path("/opt/airflow/dags/api-data-contracts") # или локальный путь
CONTRACT_PATH = PROJECT_DIR / "contracts" / "launch_library.yml"

# Настройки БД (из карты памяти: хост 10.0.2.2 для контейнеров)
DB_CONFIG = {
    "host": "10.0.2.2", # IP хоста Windows из VM
    "port": 5432,
    "dbname": "dwh_marketplace", # или создадим отдельную базу
    "user": "dwh_user",
    "password": "dwh_password",
}

API_URL = "https://ll.thespacedevs.com/2.2.0/launch/?limit=50"

# ---------------------------------------------------------------- tasks

def fetch_api_data(**kwargs):
    """Забирает данные из API и пушит их в XCom."""
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    # Сохраняем сырой JSON в файл (для отладки)
    raw_path = Path("/tmp") / "launch_library_raw.json"
    with raw_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        
    print(f"Fetched {data.get('count', 0)} total, {len(data.get('results', []))} in this batch")
    return data

def validate_data(**kwargs):
    """Берет данные из XCom и валидирует по контракту."""
    ti = kwargs['ti']
    api_data = ti.xcom_pull(task_ids='fetch_api_data')
    
    # Импортируем валидатор (он лежит в scripts/)
    import sys
    sys.path.append(str(PROJECT_DIR / "scripts"))
    from api_contract_validator import validate_api_response
    
    passed, errors = validate_api_response(api_data, CONTRACT_PATH)
    
    if not passed:
        error_msg = f"Data Contract Violation! {len(errors)} errors.\n" + "\n".join(errors[:5])
        raise AirflowFailException(error_msg)
    
    print("[OK] Contract passed")
    return True

def load_to_postgres(**kwargs):
    """Загружает данные в PostgreSQL (только если валидация прошла)."""
    ti = kwargs['ti']
    api_data = ti.xcom_pull(task_ids='fetch_api_data')
    
    records = []
    for item in api_data.get("results", []):
        # Извлекаем нужные поля
        status = item.get("status") or {}
        provider = item.get("launch_service_provider") or {}
        
        records.append((
            item.get("id"),
            item.get("name"),
            item.get("net"),
            item.get("probability"),
            item.get("hashtag"),
            status.get("abbrev"),
            status.get("id"),
            provider.get("name"),
            provider.get("type"),
            json.dumps(item) # сырой JSON на всякий случай
        ))
    
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # Создаем таблицу, если её нет
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.launch_library (
                    id VARCHAR(36) PRIMARY KEY,
                    name TEXT NOT NULL,
                    net TIMESTAMP NOT NULL,
                    probability FLOAT,
                    hashtag TEXT,
                    status_abbrev TEXT,
                    status_id INTEGER,
                    provider_name TEXT,
                    provider_type TEXT,
                    raw_json JSONB,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Вставляем данные (on conflict do nothing, чтобы не падать на дублях)
            execute_values(
                cur,
                """
                INSERT INTO public.launch_library 
                (id, name, net, probability, hashtag, status_abbrev, status_id, provider_name, provider_type, raw_json)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    probability = EXCLUDED.probability,
                    raw_json = EXCLUDED.raw_json,
                    loaded_at = CURRENT_TIMESTAMP
                """,
                records
            )
        conn.commit()
        print(f"[OK] Inserted/Updated {len(records)} records")
    finally:
        conn.close()

# ---------------------------------------------------------------- dag

default_args = {
    'owner': 'pavel.onishchenko',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['pavloni@mail.ru'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='launch_library_contract_pipeline',
    default_args=default_args,
    description='E2E pipeline: Launch Library API -> Contract Validation -> PostgreSQL',
    schedule_interval='@daily',
    start_date=datetime(2026, 11, 8),
    catchup=False,
    tags=['data-quality', 'contracts', 'api'],
) as dag:

    task_fetch = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
    )

    task_validate = PythonOperator(
        task_id='validate_contract',
        python_callable=validate_data,
    )

    task_load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    task_fetch >> task_validate >> task_load