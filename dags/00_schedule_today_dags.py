from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import pandasql as ps
from sqlalchemy import create_engine, text
from airflow.models import DagRun
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta, time
from airflow.api.common.experimental.trigger_dag import trigger_dag

from pytz import timezone as pytz_timezone

kst = pytz_timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'schedule_dag',
    default_args=default_args,
    description='야구장별 Park Factor 계산 및 저장',
    schedule_interval=None,
    start_date=datetime(2025, 4, 18),
    catchup=False,
)

def get_db_connection():
    db_config = {
        'user': 'niscom',
        'password': 'niscom',
        'host': '116.37.91.221',
        'port': 3306,
        'database': 'baseball'
    }
    
    engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
                        f"{db_config['host']}:{db_config['port']}/{db_config['database']}")
    return engine


def schedule_dag():
    engine = get_db_connection()
    query = text("""
        SELECT game_date
        FROM today_games
    """)

    with engine.connect() as conn:
        results = conn.execute(query).fetchall()

    if results:
        for time in results:
            exec_time = datetime.combine(datetime.today(), time[0].time())
            exec_time -= timedelta(minutes=15)  # ⏰ 15분 빼기
            aware_dt = kst.localize(exec_time)
            schedule_dag_run('baseball_lineup_scraper', aware_dt)


def schedule_dag_run(dag_id, execution_time):
    existing_dag_run = DagRun.find(dag_id=dag_id, execution_date=execution_time)
    if existing_dag_run:
        print(f"{dag_id}가 이미 {execution_time}에 예약되어 있습니다.")
        return

    print(f"{dag_id}를(을) {execution_time}에 예약했습니다.")
    trigger_dag(dag_id=dag_id, run_id=f'scheduled__{execution_time.isoformat()}', execution_date=execution_time, replace_microseconds=False)



# Task 정의
schedule_dag = PythonOperator(
    task_id='schedule_dag',
    python_callable=schedule_dag,
    dag=dag,
)

# Task 의존성 설정
schedule_dag