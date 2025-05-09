from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import text
from airflow.models import DagRun
from datetime import datetime, timedelta
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.utils.dates import days_ago

from utils.create_db_connection import get_sync_db_connection

from pytz import timezone as pytz_timezone
kst = pytz_timezone("Asia/Seoul")

default_args = {
    'owner': 'niscom',
    'depends_on_past': False,
}

def get_today_games_time_from_db():
    engine = get_sync_db_connection()
    get_today_games_time_query = text("""
        SELECT game_date
        FROM today_games
    """)

    with engine.connect() as conn:
        times = conn.execute(get_today_games_time_query).fetchall()
    return times

def _schedule_dag():
    times = get_today_games_time_from_db()

    if not times:
        return

    for time in times:
        game_time = time[0]  # 이미 datetime.datetime 객체
        print("원래 경기 시작 시간:", game_time)

        # 15분 전 계산
        exec_time = game_time - timedelta(minutes=15)

        # 타임존 지정
        if exec_time.tzinfo is None:
            aware_dt = kst.localize(exec_time)
        else:
            aware_dt = exec_time.astimezone(kst)
        trigger_lineup_scrapper_dag(dag_id='040_scrape_today_lineup', execution_time=aware_dt)

def trigger_lineup_scrapper_dag(dag_id, execution_time):
    existing_dag_run = DagRun.find(dag_id=dag_id, execution_date=execution_time)
    if existing_dag_run:
        print(f"{dag_id}가 이미 {execution_time}에 예약되어 있습니다.")
        return

    print(f"{dag_id}를(을) {execution_time}에 예약했습니다.")
    trigger_dag(dag_id=dag_id, run_id=f'scheduled__{execution_time.isoformat()}', execution_date=execution_time, replace_microseconds=False)

# ---------------------- DAG 정의 ----------------------

with DAG(
    dag_id='031_schedule_dag_for_get_today_line_up',
    default_args=default_args,
    description='오늘 날짜의 라인업을 가져오는 DAG 스케줄링',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    schedule_dag = PythonOperator(
        task_id='schedule_dag',
        python_callable=_schedule_dag,
    )