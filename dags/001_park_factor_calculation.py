from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

import pandas as pd
import pandasql as ps
from sqlalchemy import text

from utils.create_db_connection import get_sync_db_connection

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

def _initialize_table():
    engine = get_sync_db_connection()
    
    drop_table_query = text("""
        DROP TABLE IF EXISTS park_factor;
    """)
    
    create_table_query = text("""
        CREATE TABLE IF NOT EXISTS park_factor (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stadium VARCHAR(50) NOT NULL,
            park_factor FLOAT NOT NULL
        );
    """)
    
    with engine.connect() as conn:
        conn.execute(drop_table_query)
        conn.execute(create_table_query)

def _calculate_and_insert_park_factors():
    engine = get_sync_db_connection()
    
    # game_schedule 데이터 읽기
    df = pd.read_sql("SELECT * FROM game_records", engine)
    
    stats_case_by_stadium = df.groupby('stadium').agg(
        scored=('home_score', 'sum'),
        allowed_score=('away_score', 'sum'),
        games=('id', 'count')
    ).reset_index()
    
    for _, row in stats_case_by_stadium.iterrows():
        cur_stadium = row["stadium"]
        cur_scored = row["scored"]
        cur_allowed_score = row["allowed_score"]
        cur_games = row["games"]

        # SQL 쿼리 작성
        query = f"""
            SELECT
                SUM(scored) AS home_score_sum,
                SUM(allowed_score) AS away_score_sum,
                SUM(games) AS game_count
            FROM stats_case_by_stadium
            WHERE stadium != '{cur_stadium}'
        """

        # SQL 실행
        result = ps.sqldf(query, locals())

        # 결과를 파이썬 변수에 저장
        other_scored = result.at[0, 'home_score_sum']
        other_allowed_score = result.at[0, 'away_score_sum']
        other_games = result.at[0, 'game_count']

        park_factor = ((cur_scored + cur_allowed_score) / cur_games) / ((other_scored + other_allowed_score) / other_games)
        
        insert_query = text("""
            INSERT INTO park_factor (stadium, park_factor)
            VALUES (:stadium, :park_factor);
        """)

        # 트랜잭션 필요 - begin()
        with engine.begin() as conn:
            conn.execute(insert_query, {"stadium": cur_stadium, "park_factor": park_factor})

# ---------------------- DAG 정의 ----------------------

with DAG(
    dag_id='001_park_factor_calculation',
    default_args=default_args,
    description='파크 팩터를 계산하는 DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # Task 정의
    initialize_table = PythonOperator(
        task_id='initialize_table',
        python_callable=_initialize_table,
        dag=dag,
    )

    calculate_and_insert_park_factors = PythonOperator(
        task_id='calculate_and_insert_park_factors',
        python_callable=_calculate_and_insert_park_factors,
        dag=dag,
    )

    trigger_scrape_hitters_stats_dag = TriggerDagRunOperator(
        task_id='trigger_scrape_hitters_stats_dag',
        trigger_dag_id='010_scrape_hitters_stats',
        wait_for_completion=False
    )

    # Task 의존성 설정
    initialize_table >> calculate_and_insert_park_factors