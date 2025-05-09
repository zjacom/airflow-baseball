from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from sqlalchemy import text
from utils.create_db_connection import get_sync_db_connection

# DAG 기본 인수 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

# wRC 계산 및 업데이트 함수
def _calculate_and_update_wRC():
    engine = get_sync_db_connection()
    
    # 리그 전체 통계 쿼리들
    select_league_runs_query = text("""
        SELECT (SUM(away_score) + SUM(home_score)) league_runs
        FROM game_records;
    """)

    select_league_pa_query = text("""
        SELECT SUM(pa) as league_pa
        FROM hitters;
    """)

    select_league_wOBA_query = text("""
        SELECT AVG(wOBA) as league_wOBA
        FROM hitter_metrics;
    """)

    select_league_OBP_query = text("""
        SELECT AVG(obp) as league_OBP
        FROM hitters;
    """)

    select_league_slg_query = text("""
        SELECT AVG(slg) as league_slg
        FROM hitters;
    """)

    select_hitter_wOBA_query = text("""
        SELECT hitter_id, wOBA
        FROM hitter_metrics;
    """)

    select_hitter_pa_query = text("""
        SELECT pa
        FROM hitters
        WHERE hitter_id = :hitter_id
    """)

    upsert_hitter_wRC_query = text("""
        INSERT INTO hitter_metrics (hitter_id, wRC)
        VALUES (:hitter_id, :wRC)
        ON DUPLICATE KEY UPDATE wRC = VALUES(wRC);
    """)
    
    # 리그 전체 통계 데이터 가져오기
    with engine.connect() as conn:
        select_league_runs_result = conn.execute(select_league_runs_query)
        league_runs = int(select_league_runs_result.scalar())

        select_league_pa_result = conn.execute(select_league_pa_query)
        league_pa = int(select_league_pa_result.scalar())

        select_league_wOBA_result = conn.execute(select_league_wOBA_query)
        league_wOBA = float(select_league_wOBA_result.scalar())

        select_league_OBP_result = conn.execute(select_league_OBP_query)
        league_OBP = float(select_league_OBP_result.scalar())

        select_league_slg_result = conn.execute(select_league_slg_query)
        league_slg = float(select_league_slg_result.scalar())

        select_hitter_wOBA_results = conn.execute(select_hitter_wOBA_query).fetchall()
    
    # wOBA 스케일 계산
    wOBA_scale = (league_wOBA - league_OBP) / (league_slg - league_OBP)
    
    # 각 타자별 wRC 계산 및 업데이트
    for row in select_hitter_wOBA_results:
        hitter_id, wOBA = row

        with engine.connect() as conn:
            select_hitter_pa_result = conn.execute(select_hitter_pa_query, {"hitter_id": hitter_id})
            pa = int(select_hitter_pa_result.scalar())

        # wRC 계산
        wRC = (((wOBA - league_wOBA) / wOBA_scale) + (league_runs / league_pa)) * pa

        # 데이터 업데이트
        data = {"hitter_id": hitter_id, "wRC": wRC}
        with engine.begin() as conn:
            conn.execute(upsert_hitter_wRC_query, data)

with DAG(
    dag_id='012_calculate_hitter_wRC',
    default_args=default_args,
    description='타자 wRC 계산하는 DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # Task 정의
    calculate_and_update_wRC = PythonOperator(
        task_id='calculate_and_update_wRC',
        python_callable=_calculate_and_update_wRC,
        dag=dag,
    )

    trigger_calculate_hitter_metrics_dag = TriggerDagRunOperator(
        task_id='trigger_calculate_hitter_metrics_dag',
        trigger_dag_id='013_calculate_hitter_metrics',
        wait_for_completion=False
    )

    # Task 의존성 설정
    calculate_and_update_wRC >> trigger_calculate_hitter_metrics_dag
