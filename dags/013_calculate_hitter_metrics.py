from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from sqlalchemy import text
from utils.create_db_connection import get_sync_db_connection


# DAG 기본 인수 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

def _calculate_hitter_metrics():
    engine = get_sync_db_connection()
    
    select_hitter_stats_query = text("""
        SELECT hitter_id, so, bb, pa, hits, hr, ab, sf, ops
        FROM hitters;
    """)

    with engine.connect() as conn:
        select_hitter_stats_results = conn.execute(select_hitter_stats_query)
        rows = select_hitter_stats_results.fetchall()

    upsert_hitter_remain_metrics_query = text("""
        INSERT INTO hitter_metrics (hitter_id, k_rate, bb_rate, BABIP)
        VALUES (:hitter_id, :k_rate, :bb_rate, :BABIP)
        ON DUPLICATE KEY UPDATE
            k_rate = VALUES(k_rate),
            bb_rate = VALUES(bb_rate),
            BABIP = VALUES(BABIP);
    """)

    for row in rows:
        hitter_id, so, bb, pa, hits, hr, ab, sf, ops = row
        if pa == 0:
            continue
        if int(ab) - int(so) - int(hr) + int(sf) == 0:
            continue
        
        k_rate = int(so) / int(pa)
        bb_rate = int(bb) / int(pa)
        babip = ((int(hits) - int(hr)) / (int(ab) - int(so) - int(hr) + int(sf)))

        data = {"hitter_id": hitter_id, "k_rate": k_rate, "bb_rate": bb_rate, "BABIP": babip}
        with engine.begin() as conn:
            conn.execute(upsert_hitter_remain_metrics_query, data)

with DAG(
    dag_id='013_calculate_hitter_metrics',
    default_args=default_args,
    description='타자 지표를 계산하는 DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # Task 정의
    calculate_hitter_metrics = PythonOperator(
        task_id='calculate_and_update_wRC',
        python_callable=_calculate_hitter_metrics,
        dag=dag,
    )

    trigger_scrape_pitchers_stats_dag = TriggerDagRunOperator(
        task_id='trigger_scrape_pitchers_stats_dag',
        trigger_dag_id='020_scrape_pitchers_stats',
        wait_for_completion=False
    )

    # Task 의존성 설정
    calculate_hitter_metrics >> trigger_scrape_pitchers_stats_dag
