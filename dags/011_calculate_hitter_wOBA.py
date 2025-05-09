from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from sqlalchemy import text

from utils.create_db_connection import get_sync_db_connection

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

# wOBA 계산 및 업데이트 함수
def _calculate_and_update_wOBA():
    engine = get_sync_db_connection()
    
    select_hitter_details_query = text("""
        SELECT hitter_id, bb, ibb, hbp, hits, doubles, triples, hr, sb, cs, pa, sac
        FROM hitters;
    """)
    
    upsert_hitter_wOBA_query = text("""
        INSERT INTO hitter_metrics (hitter_id, wOBA)
        VALUES (:hitter_id, :wOBA)
        ON DUPLICATE KEY UPDATE wOBA = VALUES(wOBA);
    """)
    
    with engine.connect() as conn:
        results = conn.execute(select_hitter_details_query)
        
        for row in results:
            hitter_id, bb, ibb, hbp, hits, doubles, triples, hr, sb, cs, pa, sac = row
            
            # 분모가 0이 되는 경우 건너뛰기
            if (pa - ibb - sac) == 0:
                continue
                
            # wOBA 계산
            wOBA = ((0.7 * (bb - ibb + hbp)) + (0.9 * hits) + (1.25 * doubles) + 
                    (1.6 * triples) + (2 * hr) + (0.25 * sb) - (0.5 * cs)) / (pa - ibb - sac)
            
            # 데이터 업데이트
            data = {"hitter_id": hitter_id, "wOBA": wOBA}
            with engine.begin() as conn:
                conn.execute(upsert_hitter_wOBA_query, data)

# ---------------------- DAG 정의 ----------------------

with DAG(
    dag_id='011_calculate_hitter_wOBA',
    default_args=default_args,
    description='타자 wOBA를 계산하는 DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # Task 정의

    calculate_and_update_wOBA = PythonOperator(
        task_id='calculate_and_update_wOBA',
        python_callable=_calculate_and_update_wOBA,
        dag=dag,
    )

    trigger_calculate_hitter_wRC_dag = TriggerDagRunOperator(
        task_id='trigger_calculate_hitter_wRC_dag',
        trigger_dag_id='012_calculate_hitter_wRC',
        wait_for_completion=False
    )

    # Task 의존성 설정
    calculate_and_update_wOBA >> trigger_calculate_hitter_wRC_dag