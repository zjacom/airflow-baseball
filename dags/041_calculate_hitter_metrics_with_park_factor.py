from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from collections import defaultdict
from sqlalchemy import text

from utils.create_db_connection import get_sync_db_connection

default_args = {
    'owner': 'niscom',
    'depends_on_past': False,
}

def _process_hitters_metrics_with_park_factor():
    engine = get_sync_db_connection()

    park_factor_dic = defaultdict(float)

    select_park_factor_query = text("""
        SELECT stadium, park_factor
        FROM park_factor;
    """)

    with engine.connect() as conn:
        select_park_factor_results = conn.execute(select_park_factor_query)

    for row in select_park_factor_results:
        stadium, park_factor = row
        park_factor_dic[stadium] = park_factor

    select_league_wRC_query = text("""
        SELECT SUM(wRC) league_wRC
        FROM hitter_metrics;
    """)

    select_league_obp_query = text("""
        SELECT AVG(obp) league_obp
        FROM hitters;
    """)

    select_league_slg_query = text("""
        SELECT AVG(slg) league_slg
        FROM hitters;
    """)

    select_league_pa_query = text("""
        SELECT SUM(pa) as league_pa
        FROM hitters;
    """)

    with engine.connect() as conn:
        select_league_wRC_result = conn.execute(select_league_wRC_query)
        league_wRC = float(select_league_wRC_result.scalar())

        select_league_obp_result = conn.execute(select_league_obp_query)
        league_obp = float(select_league_obp_result.scalar())

        select_league_slg_result = conn.execute(select_league_slg_query)
        league_slg = float(select_league_slg_result.scalar())

        select_league_pa_result = conn.execute(select_league_pa_query)
        league_pa = int(select_league_pa_result.scalar())

    select_today_lineup_query = text("""
        SELECT player, team, position, stadium
        FROM today_lineup;
    """)

    with engine.connect() as conn:
        select_today_lineup_results = conn.execute(select_today_lineup_query)

    select_hitter_id_query = text("""
        SELECT hitter_id
        FROM hitters
        WHERE player_name = :player AND team_name = :team
    """)

    select_hitter_wRC_query = text("""
        SELECT wRC
        FROM hitter_metrics
        WHERE hitter_id = :hitter_id
    """)

    select_hitter_obp_query = text("""
        SELECT obp
        FROM hitters
        WHERE hitter_id = :hitter_id
    """)

    select_hitter_slg_query = text("""
        SELECT slg
        FROM hitters
        WHERE hitter_id = :hitter_id
    """)

    select_hitter_pa_query = text("""
        SELECT SUM(pa)
        FROM hitters
        WHERE hitter_id = :hitter_id
    """)

    upsert_hitter_metric_query = text("""
        INSERT INTO hitter_metrics (hitter_id, wRC_plus, OPS_plus)
        VALUES (:hitter_id, :wRC_plus, :OPS_plus)
        ON DUPLICATE KEY UPDATE
            wRC_plus = VALUES(wRC_plus),
            OPS_plus = VALUES(OPS_plus);
    """)

    for row in select_today_lineup_results:
        player, team, position, stadium = row
        # 타자일 경우
        if position != 0:
            with engine.connect() as conn:
                # hitter_id 가져오기
                select_hitter_id_result = conn.execute(select_hitter_id_query, {"player": player, "team": team})
                hitter_id = select_hitter_id_result.fetchone()
                if hitter_id:
                    hitter_id = int(hitter_id[0])
                else:
                    continue
                try:
                    # 타자의 wRC 가져오기
                    select_hitter_wRC_result = conn.execute(select_hitter_wRC_query, {"hitter_id": hitter_id})
                    wRC = float(select_hitter_wRC_result.scalar())
                    # 타자의 PA 가져오기
                    select_hitter_pa_result = conn.execute(select_hitter_pa_query, {"hitter_id": hitter_id})
                    pa = float(select_hitter_pa_result.scalar())
                    # 타자의 OPS 가져오기
                    select_hitter_obp_result = conn.execute(select_hitter_obp_query, {"hitter_id": hitter_id})
                    obp = float(select_hitter_obp_result.scalar())
                    # 타자의 slg 가져오기
                    select_hitter_slg_result = conn.execute(select_hitter_slg_query, {"hitter_id": hitter_id})
                    slg = float(select_hitter_slg_result.scalar())
                except:
                    continue
            # 파크팩터 가져오기
            park_factor = park_factor_dic[stadium]
            # wRC_plus 계산
            wRC_plus = ((wRC / pa) / ((league_wRC / league_pa) / park_factor)) * 100
            # OPS_plus 계산
            ops_plus = (100 / park_factor) * ((obp / league_obp) + (slg / league_slg) - 1)

            data = {
                "hitter_id": hitter_id,
                "wRC_plus": wRC_plus,
                "OPS_plus": ops_plus
            }

            with engine.begin() as conn:
                conn.execute(upsert_hitter_metric_query, data)

with DAG(
    dag_id='041_calculate_hitter_metrics_with_park_factor',
    default_args=default_args,
    description='타자 지표를 Park Factor와 계산하는 DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # Task 정의
    process_hitters_metrics_with_park_factor = PythonOperator(
        task_id='process_hitters_metrics_with_park_factor',
        python_callable=_process_hitters_metrics_with_park_factor,
        dag=dag,
    )

    # Trigger DAG 정의
    trigger_insert_hitter_integration_data_dag = TriggerDagRunOperator(
        task_id='trigger_insert_hitter_integration_data_dag',
        trigger_dag_id='050_insert_hitter_integration_data',
        wait_for_completion=False,
    )

    # Task 의존성 설정
    process_hitters_metrics_with_park_factor >> trigger_insert_hitter_integration_data_dag