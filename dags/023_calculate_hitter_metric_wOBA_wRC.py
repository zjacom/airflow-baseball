from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text

# DAG 기본 인수 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'baseball_advanced_metrics_calculation',
    default_args=default_args,
    description='야구 선수의 wOBA 및 wRC 지표 계산 및 업데이트',
    schedule_interval=None,
    catchup=False,
)

# DB 접속 설정 함수
def get_db_engine():
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

# wOBA 계산 및 업데이트 함수
def calculate_and_update_woba():
    engine = get_db_engine()
    
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
    
    return f"wOBA 계산 및 업데이트 완료: {datetime.now()}"

# wRC 계산 및 업데이트 함수
def calculate_and_update_wrc():
    engine = get_db_engine()
    
    # 리그 전체 통계 쿼리들
    select_league_runs_query = text("""
        SELECT (SUM(away_score) + SUM(home_score)) league_runs
        FROM game_schedule;
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

    select_league_games_query = text("""
        SELECT COUNT(*) AS league_games
        FROM game_schedule;
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

    select_hitter_team_query = text("""
        SELECT team_name
        FROM hitters
        WHERE hitter_id = :hitter_id
    """)

    select_team_games_query = text("""
        SELECT COUNT(*) AS team_games
        FROM game_schedule
        WHERE away_team = :team_name OR home_team = :team_name
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

        select_league_games_result = conn.execute(select_league_games_query)
        league_games = int(select_league_games_result.scalar())
    
    # wOBA 스케일 계산
    wOBA_scale = (league_wOBA - league_OBP) / (league_slg - league_OBP)
    
    # 각 타자별 wRC 계산 및 업데이트
    for row in select_hitter_wOBA_results:
        hitter_id, wOBA = row

        with engine.connect() as conn:
            select_hitter_pa_result = conn.execute(select_hitter_pa_query, {"hitter_id": hitter_id})
            pa = int(select_hitter_pa_result.scalar())

            select_hitter_team_result = conn.execute(select_hitter_team_query, {"hitter_id": hitter_id})
            team = select_hitter_team_result.fetchone()
            team = team[0]

            select_team_games_result = conn.execute(select_team_games_query, {"team_name": team})
            team_games = int(select_team_games_result.scalar())

        # wRC 계산
        wRC = (((wOBA - league_wOBA) / wOBA_scale) + (league_runs / league_pa)) * pa

        # 데이터 업데이트
        data = {"hitter_id": hitter_id, "wRC": wRC}
        with engine.begin() as conn:
            conn.execute(upsert_hitter_wRC_query, data)
    
    return f"wRC 계산 및 업데이트 완료: {datetime.now()}"

# DAG에 작업 추가
woba_task = PythonOperator(
    task_id='calculate_and_update_woba',
    python_callable=calculate_and_update_woba,
    dag=dag,
)

wrc_task = PythonOperator(
    task_id='calculate_and_update_wrc',
    python_callable=calculate_and_update_wrc,
    dag=dag,
)

# 작업 의존성 설정 - wRC는 wOBA 계산 이후에 실행되어야 함
woba_task >> wrc_task

if __name__ == "__main__":
    dag.cli()