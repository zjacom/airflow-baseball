from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from collections import defaultdict

# DAG의 기본 인자 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'baseball_metrics_calculation',
    default_args=default_args,
    description='야구 타자 지표 계산 및 업데이트 DAG',
    schedule_interval=None,  # 매일 03:00에 실행
    catchup=False,
)

# DB 접속 설정 - Airflow Connection으로 대체하는 것이 좋습니다.
def get_db_connection():
    db_config = {
        'user': '{{ conn.baseball_db.login }}',
        'password': '{{ conn.baseball_db.password }}',
        'host': '{{ conn.baseball_db.host }}',
        'port': '{{ conn.baseball_db.port }}',
        'database': '{{ conn.baseball_db.schema }}',
    }
    
    # SQLAlchemy 연결 문자열
    engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
                        f"{db_config['host']}:{db_config['port']}/{db_config['database']}")
    return engine

# 파크 팩터 데이터 가져오는 태스크
def get_park_factors(**context):
    engine = get_db_connection()
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
    
    return park_factor_dic

# 리그 통계 데이터 가져오는 태스크
def get_league_stats(**context):
    engine = get_db_connection()
    league_stats = {}
    
    # 리그 득점 총합
    select_league_runs_query = text("""
        SELECT (SUM(away_score) + SUM(home_score)) league_runs
        FROM game_schedule;
    """)
    
    # 리그 wRC 총합
    select_league_wRC_query = text("""
        SELECT SUM(wRC) league_wRC
        FROM hitter_metrics;
    """)
    
    # 리그 OBP 평균
    select_league_obp_query = text("""
        SELECT AVG(obp) league_obp
        FROM hitters;
    """)
    
    # 리그 SLG 평균
    select_league_slg_query = text("""
        SELECT AVG(slg) league_slg
        FROM hitters;
    """)
    
    # 리그 타석 총합
    select_league_pa_query = text("""
        SELECT SUM(pa) as league_pa
        FROM hitters;
    """)
    
    # 리그 wOBA 평균
    select_league_wOBA_query = text("""
        SELECT AVG(wOBA) as league_wOBA
        FROM hitter_metrics;
    """)
    
    with engine.connect() as conn:
        select_league_runs_result = conn.execute(select_league_runs_query)
        league_stats['league_runs'] = int(select_league_runs_result.scalar())
        
        select_league_wRC_result = conn.execute(select_league_wRC_query)
        league_stats['league_wRC'] = float(select_league_wRC_result.scalar())
        
        select_league_obp_result = conn.execute(select_league_obp_query)
        league_stats['league_obp'] = float(select_league_obp_result.scalar())
        
        select_league_slg_result = conn.execute(select_league_slg_query)
        league_stats['league_slg'] = float(select_league_slg_result.scalar())
        
        select_league_pa_result = conn.execute(select_league_pa_query)
        league_stats['league_pa'] = int(select_league_pa_result.scalar())
        
        select_league_wOBA_result = conn.execute(select_league_wOBA_query)
        league_stats['league_wOBA'] = float(select_league_wOBA_result.scalar())
    
    return league_stats

# 오늘 라인업 데이터 가져오는 태스크
def get_today_lineup(**context):
    engine = get_db_connection()
    
    select_today_lineup_query = text("""
        SELECT player, team, position, stadium
        FROM today_lineup;
    """)
    
    with engine.connect() as conn:
        select_today_lineup_results = conn.execute(select_today_lineup_query)
        today_lineup = [dict(row) for row in select_today_lineup_results]
    
    return today_lineup

# 타자 메트릭스 계산 및 업데이트 태스크
def calculate_and_update_metrics(**context):
    # Airflow XCom에서 이전 태스크 결과 가져오기
    ti = context['ti']
    park_factor_dic = ti.xcom_pull(task_ids='get_park_factors')
    league_stats = ti.xcom_pull(task_ids='get_league_stats')
    today_lineup = ti.xcom_pull(task_ids='get_today_lineup')
    
    engine = get_db_connection()
    
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
    
    select_hitter_wOBA_query = text("""
        SELECT wOBA
        FROM hitter_metrics
        WHERE hitter_id = :hitter_id
    """)
    
    upsert_hitter_metric_query = text("""
        INSERT INTO hitter_metrics (hitter_id, wRC_plus, OPS_plus)
        VALUES (:hitter_id, :wRC_plus, :OPS_plus)
        ON DUPLICATE KEY UPDATE
            wRC_plus = VALUES(wRC_plus),
            OPS_plus = VALUES(OPS_plus);
    """)
    
    updated_count = 0
    
    for player_info in today_lineup:
        player = player_info['player']
        team = player_info['team']
        position = player_info['position']
        stadium = player_info['stadium']
        
        # 타자일 경우
        if position != 0:
            with engine.connect() as conn:
                # hitter_id 가져오기
                select_hitter_id_result = conn.execute(select_hitter_id_query, {"player": player, "team": team})
                hitter_id_row = select_hitter_id_result.fetchone()
                
                if not hitter_id_row:
                    continue
                
                hitter_id = int(hitter_id_row[0])
                
                try:
                    # 타자의 wRC 가져오기
                    select_hitter_wRC_result = conn.execute(select_hitter_wRC_query, {"hitter_id": hitter_id})
                    wRC = float(select_hitter_wRC_result.scalar())
                    
                    # 타자의 PA 가져오기
                    select_hitter_pa_result = conn.execute(select_hitter_pa_query, {"hitter_id": hitter_id})
                    pa = float(select_hitter_pa_result.scalar())
                    
                    # 타자의 OBP 가져오기
                    select_hitter_obp_result = conn.execute(select_hitter_obp_query, {"hitter_id": hitter_id})
                    obp = float(select_hitter_obp_result.scalar())
                    
                    # 타자의 SLG 가져오기
                    select_hitter_slg_result = conn.execute(select_hitter_slg_query, {"hitter_id": hitter_id})
                    slg = float(select_hitter_slg_result.scalar())
                    
                    # 타자의 wOBA 가져오기
                    select_hitter_wOBA_result = conn.execute(select_hitter_wOBA_query, {"hitter_id": hitter_id})
                    wOBA = float(select_hitter_wOBA_result.scalar())
                except:
                    continue
            
            # 파크팩터 가져오기
            park_factor = park_factor_dic[stadium]
            
            # wRC_plus 계산
            wRC_plus = ((wRC / pa) / ((league_stats['league_wRC'] / league_stats['league_pa']) / park_factor)) * 100
            
            # OPS_plus 계산
            ops_plus = (100 / park_factor) * ((obp / league_stats['league_obp']) + (slg / league_stats['league_slg']) - 1)
            
            data = {
                "hitter_id": hitter_id,
                "wRC_plus": wRC_plus,
                "OPS_plus": ops_plus
            }
            
            with engine.begin() as conn:
                conn.execute(upsert_hitter_metric_query, data)
                updated_count += 1
    
    return f"{updated_count}명의 타자 메트릭스가 업데이트되었습니다."

# 태스크 정의
park_factors_task = PythonOperator(
    task_id='get_park_factors',
    python_callable=get_park_factors,
    provide_context=True,
    dag=dag,
)

league_stats_task = PythonOperator(
    task_id='get_league_stats',
    python_callable=get_league_stats,
    provide_context=True,
    dag=dag,
)

today_lineup_task = PythonOperator(
    task_id='get_today_lineup',
    python_callable=get_today_lineup,
    provide_context=True,
    dag=dag,
)

calculate_metrics_task = PythonOperator(
    task_id='calculate_and_update_metrics',
    python_callable=calculate_and_update_metrics,
    provide_context=True,
    dag=dag,
)

# 태스크 의존성 설정
[park_factors_task, league_stats_task, today_lineup_task] >> calculate_metrics_task