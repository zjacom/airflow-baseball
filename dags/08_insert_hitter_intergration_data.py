from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import pandas as pd
from sqlalchemy import create_engine, text

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'baseball_stats_etl',
    default_args=default_args,
    description='일일 야구 선수 통계 수집 및 처리 파이프라인',
    schedule_interval='None',  # 매일 오전 8시에 실행
    catchup=False,
)

# DB 접속 설정
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

# 쿼리 정의
def get_queries():
    select_today_lineup_query = text("""
        SELECT game_date, player, team, position, opponent, stadium
        FROM today_lineup;
    """)

    select_hitter_stats_query = text("""
        SELECT hitter_id, avg, games, pa, ab, runs, hits, doubles, triples, hr, rbi, sb, cs, sac, sf, bb, ibb, hbp, so, gdp, slg, obp, errors, sb_percentage, mh, ops, risp, ph_ba
        FROM hitters
        WHERE player_name = :player AND team_name = :team
    """)

    select_hitter_metrics_query = text("""
        SELECT wOBA, wRC, wRC_plus, OPS_plus, k_rate, bb_rate, BABIP
        FROM hitter_metrics
        WHERE hitter_id = :hitter_id
    """)

    select_hitter_opponents_query = text("""
        SELECT games, avg, pa, ab, runs, hits, doubles, triples, hr, rbi, sb, cs, bb, hbp, so, gdp
        FROM hitter_opponents
        WHERE hitter_id = :hitter_id AND opponent_team = :opponent
    """)

    select_hitter_stadiums_query = text("""
        SELECT games, avg, pa, ab, runs, hits, doubles, triples, hr, rbi, sb, cs, bb, hbp, so, gdp
        FROM hitter_stadiums
        WHERE hitter_id = :hitter_id AND stadium = :stadium
    """)

    select_hitter_recent_games_query = text("""
        SELECT game_date, opponent_team, avg, pa, ab, runs, hits, doubles, triples, hr, rbi, sb, cs, bb, hbp, so, gdp
        FROM hitter_games
        WHERE hitter_id = :hitter_id
        ORDER BY game_date DESC
        LIMIT 5;
    """)

    insert_hitter_records_query = text("""
        INSERT INTO hitter_records (
            hitter_id, player_name, team_name, game_date, position, avg, games, pa, ab, runs, hits,
            doubles, triples, hr, rbi, sb, cs, sac, sf, bb, ibb, hbp, so, gdp, slg, obp, errors,
            sb_percentage, mh, ops, risp, ph_ba, wOBA, wRC, wRC_plus, OPS_plus, k_rate, bb_rate, BABIP,
            opponent_team, opponent_games, opponent_avg, opponent_pa, opponent_ab, opponent_runs,
            opponent_hits, opponent_doubles, opponent_triples, opponent_hr, opponent_rbi, opponent_sb,
            opponent_cs, opponent_bb, opponent_hbp, opponent_so, opponent_gdp,
            stadium, stadium_games, stadium_avg, stadium_pa, stadium_ab, stadium_runs,
            stadium_hits, stadium_doubles, stadium_triples, stadium_hr, stadium_rbi,
            stadium_sb, stadium_cs, stadium_bb, stadium_hbp, stadium_so, stadium_gdp,
            recent_games_file_path
        )
        VALUES (
            :hitter_id, :player_name, :team_name, :game_date, :position, :avg, :games, :pa, :ab, :runs, :hits,
            :doubles, :triples, :hr, :rbi, :sb, :cs, :sac, :sf, :bb, :ibb, :hbp, :so, :gdp, :slg, :obp, :errors,
            :sb_percentage, :mh, :ops, :risp, :ph_ba, :wOBA, :wRC, :wRC_plus, :OPS_plus, :k_rate, :bb_rate, :BABIP,
            :opponent_team, :opponent_games, :opponent_avg, :opponent_pa, :opponent_ab, :opponent_runs,
            :opponent_hits, :opponent_doubles, :opponent_triples, :opponent_hr, :opponent_rbi, :opponent_sb,
            :opponent_cs, :opponent_bb, :opponent_hbp, :opponent_so, :opponent_gdp,
            :stadium, :stadium_games, :stadium_avg, :stadium_pa, :stadium_ab, :stadium_runs,
            :stadium_hits, :stadium_doubles, :stadium_triples, :stadium_hr, :stadium_rbi,
            :stadium_sb, :stadium_cs, :stadium_bb, :stadium_hbp, :stadium_so, :stadium_gdp,
            :recent_games_file_path
        )
    """)
    
    return {
        "select_today_lineup_query": select_today_lineup_query,
        "select_hitter_stats_query": select_hitter_stats_query,
        "select_hitter_metrics_query": select_hitter_metrics_query,
        "select_hitter_opponents_query": select_hitter_opponents_query,
        "select_hitter_stadiums_query": select_hitter_stadiums_query,
        "select_hitter_recent_games_query": select_hitter_recent_games_query,
        "insert_hitter_records_query": insert_hitter_records_query
    }

# 라인업 데이터 가져오기
def fetch_lineup(**kwargs):
    engine = get_db_engine()
    queries = get_queries()
    
    with engine.connect() as conn:
        select_today_lineup_results = conn.execute(queries["select_today_lineup_query"]).fetchall()
    
    return [dict(row._mapping) for row in select_today_lineup_results]

# 타자 데이터 처리 함수
def process_hitter_data(**kwargs):
    ti = kwargs['ti']
    lineup_data = ti.xcom_pull(task_ids='fetch_lineup')
    
    if not lineup_data:
        print("라인업 데이터가 없습니다.")
        return
    
    engine = get_db_engine()
    queries = get_queries()
    
    # 데이터 처리 결과 저장할 리스트
    processed_records = []
    
    for player_data in lineup_data:
        game_date = player_data['game_date']
        player = player_data['player']
        team = player_data['team']
        position = player_data['position']
        opponent = player_data['opponent']
        stadium = player_data['stadium']
        
        # 투수가 아닌 타자만 처리
        if position != 0:
            with engine.connect() as conn:
                # 타자 기본 통계 가져오기
                select_hitter_stats_result = conn.execute(
                    queries["select_hitter_stats_query"], 
                    {"player": player, "team": team}
                ).fetchone()
                
                if select_hitter_stats_result is None:
                    print(f"선수 {player}(팀: {team})에 대한 통계 정보가 없습니다.")
                    continue
                
                hitter_id, avg, games, pa, ab, runs, hits, doubles, triples, hr, rbi, sb, cs, sac, sf, bb, ibb, hbp, so, gdp, slg, obp, errors, sb_percentage, mh, ops, risp, ph_ba = select_hitter_stats_result
                
                # 타자 고급 지표 가져오기
                select_hitter_metrics_result = conn.execute(
                    queries["select_hitter_metrics_query"], 
                    {"hitter_id": hitter_id}
                ).fetchone()
                
                if select_hitter_metrics_result is None:
                    print(f"선수 ID {hitter_id}에 대한 고급 지표 정보가 없습니다.")
                    wOBA, wRC, wRC_plus, OPS_plus, k_rate, bb_rate, babip = None, None, None, None, None, None, None
                else:
                    wOBA, wRC, wRC_plus, OPS_plus, k_rate, bb_rate, babip = select_hitter_metrics_result
                
                # 상대팀 상대 기록 가져오기
                select_hitter_opponents_result = conn.execute(
                    queries["select_hitter_opponents_query"], 
                    {"hitter_id": hitter_id, "opponent": opponent}
                ).fetchone()
                
                if select_hitter_opponents_result is None:
                    print(f"선수 ID {hitter_id}의 {opponent} 상대 기록이 없습니다.")
                    o_games, o_avg, o_pa, o_ab, o_runs, o_hits, o_doubles, o_triples, o_hr, o_rbi, o_sb, o_cs, o_bb, o_hbp, o_so, o_gdp = None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None
                else:
                    o_games, o_avg, o_pa, o_ab, o_runs, o_hits, o_doubles, o_triples, o_hr, o_rbi, o_sb, o_cs, o_bb, o_hbp, o_so, o_gdp = select_hitter_opponents_result
                
                # 구장별 기록 가져오기
                select_hitter_stadiums_result = conn.execute(
                    queries["select_hitter_stadiums_query"], 
                    {"hitter_id": hitter_id, "stadium": stadium}
                ).fetchone()
                
                if select_hitter_stadiums_result is None:
                    print(f"선수 ID {hitter_id}의 {stadium} 구장 기록이 없습니다.")
                    s_games, s_avg, s_pa, s_ab, s_runs, s_hits, s_doubles, s_triples, s_hr, s_rbi, s_sb, s_cs, s_bb, s_hbp, s_so, s_gdp = None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None
                else:
                    s_games, s_avg, s_pa, s_ab, s_runs, s_hits, s_doubles, s_triples, s_hr, s_rbi, s_sb, s_cs, s_bb, s_hbp, s_so, s_gdp = select_hitter_stadiums_result
                
                # 최근 경기 기록 가져오기 및 CSV로 저장
                recent_games_df = pd.read_sql(
                    queries["select_hitter_recent_games_query"].text, 
                    conn, 
                    params={"hitter_id": hitter_id}
                )
                
                # 데이터 저장 경로를 Airflow의 데이터 디렉토리로 설정
                data_dir = f"/opt/airflow/data/baseball/hitter/{kwargs['ds']}"
                os.makedirs(data_dir, exist_ok=True)

                output_path = f"{data_dir}/{hitter_id}.csv"
                recent_games_df.to_csv(output_path, index=False)
                
                # 모든 데이터 수집
                columns = [
                    "hitter_id", "player_name", "team_name", "game_date", "position",
                    "avg", "games", "pa", "ab", "runs", "hits", "doubles", "triples", "hr", "rbi", "sb", "cs", "sac", "sf",
                    "bb", "ibb", "hbp", "so", "gdp", "slg", "obp", "errors", "sb_percentage", "mh", "ops", "risp", "ph_ba",
                    "wOBA", "wRC", "wRC_plus", "OPS_plus", "k_rate", "bb_rate", "BABIP",
                    "opponent_team", "opponent_games", "opponent_avg", "opponent_pa", "opponent_ab", "opponent_runs",
                    "opponent_hits", "opponent_doubles", "opponent_triples", "opponent_hr", "opponent_rbi", "opponent_sb",
                    "opponent_cs", "opponent_bb", "opponent_hbp", "opponent_so", "opponent_gdp",
                    "stadium", "stadium_games", "stadium_avg", "stadium_pa", "stadium_ab", "stadium_runs",
                    "stadium_hits", "stadium_doubles", "stadium_triples", "stadium_hr", "stadium_rbi",
                    "stadium_sb", "stadium_cs", "stadium_bb", "stadium_hbp", "stadium_so", "stadium_gdp",
                    "recent_games_file_path"
                ]
                
                data = (hitter_id, player, team, game_date, position,
                        avg, games, pa, ab, runs, hits, doubles, triples, hr, rbi, sb, cs, sac, sf,
                        bb, ibb, hbp, so, gdp, slg, obp, errors, sb_percentage, mh, ops, risp, ph_ba,
                        wOBA, wRC, wRC_plus, OPS_plus, k_rate, bb_rate, babip,
                        opponent, o_games, o_avg, o_pa, o_ab, o_runs, o_hits, o_doubles, o_triples,
                        o_hr, o_rbi, o_sb, o_cs, o_bb, o_hbp, o_so, o_gdp,
                        stadium, s_games, s_avg, s_pa, s_ab, s_runs, s_hits, s_doubles, s_triples,
                        s_hr, s_rbi, s_sb, s_cs, s_bb, s_hbp, s_so, s_gdp, output_path)
                
                # 딕셔너리로 변환하여 저장
                data_dict = dict(zip(columns, data))
                processed_records.append(data_dict)
    
    return processed_records

# DB에 결과 저장
def save_to_database(**kwargs):
    ti = kwargs['ti']
    processed_records = ti.xcom_pull(task_ids='process_hitter_data')
    
    if not processed_records:
        print("처리된 데이터가 없습니다.")
        return
    
    engine = get_db_engine()
    queries = get_queries()
    
    with engine.connect() as conn:
        for record in processed_records:
            try:
                conn.execute(queries["insert_hitter_records_query"], record)
                conn.commit()
                print(f"선수 ID {record['hitter_id']}의 기록이 성공적으로 저장되었습니다.")
            except Exception as e:
                conn.rollback()
                print(f"선수 ID {record['hitter_id']}의 기록 저장 중 오류 발생: {str(e)}")

# Task 정의
task_fetch_lineup = PythonOperator(
    task_id='fetch_lineup',
    python_callable=fetch_lineup,
    provide_context=True,
    dag=dag,
)

task_process_hitter_data = PythonOperator(
    task_id='process_hitter_data',
    python_callable=process_hitter_data,
    provide_context=True,
    dag=dag,
)

task_save_to_database = PythonOperator(
    task_id='save_to_database',
    python_callable=save_to_database,
    provide_context=True,
    dag=dag,
)

# Task 간 의존성 설정
task_fetch_lineup >> task_process_hitter_data >> task_save_to_database