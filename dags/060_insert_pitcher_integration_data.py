from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import os
from sqlalchemy import text
import pandas as pd

from utils.create_db_connection import get_sync_db_connection

default_args = {
    'owner': 'niscom',
    'depends_on_past': False,
}

def _process_pitchers_integration_data():
    engine = get_sync_db_connection()

    select_today_lineup_query = text("""
        SELECT game_date, player, team, position, opponent, stadium
        FROM today_lineup;
    """)

    select_pitcher_stats_query = text("""
        SELECT pitcher_id, era, games, cg, sho, wins, losses, sv, hld, wpct, tbf, np, ip, hits, doubles, triples, hr, sac, sf, bb, ibb, so, wp, bk, runs, er, bsv, whip, avg, qs
        FROM pitchers
        WHERE player_name = :player AND team_name = :team
    """)

    select_pitcher_metrics_query = text("""
        SELECT FIP, k_rate, bb_rate, hr_rate
        FROM pitcher_metrics
        WHERE pitcher_id = :pitcher_id
    """)

    select_pitcher_opponents_query = text("""
        SELECT games, era, wins, losses, sv, hld, wpct, tbf, ip, hits, hr, bb, hbp, so, runs, er, avg
        FROM pitcher_opponents
        WHERE pitcher_id = :pitcher_id AND opponent_team = :opponent
    """)

    select_pitcher_stadiums_query = text("""
        SELECT games, era, wins, losses, sv, hld, wpct, tbf, ip, hits, hr, bb, hbp, so, runs, er, avg
        FROM pitcher_stadiums
        WHERE pitcher_id = :pitcher_id AND stadium = :stadium
    """)

    select_pitcher_recent_games_query = text("""
        SELECT game_date, opponent_team, result, era, tbf, ip, hits, hr, bb, hbp, so, runs, er, avg
        FROM pitcher_games
        WHERE pitcher_id = :pitcher_id
        ORDER BY game_date DESC
        LIMIT 5;
    """)


    insert_pitcher_records_query = text("""
        INSERT INTO pitcher_records (
            pitcher_id, player_name, team_name, game_date, era, games, cg, sho, wins, losses, sv, hld, wpct, tbf, np, ip,
            hits, doubles, triples, hr, sac, sf, bb, ibb, so, wp, bk, runs, er, bsv, whip, avg, qs, FIP, k_rate, bb_rate, hr_rate,
            opponent_team, opponent_games, opponent_era, opponent_wins, opponent_losses, opponent_sv, opponent_hld,
            opponent_wpct, opponent_tbf, opponent_ip, opponent_hits, opponent_hr, opponent_bb, opponent_hbp, opponent_so,
            opponent_runs, opponent_er, opponent_avg,
            stadium, stadium_games, stadium_era, stadium_wins, stadium_losses, stadium_sv, stadium_hld,
            stadium_wpct, stadium_tbf, stadium_ip, stadium_hits, stadium_hr, stadium_bb, stadium_hbp, stadium_so,
            stadium_runs, stadium_er, stadium_avg,
            recent_games_file_path
        ) VALUES (
            :pitcher_id, :player_name, :team_name, :game_date, :era, :games, :cg, :sho, :wins, :losses, :sv, :hld, :wpct, :tbf, :np, :ip,
            :hits, :doubles, :triples, :hr, :sac, :sf, :bb, :ibb, :so, :wp, :bk, :runs, :er, :bsv, :whip, :avg, :qs, :FIP, :k_rate, :bb_rate, :hr_rate,
            :opponent_team, :opponent_games, :opponent_era, :opponent_wins, :opponent_losses, :opponent_sv, :opponent_hld,
            :opponent_wpct, :opponent_tbf, :opponent_ip, :opponent_hits, :opponent_hr, :opponent_bb, :opponent_hbp, :opponent_so,
            :opponent_runs, :opponent_er, :opponent_avg,
            :stadium, :stadium_games, :stadium_era, :stadium_wins, :stadium_losses, :stadium_sv, :stadium_hld,
            :stadium_wpct, :stadium_tbf, :stadium_ip, :stadium_hits, :stadium_hr, :stadium_bb, :stadium_hbp, :stadium_so,
            :stadium_runs, :stadium_er, :stadium_avg,
            :recent_games_file_path
        )
    """)

    with engine.connect() as conn:
        select_today_lineup_results = conn.execute(select_today_lineup_query)

    for row in select_today_lineup_results:
        game_date, player, team, position, opponent, stadium = row
        # 투수일 경우
        if position == 0:
            with engine.connect() as conn:
                # pitchers 테이블에서 정보 가져오기
                select_pitcher_stats_result = conn.execute(select_pitcher_stats_query, {"player": player, "team": team}).fetchone()
                if select_pitcher_stats_result is None:
                    print(f"No stats found for player {player} from team {team}")
                    continue
                pitcher_id, era, games, cg, sho, wins, losses, sv, hld, wpct, tbf, np, ip, hits, doubles, triples, hr, sac, sf, bb, ibb, so, wp, bk, runs, er, bsv, whip, avg, qs = select_pitcher_stats_result
                
                # pitchers_metrics 테이블에서 정보 가져오기
                select_pitcher_metrics_result = conn.execute(select_pitcher_metrics_query, {"pitcher_id": pitcher_id}).fetchone()
                if select_pitcher_metrics_result is None:
                    print(f"No metrics found for pitcher_id {pitcher_id}")
                    fip, k_rate, bb_rate, hr_rate = None, None, None, None
                else:
                    fip, k_rate, bb_rate, hr_rate = select_pitcher_metrics_result

                # pitcher_opponents 테이블에서 정보 가져오기
                select_pitcher_opponents_result = conn.execute(select_pitcher_opponents_query, {"pitcher_id": pitcher_id, "opponent": opponent}).fetchone()
                if select_pitcher_opponents_result is None:
                    print(f"No opponent stats found for hitter_id {pitcher_id} against {opponent}")
                    o_games, o_era, o_wins, o_losses, o_sv, o_hld, o_wpct, o_tbf, o_ip, o_hits, o_hr, o_bb, o_hbp, o_so, o_runs, o_er, o_avg = None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None
                else:
                    o_games, o_era, o_wins, o_losses, o_sv, o_hld, o_wpct, o_tbf, o_ip, o_hits, o_hr, o_bb, o_hbp, o_so, o_runs, o_er, o_avg = select_pitcher_opponents_result

                # pitcher_stadiums 테이블에서 정보 가져오기
                select_pitcher_stadiums_result = conn.execute(select_pitcher_stadiums_query, {"pitcher_id": pitcher_id, "stadium": stadium}).fetchone()
                if select_pitcher_stadiums_result is None:
                    print(f"No stadium stats found for hitter_id {pitcher_id} at {stadium}")
                    s_games, s_era, s_wins, s_losses, s_sv, s_hld, s_wpct, s_tbf, s_ip, s_hits, s_hr, s_bb, s_hbp, s_so, s_runs, s_er, s_avg = None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None
                else:
                    s_games, s_era, s_wins, s_losses, s_sv, s_hld, s_wpct, s_tbf, s_ip, s_hits, s_hr, s_bb, s_hbp, s_so, s_runs, s_er, s_avg = select_pitcher_stadiums_result

                # pitcher_games 테이블에서 game_date를 기준으로 최근 10경기 데이터를 가져온 뒤 CSV 파일 형식으로 저장하여 해당 경로 DB에 저장
                select_pitcher_recent_games_result = pd.read_sql(select_pitcher_recent_games_query, conn, params={"pitcher_id": pitcher_id})
                output_path = f"data/pitcher/{game_date}/{pitcher_id}.csv"
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                select_pitcher_recent_games_result.to_csv(output_path, index=False)

                # 먼저 컬럼 리스트 정의
                columns = [
                    "pitcher_id", "player_name", "team_name", "game_date",
                    "era", "games", "cg", "sho", "wins", "losses", "sv", "hld", "wpct", "tbf", "np", "ip",
                    "hits", "doubles", "triples", "hr", "sac", "sf", "bb", "ibb", "so", "wp", "bk", "runs", "er", "bsv", "whip", "avg", "qs",
                    "FIP", "k_rate", "bb_rate", "hr_rate",
                    "opponent_team", "opponent_games", "opponent_era", "opponent_wins", "opponent_losses", "opponent_sv", "opponent_hld",
                    "opponent_wpct", "opponent_tbf", "opponent_ip", "opponent_hits", "opponent_hr", "opponent_bb", "opponent_hbp",
                    "opponent_so", "opponent_runs", "opponent_er", "opponent_avg",
                    "stadium", "stadium_games", "stadium_era", "stadium_wins", "stadium_losses", "stadium_sv", "stadium_hld",
                    "stadium_wpct", "stadium_tbf", "stadium_ip", "stadium_hits", "stadium_hr", "stadium_bb", "stadium_hbp",
                    "stadium_so", "stadium_runs", "stadium_er", "stadium_avg", "recent_games_file_path"
                ]

                data = (pitcher_id, player, team, game_date,
                        era, games, cg, sho, wins, losses, sv, hld, wpct, tbf, np, ip,
                        hits, doubles, triples, hr, sac, sf, bb, ibb, so, wp, bk, runs, er, bsv, whip, avg, qs,
                        fip, k_rate, bb_rate, hr_rate,
                        opponent, o_games, o_era, o_wins, o_losses, o_sv, o_hld, o_wpct, o_tbf, o_ip, o_hits, o_hr,
                        o_bb, o_hbp, o_so, o_runs, o_er, o_avg,
                        stadium, s_games, s_era, s_wins, s_losses, s_sv, s_hld, s_wpct, s_tbf, s_ip, s_hits, s_hr,
                        s_bb, s_hbp, s_so, s_runs, s_er, s_avg, output_path)

                # 컬럼과 값을 매핑하여 dict 생성
                data_dict = dict(zip(columns, data))

                conn.execute(insert_pitcher_records_query, data_dict)

with DAG(
    dag_id='060_insert_pitcher_integration_data',
    default_args=default_args,
    description='투수의 모든 데이터를 통합하는 DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # Task 정의
    process_pitchers_integration_data = PythonOperator(
        task_id='process_pitchers_integration_data',
        python_callable=_process_pitchers_integration_data,
        dag=dag,
    )

    # Task 의존성 설정
    process_pitchers_integration_data