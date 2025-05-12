from airflow import DAG
from airflow.operators.python import PythonOperator
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
    
    drop_table_query = """
        DROP TABLE IF EXISTS game_records;
        DROP TABLE IF EXISTS game_records_until_5_innings;
        DROP TABLE IF EXISTS hitter_games;
        DROP TABLE IF EXISTS hitter_metrics;
        DROP TABLE IF EXISTS hitter_opponents;
        DROP TABLE IF EXISTS hitter_records;
        DROP TABLE IF EXISTS hitter_stadiums;
        DROP TABLE IF EXISTS hitters;
        DROP TABLE IF EXISTS park_factor;
        DROP TABLE IF EXISTS pitcher_games;
        DROP TABLE IF EXISTS pitcher_metrics;
        DROP TABLE IF EXISTS pitcher_opponents;
        DROP TABLE IF EXISTS pitcher_stadiums;
        DROP TABLE IF EXISTS pitchers;
        DROP TABLE IF EXISTS today_games;
        DROP TABLE IF EXISTS today_lineup;
    """
    
    create_table_query = """
        CREATE TABLE IF NOT EXISTS park_factor (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stadium VARCHAR(50) NOT NULL,
            park_factor FLOAT NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS hitters (
            hitter_id INT PRIMARY KEY COMMENT '타자 고유 ID',
            player_name VARCHAR(50) COMMENT '선수 이름',
            team_name VARCHAR(50) COMMENT '소속 팀 이름',
            avg DECIMAL(5, 3) COMMENT '타율',
            games INT COMMENT '경기 수',
            pa INT COMMENT '타석',
            ab INT COMMENT '타수',
            runs INT COMMENT '득점',
            hits INT COMMENT '안타',
            doubles INT COMMENT '2루타',
            triples INT COMMENT '3루타',
            hr INT COMMENT '홈런',
            rbi INT COMMENT '타점',
            sb INT COMMENT '도루',
            cs INT COMMENT '도루 실패',
            sac INT COMMENT '희생 번트',
            sf INT COMMENT '희생 플라이',
            bb INT COMMENT '볼넷',
            ibb INT COMMENT '고의 4구',
            hbp INT COMMENT '몸에 맞는 공',
            so INT COMMENT '삼진',
            gdp INT COMMENT '병살타',
            slg DECIMAL(5, 3) COMMENT '장타율',
            obp DECIMAL(5, 3) COMMENT '출루율',
            errors INT COMMENT '실책',
            sb_percentage DECIMAL(5, 3) COMMENT '도루 성공률',
            mh INT COMMENT '멀티 히트',
            ops DECIMAL(5, 3) COMMENT '출루율 + 장타율',
            risp DECIMAL(5, 3) COMMENT '득점권 타율',
            ph_ba DECIMAL(5, 3) COMMENT '대타 타율',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '데이터 수정 시각'
        ) COMMENT = '타자 기본 기록 통계 테이블';
                              
        CREATE TABLE IF NOT EXISTS hitter_opponents (
            id INT AUTO_INCREMENT PRIMARY KEY COMMENT '상대별 고유 ID',
            hitter_id INT NOT NULL COMMENT '타자 고유 ID (hitters 테이블의 외래키)',
            opponent_team VARCHAR(50) COMMENT '상대 팀 이름',
            games INT COMMENT '경기수',
            avg DECIMAL(5, 3) COMMENT '타율',
            pa INT COMMENT '타석',
            ab INT COMMENT '타수',
            runs INT COMMENT '득점',
            hits INT COMMENT '안타 수',
            doubles INT COMMENT '2루타',
            triples INT COMMENT '3루타',
            hr INT COMMENT '홈런',
            rbi INT COMMENT '타점',
            sb INT COMMENT '도루 성공',
            cs INT COMMENT '도루 실패',
            bb INT COMMENT '볼넷',
            hbp INT COMMENT '몸에 맞는 공',
            so INT COMMENT '삼진',
            gdp INT COMMENT '병살타',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '기록 업데이트 시각',
            UNIQUE KEY unique_hitter_opponent (hitter_id, opponent_team)  -- UNIQUE 제약 추가
        ) COMMENT = '상대별 타자 성적 테이블';
                              
        CREATE TABLE IF NOT EXISTS hitter_stadiums (
            id INT AUTO_INCREMENT PRIMARY KEY COMMENT '구장별 고유 ID',
            hitter_id INT NOT NULL COMMENT '타자 고유 ID (hitters 테이블의 외래키)',
            stadium VARCHAR(50) COMMENT '구장',
            games INT COMMENT '경기수',
            avg DECIMAL(5, 3) COMMENT '타율',
            pa INT COMMENT '타석',
            ab INT COMMENT '타수',
            runs INT COMMENT '득점',
            hits INT COMMENT '안타 수',
            doubles INT COMMENT '2루타',
            triples INT COMMENT '3루타',
            hr INT COMMENT '홈런',
            rbi INT COMMENT '타점',
            sb INT COMMENT '도루 성공',
            cs INT COMMENT '도루 실패',
            bb INT COMMENT '볼넷',
            hbp INT COMMENT '몸에 맞는 공',
            so INT COMMENT '삼진',
            gdp INT COMMENT '병살타',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '기록 업데이트 시각',
            UNIQUE KEY unique_hitter_stadium (hitter_id, stadium)  -- UNIQUE 제약 추가
        ) COMMENT = '구장별 타자 성적 테이블';

        CREATE TABLE IF NOT EXISTS hitter_games (
            id INT AUTO_INCREMENT PRIMARY KEY COMMENT '게임별 고유 ID',
            hitter_id INT NOT NULL COMMENT '타자 고유 ID (hitters 테이블의 외래키)',
            game_date DATE NOT NULL COMMENT '경기 날짜',
            opponent_team VARCHAR(50) COMMENT '상대 팀 이름',
            avg DECIMAL(5, 3) COMMENT '타율',
            pa INT COMMENT '타석',
            ab INT COMMENT '타수',
            runs INT COMMENT '득점',
            hits INT COMMENT '안타 수',
            doubles INT COMMENT '2루타',
            triples INT COMMENT '3루타',
            hr INT COMMENT '홈런',
            rbi INT COMMENT '타점',
            sb INT COMMENT '도루 성공',
            cs INT COMMENT '도루 실패',
            bb INT COMMENT '볼넷',
            hbp INT COMMENT '몸에 맞는 공',
            so INT COMMENT '삼진',
            gdp INT COMMENT '병살타',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '기록 저장 시각',
            UNIQUE KEY unique_hitter_game (hitter_id, game_date)  -- UNIQUE 제약 추가
        ) COMMENT = '경기별 타자 성적 테이블';
                              
        CREATE TABLE IF NOT EXISTS pitchers (
            pitcher_id INT PRIMARY KEY COMMENT '투수 고유 ID',
            player_name VARCHAR(50) COMMENT '선수 이름',
            team_name VARCHAR(50) COMMENT '소속 팀 이름',
            era DECIMAL(5, 2) COMMENT '평균 자책점',
            games INT COMMENT '등판 경기 수',
            cg INT COMMENT '완투 수',
            sho INT COMMENT '완봉 수',
            wins INT COMMENT '승리 수',
            losses INT COMMENT '패배 수',
            sv INT COMMENT '세이브 수',
            hld INT COMMENT '홀드 수',
            wpct DECIMAL(5, 3) COMMENT '승률',
            tbf INT COMMENT '상대한 타자 수',
            np INT COMMENT '총 투구 수',
            ip DECIMAL(5, 1) COMMENT '투구 이닝 수',
            hits INT COMMENT '피안타 수',
            doubles INT COMMENT '피 2루타 수',
            triples INT COMMENT '피 3루타 수',
            hr INT COMMENT '피홈런 수',
            sac INT COMMENT '희생 번트 허용 수',
            sf INT COMMENT '희생 플라이 허용 수',
            bb INT COMMENT '볼넷 허용 수',
            ibb INT COMMENT '고의 4구 허용 수',
            so INT COMMENT '삼진 수',
            wp INT COMMENT '폭투 수',
            bk INT COMMENT '보크 수',
            runs INT COMMENT '실점 수',
            er INT COMMENT '자책점 수',
            bsv INT COMMENT '블론 세이브 수',
            whip DECIMAL(5, 3) COMMENT 'WHIP (이닝당 허용 주자 수)',
            avg DECIMAL(5, 3) COMMENT '피안타율',
            qs INT COMMENT '퀄리티 스타트 수',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '데이터 생성 시각'
        ) COMMENT = '투수 기본 기록 통계 테이블';
                              
        CREATE TABLE IF NOT EXISTS pitcher_games (
            id INT AUTO_INCREMENT PRIMARY KEY COMMENT '게임별 고유 ID',
            pitcher_id INT COMMENT '투수 고유 ID',
            game_date DATE NOT NULL COMMENT '경기 날짜',
            opponent_team VARCHAR(50) COMMENT '상대 팀 이름',
            result VARCHAR(50) COMMENT '결과',
            era DECIMAL(5, 2) COMMENT '평균 자책점',
            tbf INT COMMENT '상대한 타자 수',
            ip VARCHAR(50) COMMENT '투구 이닝 수',
            hits INT COMMENT '피안타 수',
            hr INT COMMENT '피홈런 수',
            bb INT COMMENT '볼넷 허용 수',
            hbp INT COMMENT '사구',
            so INT COMMENT '삼진 수',
            runs INT COMMENT '실점 수',
            er INT COMMENT '자책점 수',
            avg DECIMAL(5, 3) COMMENT '피안타율',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '데이터 생성 시각',
            UNIQUE KEY unique_hitter_game (pitcher_id, game_date)  -- UNIQUE 제약 추가
        ) COMMENT = '경기별 투수 성적 테이블';
                              
        CREATE TABLE IF NOT EXISTS pitcher_opponents (
            id INT AUTO_INCREMENT PRIMARY KEY COMMENT '상대별 고유 ID',
            pitcher_id INT NOT NULL COMMENT '투수 고유 ID',
            opponent_team VARCHAR(50) COMMENT '상대 팀 이름',
            games INT COMMENT '경기수',
            era DECIMAL(5, 2) COMMENT '평균 자책점',
            wins INT COMMENT '승리 수',
            losses INT COMMENT '패배 수',
            sv INT COMMENT '세이브 수',
            hld INT COMMENT '홀드 수',
            wpct DECIMAL(5, 3) COMMENT '승률',
            tbf INT COMMENT '상대한 타자 수',
            ip VARCHAR(50) COMMENT '투구 이닝 수',
            hits INT COMMENT '피안타 수',
            hr INT COMMENT '피홈런 수',
            bb INT COMMENT '볼넷 허용 수',
            hbp INT COMMENT '사구',
            so INT COMMENT '삼진 수',
            runs INT COMMENT '실점 수',
            er INT COMMENT '자책점 수',
            avg DECIMAL(5, 3) COMMENT '피안타율',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '기록 저장 시각',
            UNIQUE KEY unique_hitter_opponent (pitcher_id, opponent_team)  -- UNIQUE 제약 추가
        ) COMMENT = '상대별 투수 성적 테이블';
                              
        CREATE TABLE IF NOT EXISTS pitcher_stadiums (
            id INT AUTO_INCREMENT PRIMARY KEY COMMENT '구장별 고유 ID',
            pitcher_id INT NOT NULL COMMENT '투수 고유 ID',
            stadium VARCHAR(50) COMMENT '구장',
            games INT COMMENT '경기수',
            era DECIMAL(5, 2) COMMENT '평균 자책점',
            wins INT COMMENT '승리 수',
            losses INT COMMENT '패배 수',
            sv INT COMMENT '세이브 수',
            hld INT COMMENT '홀드 수',
            wpct DECIMAL(5, 3) COMMENT '승률',
            tbf INT COMMENT '상대한 타자 수',
            ip VARCHAR(50) COMMENT '투구 이닝 수',
            hits INT COMMENT '피안타 수',
            hr INT COMMENT '피홈런 수',
            bb INT COMMENT '볼넷 허용 수',
            hbp INT COMMENT '사구',
            so INT COMMENT '삼진 수',
            runs INT COMMENT '실점 수',
            er INT COMMENT '자책점 수',
            avg DECIMAL(5, 3) COMMENT '피안타율',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '기록 저장 시각',
            UNIQUE KEY unique_hitter_opponent (pitcher_id, stadium)  -- UNIQUE 제약 추가
        ) COMMENT = '구장별 투수 성적 테이블';
                              
        CREATE TABLE IF NOT EXISTS game_records (
            id INT AUTO_INCREMENT PRIMARY KEY,
            game_date DATETIME NOT NULL,
            away_team VARCHAR(50) NOT NULL,
            away_score INT NOT NULL,
            home_team VARCHAR(50) NOT NULL,
            home_score INT NOT NULL,
            stadium VARCHAR(50) NOT NULL
        );
                              
        CREATE TABLE IF NOT EXISTS game_records_until_5_innings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            game_date TIMESTAMP,
            away_team VARCHAR(50),
            home_team VARCHAR(50),
            result INT
        );
                              
        CREATE TABLE IF NOT EXISTS today_games (
            game_date DATETIME NOT NULL,
            away_team VARCHAR(50),
            home_team VARCHAR(50),
            stadium VARCHAR(100),
            PRIMARY KEY (game_date, away_team)
        );
                              
        CREATE TABLE IF NOT EXISTS hitter_metrics (
            hitter_id INT PRIMARY KEY,
            wOBA FLOAT,
            wRC FLOAT,
            wRC_plus FLOAT,
            OPS_plus FLOAT,
            k_rate FLOAT,
            bb_rate FLOAT,
            BABIP FLOAT
        );
                              
        CREATE TABLE IF NOT EXISTS pitcher_metrics (
            pitcher_id INT PRIMARY KEY,
            FIP FLOAT,
            k_rate FLOAT,
            bb_rate FLOAT,
            hr_rate FLOAT
        );
                              
        CREATE TABLE IF NOT EXISTS hitter_records (
            -- 기본 ID 및 PK
            hitter_id INT NOT NULL,
            player_name VARCHAR(50),
            team_name VARCHAR(50),
            game_date TIMESTAMP,
            position INT,
            
            -- hitters 테이블의 고유 필드
            avg DECIMAL(5,3),
            games INT,
            pa INT,
            ab INT,
            runs INT,
            hits INT,
            doubles INT,
            triples INT,
            hr INT,
            rbi INT,
            sb INT,
            cs INT,
            sac INT,
            sf INT,
            bb INT,
            ibb INT,
            hbp INT,
            so INT,
            gdp INT,
            slg DECIMAL(5,3),
            obp DECIMAL(5,3),
            errors INT,
            sb_percentage DECIMAL(5,3),
            mh INT,
            ops DECIMAL(5,3),
            risp DECIMAL(5,3),
            ph_ba DECIMAL(5,3),
            
            -- hitter_metrics 테이블의 고유 필드 (OPS는 hitters와 중복되므로 제외)
            wOBA FLOAT,
            wRC FLOAT,
            wRC_plus FLOAT,
            OPS_plus FLOAT,
            k_rate FLOAT,
            bb_rate FLOAT,
            BABIP FLOAT,
            
            -- opponent 정보 (opponent_ 접두사 사용)
            opponent_team VARCHAR(50),
            opponent_games INT,
            opponent_avg DECIMAL(5,3),
            opponent_pa INT,
            opponent_ab INT,
            opponent_runs INT,
            opponent_hits INT,
            opponent_doubles INT,
            opponent_triples INT,
            opponent_hr INT,
            opponent_rbi INT,
            opponent_sb INT,
            opponent_cs INT,
            opponent_bb INT,
            opponent_hbp INT,
            opponent_so INT,
            opponent_gdp INT,
            
            -- stadium 정보 (stadium_ 접두사 사용)
            stadium VARCHAR(50),
            stadium_games INT,
            stadium_avg DECIMAL(5,3),
            stadium_pa INT,
            stadium_ab INT,
            stadium_runs INT,
            stadium_hits INT,
            stadium_doubles INT,
            stadium_triples INT,
            stadium_hr INT,
            stadium_rbi INT,
            stadium_sb INT,
            stadium_cs INT,
            stadium_bb INT,
            stadium_hbp INT,
            stadium_so INT,
            stadium_gdp INT,
            
            -- 최근 5경기 게임 기록 CSV 파일 저장 경로
            recent_games_file_path TEXT,
            UNIQUE hitter_records_key (hitter_id, game_date)
        );

        CREATE TABLE IF NOT EXISTS pitcher_records (
            pitcher_id INT NOT NULL,
            player_name VARCHAR(50),
            team_name VARCHAR(50),
            game_date TIMESTAMP,
            
            -- pitchers 테이블 컬럼
            era DECIMAL(5,2),
            games INT,
            cg INT,
            sho INT,
            wins INT,
            losses INT,
            sv INT,
            hld INT,
            wpct DECIMAL(5,3),
            tbf INT,
            np INT,
            ip VARCHAR(50),
            hits INT,
            doubles INT,
            triples INT,
            hr INT,
            sac INT,
            sf INT,
            bb INT,
            ibb INT,
            so INT,
            wp INT,
            bk INT,
            runs INT,
            er INT,
            bsv INT,
            whip DECIMAL(5,3),
            avg DECIMAL(5,3),
            qs INT,
            
            -- pitcher_metrics 테이블 컬럼
            FIP FLOAT,
            k_rate FLOAT,
            bb_rate FLOAT,
            hr_rate FLOAT,
            
            -- pitcher_opponents 테이블 컬럼 (opponent_ 접두사 추가)
            opponent_team VARCHAR(50),
            opponent_games INT,
            opponent_era DECIMAL(5,2),
            opponent_wins INT,
            opponent_losses INT,
            opponent_sv INT,
            opponent_hld INT,
            opponent_wpct DECIMAL(5,3),
            opponent_tbf INT,
            opponent_ip VARCHAR(50),
            opponent_hits INT,
            opponent_hr INT,
            opponent_bb INT,
            opponent_hbp INT,
            opponent_so INT,
            opponent_runs INT,
            opponent_er INT,
            opponent_avg DECIMAL(5,3),
            
            -- pitcher_stadiums 테이블 컬럼 (stadiums_ 접두사 추가)
            stadium VARCHAR(50),
            stadium_games INT,
            stadium_era DECIMAL(5,2),
            stadium_wins INT,
            stadium_losses INT,
            stadium_sv INT,
            stadium_hld INT,
            stadium_wpct DECIMAL(5,3),
            stadium_tbf INT,
            stadium_ip VARCHAR(50),
            stadium_hits INT,
            stadium_hr INT,
            stadium_bb INT,
            stadium_hbp INT,
            stadium_so INT,
            stadium_runs INT,
            stadium_er INT,
            stadium_avg DECIMAL(5,3),
            
            -- 최근 5경기 게임 기록 CSV 파일 저장 경로
            recent_games_file_path TEXT,
            UNIQUE pitcher_records_key (pitcher_id, game_date)
        );
    """
    
    with engine.connect() as conn:
        for stmt in drop_table_query.strip().split(';'):
            if stmt.strip():
                conn.execute(text(stmt.strip()))

        for stmt in create_table_query.strip().split(';'):
            if stmt.strip():
                conn.execute(text(stmt.strip()))

# ---------------------- DAG 정의 ----------------------

with DAG(
    dag_id='0_init_baseball_table',
    default_args=default_args,
    description='테이블 초기화 DAG',
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

    # Task 의존성 설정
    initialize_table