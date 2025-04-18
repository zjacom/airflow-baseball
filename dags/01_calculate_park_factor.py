from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import pandasql as ps
from sqlalchemy import create_engine, text

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'baseball_park_factor_calculation',
    default_args=default_args,
    description='야구장별 Park Factor 계산 및 저장',
    schedule_interval=None,
    start_date=datetime(2025, 4, 18),
    catchup=False,
)

def get_db_connection():
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

def drop_and_create_table():
    engine = get_db_connection()
    
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

def calculate_and_insert_park_factors():
    engine = get_db_connection()
    
    # game_schedule 데이터 읽기
    df = pd.read_sql("SELECT * FROM game_schedule", engine)
    
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


# Task 정의
task_drop_create_table = PythonOperator(
    task_id='drop_and_create_table',
    python_callable=drop_and_create_table,
    dag=dag,
)

task_calculate_insert = PythonOperator(
    task_id='calculate_and_insert_park_factors',
    python_callable=calculate_and_insert_park_factors,
    dag=dag,
)

# Task 의존성 설정
task_drop_create_table >> task_calculate_insert