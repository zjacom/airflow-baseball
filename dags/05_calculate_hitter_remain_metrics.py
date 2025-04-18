from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text

# DAG의 기본 인수 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 야구 선수 지표를 계산하는 함수
def calculate_baseball_metrics():
    # DB 접속 설정
    db_config = {
        'user': 'niscom',
        'password': 'niscom',
        'host': '116.37.91.221',
        'port': 3306,
        'database': 'baseball'
    }

    # SQLAlchemy 연결 문자열
    engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
                        f"{db_config['host']}:{db_config['port']}/{db_config['database']}")

    select_hitter_stats_query = text("""
        SELECT hitter_id, so, bb, pa, hits, hr, ab, sf, ops
        FROM hitters;
    """)

    with engine.connect() as conn:
        select_hitter_stats_results = conn.execute(select_hitter_stats_query)
        rows = select_hitter_stats_results.fetchall()

    upsert_hitter_remain_metrics_query = text("""
        INSERT INTO hitter_metrics (hitter_id, OPS, k_rate, bb_rate, BABIP)
        VALUES (:hitter_id, :OPS, :k_rate, :bb_rate, :BABIP)
        ON DUPLICATE KEY UPDATE
            OPS = VALUES(OPS),
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

        data = {"hitter_id": hitter_id, "OPS": ops, "k_rate": k_rate, "bb_rate": bb_rate, "BABIP": babip}
        with engine.begin() as conn:
            conn.execute(upsert_hitter_remain_metrics_query, data)

# DAG 정의
dag = DAG(
    'baseball_hitter_metrics',
    default_args=default_args,
    description='야구 타자 지표 계산 및 업데이트 DAG',
    schedule_interval=None,  # 매일 실행
    start_date=datetime(2025, 4, 18),
    catchup=False
)

# 태스크 정의
calculate_metrics_task = PythonOperator(
    task_id='calculate_baseball_metrics',
    python_callable=calculate_baseball_metrics,
    dag=dag,
)

# 태스크 의존성 설정 (추가 태스크가 있으면 여기에 설정)
# 예: task1 >> task2 >> task3

if __name__ == "__main__":
    dag.cli()