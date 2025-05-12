from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from sqlalchemy import text

from utils.create_db_connection import get_sync_db_connection

default_args = {
    'owner': 'niscom',
    'depends_on_past': False,
}

def convert_pitcher_ip_to_float(ip_str):
    """투수의 이닝 문자열을 소수점 형태로 변환"""
    ip_str = ip_str.strip()
    
    if " " in ip_str:
        # "정수 분수" 형태
        whole, frac = ip_str.split()
        numerator, denominator = frac.split("/")
        return int(whole) + int(numerator) / int(denominator)
    elif "/" in ip_str:
        # "분수" 형태
        numerator, denominator = ip_str.split("/")
        return int(numerator) / int(denominator)
    else:
        # 그냥 정수
        return float(ip_str)

def _process_pitchers_metric():
    engine = get_sync_db_connection()

    select_pitcher_stats_query = text("""
        SELECT pitcher_id, hr, bb, so, ip, era
        FROM pitchers;
    """)

    with engine.connect() as conn:
        select_pitcher_stats_results = list(conn.execute(select_pitcher_stats_query))

    league_hr = 0
    league_bb = 0
    league_so = 0
    league_ip = 0
    league_era = 0

    error_count = 0

    for row in select_pitcher_stats_results:
        pitcher_id, hr, bb, so, ip_str, era = row
        ip = convert_pitcher_ip_to_float(ip_str)
        league_hr += int(hr)
        league_bb += int(bb)
        league_so += int(so)
        league_ip += ip
        try:
            league_era += float(era)
        except:
            error_count += 1
            print(f"스탯 오류 발생!: {pitcher_id}")
            continue
    league_era = league_era / (len(select_pitcher_stats_results) - error_count)

    fip_constant = league_era - (((13 * league_hr) + (3 * league_bb) - (2 * league_so)) / league_ip)

    upsert_pitcher_metrics_query = text("""
        INSERT INTO pitcher_metrics (pitcher_id, FIP, k_rate, bb_rate, hr_rate)
        VALUES (:pitcher_id, :FIP, :k_rate, :bb_rate, :hr_rate)
        ON DUPLICATE KEY UPDATE
            FIP = VALUES(FIP),
            k_rate = VALUES(k_rate),
            bb_rate = VALUES(bb_rate),
            hr_rate = VALUES(hr_rate);
    """)

    for row in select_pitcher_stats_results:
        pitcher_id, hr, bb, so, ip_str, _ = row
        ip = convert_pitcher_ip_to_float(ip_str)
        if ip == 0:
            continue
        fip = (((13 * int(hr)) + (3 * int(bb)) - (2 * int(so))) / ip) + fip_constant
        k_rate = (int(so) * 9) / ip
        bb_rate = (int(bb) * 9) / ip
        hr_rate = (int(hr) * 9) / ip

        data = {"pitcher_id": pitcher_id, "FIP": fip, "k_rate": k_rate, "bb_rate": bb_rate, "hr_rate": hr_rate}
        with engine.begin() as conn:
            conn.execute(upsert_pitcher_metrics_query, data)

with DAG(
    dag_id='021_calculate_pitcher_metrics',
    default_args=default_args,
    description='투수 지표를 계산하는 DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # Task 정의
    process_pitchers_metric = PythonOperator(
        task_id='process_pitchers_metric',
        python_callable=_process_pitchers_metric,
        dag=dag,
    )
    # Trigger DAG 정의
    trigger_scrape_today_games_time_dag = TriggerDagRunOperator(
        task_id='trigger_scrape_today_games_time_dag',
        trigger_dag_id='030_scrape_today_games_time',
        wait_for_completion=False,
    )

    # Task 의존성 설정
    process_pitchers_metric >> trigger_scrape_today_games_time_dag