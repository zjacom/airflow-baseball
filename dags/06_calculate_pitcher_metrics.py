from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'baseball_pitcher_metrics',
    default_args=default_args,
    description='야구 투수 지표(FIP, K율, BB율, HR율) 계산 DAG',
    schedule_interval=None,
    start_date=datetime(2025, 4, 1),
    catchup=False,
)

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

def calculate_league_metrics(**context):
    """리그 전체 지표 계산"""
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
    
    # XCom을 통해 다음 태스크에 데이터 전달
    context['ti'].xcom_push(key='league_stats', value={
        'fip_constant': fip_constant,
        'pitcher_stats': select_pitcher_stats_results
    })
    
    return "리그 지표 계산 완료"

def update_pitcher_metrics(**context):
    """개별 투수 지표 계산 및 데이터베이스 업데이트"""
    # XCom에서 이전 태스크의 결과 가져오기
    league_stats = context['ti'].xcom_pull(task_ids='calculate_league_metrics', key='league_stats')
    fip_constant = league_stats['fip_constant']
    select_pitcher_stats_results = league_stats['pitcher_stats']
    
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
    
    upsert_pitcher_metrics_query = text("""
        INSERT INTO pitcher_metrics (pitcher_id, FIP, k_rate, bb_rate, hr_rate)
        VALUES (:pitcher_id, :FIP, :k_rate, :bb_rate, :hr_rate)
        ON DUPLICATE KEY UPDATE
            FIP = VALUES(FIP),
            k_rate = VALUES(k_rate),
            bb_rate = VALUES(bb_rate),
            hr_rate = VALUES(hr_rate);
    """)

    success_count = 0
    skip_count = 0

    for row in select_pitcher_stats_results:
        pitcher_id, hr, bb, so, ip_str, _ = row
        ip = convert_pitcher_ip_to_float(ip_str)
        if ip == 0:
            skip_count += 1
            continue
        
        fip = (((13 * int(hr)) + (3 * int(bb)) - (2 * int(so))) / ip) + fip_constant
        k_rate = (int(so) * 9) / ip
        bb_rate = (int(bb) * 9) / ip
        hr_rate = (int(hr) * 9) / ip

        data = {"pitcher_id": pitcher_id, "FIP": fip, "k_rate": k_rate, "bb_rate": bb_rate, "hr_rate": hr_rate}
        
        with engine.begin() as conn:
            conn.execute(upsert_pitcher_metrics_query, data)
            success_count += 1
    
    return f"투수 지표 업데이트 완료: {success_count}개 성공, {skip_count}개 스킵"

calculate_league_task = PythonOperator(
    task_id='calculate_league_metrics',
    python_callable=calculate_league_metrics,
    provide_context=True,
    dag=dag,
)

update_metrics_task = PythonOperator(
    task_id='update_pitcher_metrics',
    python_callable=update_pitcher_metrics,
    provide_context=True,
    dag=dag,
)

# 태스크 순서 설정
calculate_league_task >> update_metrics_task