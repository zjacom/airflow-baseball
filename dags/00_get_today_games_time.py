from datetime import datetime, timedelta
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import List, Tuple, Optional
import re

import mysql.connector
from mysql.connector import pooling
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Playwright 임포트는 Airflow 작업자에 설치되어 있어야 함
from playwright.async_api import async_playwright, Page

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# 기본 DAG 인수
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 데이터베이스 연결 관리자 클래스
class DatabaseManager:
    def __init__(self, conn_id='baseball_db'):
        # Airflow의 MySqlHook을 사용하여 연결 정보 가져오기
        conn = BaseHook.get_connection(conn_id)
        self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="baseball_pool",
            pool_size=5,
            pool_reset_session=True,
            host=conn.host,
            port=conn.port or 3306,
            user=conn.login,
            password=conn.password,
            database=conn.schema
        )

    @asynccontextmanager
    async def get_connection(self):
        conn = self.connection_pool.get_connection()
        try:
            yield conn
        finally:
            conn.close()

    async def execute_upsert(self, query: str, data: Tuple):
        async with self.get_connection() as conn:
            try:
                print("데이터 삽입을 시작합니다")
                cursor = conn.cursor()
                cursor.execute(query, data)
                conn.commit()
            except mysql.connector.Error as err:
                logger.error(f"데이터베이스 오류: {err}")
                raise
            finally:
                cursor.close()

    async def execute(self, query: str):
        async with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                conn.commit()
            except mysql.connector.Error as err:
                logger.error(f"데이터베이스 오류: {err}")
                raise
            finally:
                cursor.close()

class BaseballDataScraper:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.count = 0

    def parse_bundle(self, game_result: str):
        match = re.match(r"(.+?)vs(.+)", game_result)
        if match:
            away_team, home_team = match.groups()
            return away_team.strip(), home_team.strip()
        return None  # 매칭 실패 시 None 반환
    
    def parse_game_datetime(self, date_str: str):
        # 정규식으로 한글 요일 제거 (예: (토) → "")
        date_str = re.sub(r"\([가-힣]\)", "", date_str)

        # 문자열을 datetime 객체로 변환
        date_obj = datetime.strptime(date_str, "%m.%d%H:%M")

        # 연도 추가 (현재 연도 기준)
        current_year = datetime.now().year
        formatted_date = date_obj.replace(year=current_year)

        return formatted_date.strftime("%Y-%m-%d %H:%M:%S")
    
    def check_date_is_today(self, date_str: str, today): 
        # 정규식으로 한글 요일 제거 (예: (토) → "")
        date_str = re.sub(r"\([가-힣]\)", "", date_str)
        # 현재 연도 가져오기
        current_year = datetime.now().year

        # '04.18' → '2025-04-18' 형식으로 변환
        formatted_date = f"{current_year}-{date_str.replace('.', '-')}"

        if today != formatted_date:
            return False
        return True

    def _get_upsert_query(self, table_name: str) -> str:
        """각 테이블별 upsert 쿼리를 반환하는 메서드"""
        queries = {
            "today_games": """
                INSERT INTO today_games (
                    game_date, away_team, home_team, stadium
                ) VALUES (
                    %s, %s, %s, %s
                );
            """
        }
        return queries.get(table_name, "")
    
    async def initialize_table(self, table_name: str):
        """테이블이 존재하면 삭제하고, 다시 생성하는 메서드"""
        drop_query = f"DROP TABLE IF EXISTS {table_name}"
        
        create_queries = {
            "today_games": f"""
            CREATE TABLE today_games (
                game_date DATETIME NOT NULL,
                away_team VARCHAR(50),
                home_team VARCHAR(50),
                stadium VARCHAR(100),
                PRIMARY KEY (game_date, away_team)
            )
            """
        }

        create_query = create_queries.get(table_name)
        if not create_query:
            raise ValueError(f"Unknown table name: {table_name}")

        print(f"[INIT] Dropping table '{table_name}' if exists...")
        await self.db_manager.execute(drop_query)
        print(f"[INIT] Creating table '{table_name}'...")
        await self.db_manager.execute(create_query)

    async def upsert_data(self, table_name: str, data: Tuple):
        """데이터 삽입/업데이트 메서드"""
        query = self._get_upsert_query(table_name)
        print(query)
        await self.db_manager.execute_upsert(query, data)

    async def goto_with_retry(self, page: Page, url: str, max_retries: int = 3):
        for attempt in range(max_retries):
            try:
                await page.goto(url, wait_until="load", timeout=30000)  # 30초 타임아웃 설정
                return True
            except Exception as e:
                if attempt == max_retries - 1:  # 마지막 시도였다면
                    logger.error(f"페이지 로드 실패 (최종 실패): {url}, 오류: {e}")
                    raise  # 마지막 시도에서도 실패하면 예외를 발생시킴
                logger.warning(f"페이지 로드 실패 (재시도 {attempt + 1}/{max_retries}): {url}, 오류: {e}")
                await asyncio.sleep(2)  # 재시도 전 2초 대기

    async def process_player_data(self, page: Page, today):
        # 스케줄 데이터 처리
        url = f"https://www.koreabaseball.com/Schedule/Schedule.aspx"
        await self.goto_with_retry(page, url)
        
        # 필요한 경우 월 선택 (주석 처리된 부분)
        # await page.select_option('//*[@id="ddlMonth"]', "03")
        # await asyncio.sleep(5)
        
        game_schedule = page.locator('//*[@id="tblScheduleList"]/tbody')
        game_schedule_elements = game_schedule.locator('tr')
        
        date = ""  # 날짜 초기화

        await self.initialize_table(table_name="today_games")
        
        for i in range(await game_schedule_elements.count()):
            tr_row = game_schedule_elements.nth(i)
            td_elements = tr_row.locator('td')
            td_count = await td_elements.count()

            if td_count == 9:  # 새로운 날짜가 포함된 행
                date = await td_elements.nth(0).inner_text()
                if not self.check_date_is_today(date_str=date, today=today):
                    continue
                print(date, today)
                time = await td_elements.nth(1).inner_text()
                bundle = await td_elements.nth(2).inner_text()
                parsed_bundle = self.parse_bundle(bundle)
                if parsed_bundle is None:
                    print("No BUNDLE")
                    continue
                else:
                    away_team, home_team = parsed_bundle
                stadium = await td_elements.nth(7).inner_text()
                timestamp = self.parse_game_datetime(date + time)
                await self.upsert_data("today_games", (timestamp, away_team, home_team, stadium))
            elif td_count == 8:  # 같은 날짜의 다른 경기
                if not self.check_date_is_today(date_str=date, today=today):
                    continue
                print(date, today)
                time = await td_elements.nth(0).inner_text()
                bundle = await td_elements.nth(1).inner_text()
                parsed_bundle = self.parse_bundle(bundle)
                if parsed_bundle is None:
                    print("No BUNDLE")
                    continue
                else:
                    away_team, home_team = parsed_bundle
                stadium = await td_elements.nth(6).inner_text()
                timestamp = self.parse_game_datetime(date + time)
                await self.upsert_data("today_games", (timestamp, away_team, home_team, stadium))

    async def run(self, today):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await self.process_player_data(page, today=today)
            await browser.close()

# Airflow 작업을 위한 실행 함수
def scrape_baseball_data(**kwargs):
    # 데이터베이스 연결 정보는 Airflow connections에서 설정 필요
    conn_id = kwargs.get('conn_id', 'baseball_db')
    db_manager = DatabaseManager(conn_id=conn_id)
    scraper = BaseballDataScraper(db_manager)

    # 실행 시간 가져오기
    execution_time = kwargs['execution_date']  # pendulum.datetime 타입

    # 33시간 추가
    future_time = execution_time.add(hours=9)

    # 'YYYY-MM-DD' 형식으로 변환
    future_date_str = future_time.to_date_string()  # 예: '2025-04-20'

    # 이후 로직에서 사용 가능
    print(f"실제 사용할 기준 날짜: {future_date_str}")
    
    # asyncio 실행
    asyncio.run(scraper.run(today=future_date_str))
    return "스크래핑 작업 완료"

# DAG 정의
dag = DAG(
    'get_today_games_time',
    default_args=default_args,
    description='KBO 야구 일정 데이터 스크래핑',
    schedule_interval=None,  # 6시간마다 실행 (필요에 따라 조정)
    start_date=days_ago(1),
)

# 작업 정의
scrape_task = PythonOperator(
    task_id='scrape_baseball_data',
    python_callable=scrape_baseball_data,
    op_kwargs={'conn_id': 'baseball_db'},
    dag=dag,
)