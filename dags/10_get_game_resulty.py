from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import List, Tuple, Optional
import re

import mysql.connector
from mysql.connector import pooling
from playwright.async_api import async_playwright, Page

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

# 데이터베이스 연결 풀 설정
class DatabaseManager:
    def __init__(self, host, user, password, database):
        self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="baseball_pool",
            pool_size=5,
            pool_reset_session=True,
            host=host,
            user=user,
            password=password,
            database=database
        )

    @asynccontextmanager
    async def get_connection(self):
        conn = self.connection_pool.get_connection()
        try:
            yield conn
        finally:
            conn.close()

    async def execute_insert(self, query: str, data: Tuple):
        async with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query, data)
                conn.commit()
                # logger.info("데이터 삽입 성공")
            except mysql.connector.Error as err:
                logger.error(f"데이터베이스 오류: {err}")
                raise
            finally:
                cursor.close()

class BaseballDataScraper:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.count = 0

    def _get_insert_query(self, table_name: str) -> str:
        """각 테이블별 upsert 쿼리를 반환하는 메서드"""
        queries = {
            "game_records": """
            INSERT INTO game_records (
                game_date, away_team, home_team, result
            ) VALUES (
                %s, %s, %s, %s
            )
            """
        }
        return queries.get(table_name, "")

    async def insert_data(self, table_name: str, data: Tuple):
        """데이터 삽입 메서드"""
        query = self._get_insert_query(table_name)
        await self.db_manager.execute_insert(query, data)

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

    async def run(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
            await self.goto_with_retry(page, url)

            date_element = await page.wait_for_selector('//*[@id="lblGameDate"]')
            date = await date_element.text_content()
            # 요일 제거 및 파싱
            date = date.split('(')[0]

            # 각 li에 대해 개별적으로 XPath를 사용하여 찾고 클릭
            ul_xpath = '//*[@id="contents"]/div[3]/div/div[1]/ul'

            # 먼저 li 요소 개수를 확인
            ul = await page.query_selector(f'xpath={ul_xpath}')
            li_elements = await ul.query_selector_all('li[class*="game-cont"]')
            li_count = len(li_elements)
            
            for i in range(1, li_count + 1):  # XPath에서는 인덱스가 1부터 시작
                game_status_element = await page.wait_for_selector(f'//*[@id="contents"]/div[3]/div/div[1]/ul/li[{i}]/div[2]/p')
                game_status = await game_status_element.text_content()
                if game_status == "경기취소":
                    continue
                # 매번 새롭게 특정 인덱스의 li 요소를 찾음
                li_xpath = f'{ul_xpath}/li[{i}]'
                try:
                    # 명시적으로 대기한 후 요소 찾기
                    li = await page.wait_for_selector(f'xpath={li_xpath}', timeout=5000)
                    if li:
                        await li.click()
                except Exception as e:
                    print(f"{i}번째 요소를 찾거나 클릭하는 중 오류 발생: {e}")

                game_time_element = await page.wait_for_selector(f'//*[@id="contents"]/div[3]/div/div[1]/ul/li[{i}]/div[1]/ul/li[3]')
                game_time = await game_time_element.text_content()

                review_button = await page.wait_for_selector('//*[@id="tabDepth2"]/li[2]/a')
                await review_button.click()

                away_score_sum, home_score_sum = 0, 0
                for i in range(1, 6):
                    away_score_element = await page.wait_for_selector(f'//*[@id="tblScordboard2"]/tbody/tr[1]/td[{i}]')
                    away_score = await away_score_element.text_content()
                    away_score_sum += int(away_score)

                    home_score_element = await page.wait_for_selector(f'//*[@id="tblScordboard2"]/tbody/tr[2]/td[{i}]')
                    home_score = await home_score_element.text_content()
                    home_score_sum += int(home_score)

                highlite_element = await page.wait_for_selector('//*[@id="tabDepth2"]/li[3]/a')
                await highlite_element.click()

                teams_element = await page.wait_for_selector('//*[@id="txtTitle"]')
                teams = await teams_element.text_content()

                datetime_str = f"{date} {game_time}"
                # 문자열을 datetime 객체로 파싱
                dt = datetime.strptime(datetime_str, "%Y.%m.%d %H:%M")

                away_team, home_team = teams.split("VS")[0].strip(), teams.split("VS")[1].strip()

                # game_records 테이블에 dt, away_team, away_score_sum, home_team, home_score_sum 삽입하는 코드
                if away_score_sum > home_score_sum:
                    result = -1
                elif away_score_sum == home_score_sum:
                    result = 0
                else:
                    result = 1
                    
                game_data = (dt, away_team, home_team, result)
                await self.insert_data("game_records", game_data)

            await browser.close()

# Airflow에서 실행할 비동기 함수를 동기 함수로 감싸기
def scrape_baseball_data():
    db_config = {
        "host": "116.37.91.221",
        "user": "niscom",
        "password": "niscom",
        "database": "baseball",
    }

    db_manager = DatabaseManager(**db_config)
    scraper = BaseballDataScraper(db_manager)
    
    # 비동기 함수를 동기적으로 실행
    asyncio.run(scraper.run())

# Airflow DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'baseball_data_scraper',
    default_args=default_args,
    description='야구 경기 데이터 스크래핑 DAG',
    schedule_interval='0 22 * * *',  # 매일 밤 10시에 실행
    start_date=datetime(2025, 4, 18),
    catchup=False,
    tags=['baseball', 'scraping'],
)

# 태스크 정의
scrape_task = PythonOperator(
    task_id='scrape_baseball_data',
    python_callable=scrape_baseball_data,
    dag=dag,
)

# 태스크 의존성 설정 (현재는 단일 태스크)
# 추가 태스크가 있다면 여기에 의존성 설정
# example: scrape_task >> process_task

if __name__ == "__main__":
    dag.cli()