from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import List, Tuple

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

    async def execute_query(self, query: str, params=None):
        async with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
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

    def _get_drop_query(self, table_name: str) -> str:
        queries = {
            "today_lineup": "DROP TABLE IF EXISTS today_lineup;"
        }
        return queries.get(table_name, "")

    def _get_create_query(self, table_name: str) -> str:
        queries = {
            "today_lineup": """CREATE TABLE IF NOT EXISTS today_lineup (
                id INT AUTO_INCREMENT PRIMARY KEY,
                game_date TIMESTAMP,
                player VARCHAR(50),
                team VARCHAR(50),
                position INT,
                opponent VARCHAR(50),
                stadium VARCHAR(50)
            );"""
        }
        return queries.get(table_name, "")
    
    def _get_insert_query(self, table_name: str) -> str:
        """각 테이블별 upsert 쿼리를 반환하는 메서드"""
        queries = {
            "today_lineup": """
            INSERT INTO today_lineup (
                game_date, player, team, position, opponent, stadium
            ) VALUES (
                %s, %s, %s, %s, %s, %s
            )
            """
        }
        return queries.get(table_name, "")

    async def insert_data(self, table_name: str, data: Tuple):
        """데이터 삽입/업데이트 메서드"""
        query = self._get_insert_query(table_name)
        await self.db_manager.execute_query(query, data)

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

    async def refresh_table(self, table_name: str):
        """테이블을 완전히 새로 만드는 메서드"""
        drop_query = self._get_drop_query(table_name)
        create_query = self._get_create_query(table_name)
        
        await self.db_manager.execute_query(drop_query)
        await self.db_manager.execute_query(create_query)
        logger.info(f"{table_name} 테이블 리프레시 완료")

    async def run(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Airflow에서는 headless 모드 사용
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
            
            for i in range(1, li_count + 1):
                # 매번 새롭게 특정 인덱스의 li 요소를 찾음
                li_xpath = f'{ul_xpath}/li[{i}]'
                game_status_element = await page.wait_for_selector(f'//*[@id="contents"]/div[3]/div/div[1]/ul/li[{i}]/div[2]/p')
                game_status = await game_status_element.text_content()
                if game_status == "경기취소":
                    continue
                try:
                    # 명시적으로 대기한 후 요소 찾기
                    li = await page.wait_for_selector(f'xpath={li_xpath}', timeout=5000)
                    if li:
                        await li.click()
                except Exception as e:
                    logger.error(f"{i}번째 요소를 찾거나 클릭하는 중 오류 발생: {e}")
                    continue
                
                stadium_element = await page.wait_for_selector(f'//*[@id="contents"]/div[3]/div/div[1]/ul/li[{i}]/div[1]/ul/li[1]')
                game_time_element = await page.wait_for_selector(f'//*[@id="contents"]/div[3]/div/div[1]/ul/li[{i}]/div[1]/ul/li[3]')
                home_pitcher_element = await page.wait_for_selector(f'//*[@id="contents"]/div[3]/div/div[1]/ul/li[{i}]/div[2]/div[2]/div[1]/div[2]/p')
                away_pitcher_element = await page.wait_for_selector(f'//*[@id="contents"]/div[3]/div/div[1]/ul/li[{i}]/div[2]/div[2]/div[3]/div[2]/p')

                line_up_button = await page.wait_for_selector('//*[@id="tabPreview"]/li[3]/a')
                await line_up_button.click()

                is_registered_element = await page.wait_for_selector('//*[@id="txtLineUp"]')
                is_registered = await is_registered_element.text_content()
                logger.info(is_registered)
                if "라인업 발표 전" in is_registered:
                    continue

                away_team_element = await page.wait_for_selector('//*[@id="txtAwayTeam"]')
                home_team_element = await page.wait_for_selector('//*[@id="txtHomeTeam"]')

                stadium = await stadium_element.text_content()
                game_time = await game_time_element.text_content()
                home_pitcher = await home_pitcher_element.text_content()
                away_pitcher = await away_pitcher_element.text_content()
                away_team = await away_team_element.text_content()
                home_team = await home_team_element.text_content()

                home_pitcher, away_pitcher = home_pitcher.replace("선", "").strip(), away_pitcher.replace("선", "").strip()
                
                away_hitters, home_hitters = [], []
                for j in range(1, 10):
                    away_hitter_element = await page.wait_for_selector(f'//*[@id="tblAwayLineUp"]/tbody/tr[{j}]')
                    away_hitter_td_elements = await away_hitter_element.query_selector_all("td")
                    away_hitter = []
                    for idx, td in enumerate(away_hitter_td_elements):
                        if idx % 2 == 1:
                            continue
                        text = await td.inner_text()
                        away_hitter.append(text)
                    away_hitters.append(tuple(away_hitter))
                    
                    home_hitter_element = await page.wait_for_selector(f'//*[@id="tblHomeLineUp"]/tbody/tr[{j}]')
                    home_hitter_td_elements = await home_hitter_element.query_selector_all("td")
                    home_hitter = []
                    for idx, td in enumerate(home_hitter_td_elements):
                        if idx % 2 == 1:
                            continue
                        text = await td.inner_text()
                        home_hitter.append(text)
                    home_hitters.append(tuple(home_hitter))

                datetime_str = f"{date} {game_time}"
                # 문자열을 datetime 객체로 파싱
                dt = datetime.strptime(datetime_str, "%Y.%m.%d %H:%M")
                
                for order, name in away_hitters:
                    data = (dt, name, away_team, order, home_team, stadium)
                    await self.insert_data("today_lineup", data)
                
                for order, name in home_hitters:
                    data = (dt, name, home_team, order, away_team, stadium)
                    await self.insert_data("today_lineup", data)

                await self.insert_data("today_lineup", (dt, home_pitcher, home_team, 0, away_team, stadium))
                await self.insert_data("today_lineup", (dt, away_pitcher, away_team, 0, home_team, stadium))
                
            await browser.close()

# Airflow에서 실행할 함수
def run_baseball_scraper():
    db_config = {
        "host": "116.37.91.221",
        "user": "niscom",
        "password": "niscom",
        "database": "baseball",
    }

    async def main():
        db_manager = DatabaseManager(**db_config)
        scraper = BaseballDataScraper(db_manager)
        
        # 테이블 리프레시
        await scraper.refresh_table("today_lineup")
        
        await scraper.run()

    # asyncio.run을 사용하여 비동기 함수 실행
    asyncio.run(main())

# Airflow DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'baseball_lineup_scraper',
    default_args=default_args,
    description='KBO 야구 라인업 데이터 스크래핑 DAG',
    schedule_interval=None,  # 매일 오전 8시 실행
    start_date=datetime(2025, 4, 18),
    catchup=False,
    tags=['baseball', 'scraping'],
)

# 태스크 정의
scrape_task = PythonOperator(
    task_id='scrape_baseball_lineups',
    python_callable=run_baseball_scraper,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()