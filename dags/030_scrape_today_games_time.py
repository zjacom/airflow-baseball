from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import asyncio
from datetime import datetime
import re
from sqlalchemy import text

from utils.create_db_connection import get_async_db_connection
from playwright.async_api import async_playwright, Page

# 기본 DAG 인수
default_args = {
    'owner': 'niscom',
    'depends_on_past': False,
}

# ---------------------- 유틸 함수 정의 ----------------------

def parse_bundle(game_result: str):
    match = re.match(r"(.+?)vs(.+)", game_result)
    if match:
        away_team, home_team = match.groups()
        return away_team.strip(), home_team.strip()
    return None

def parse_game_datetime(date_str: str):
    date_str = re.sub(r"\([가-힣]\)", "", date_str)
    date_obj = datetime.strptime(date_str, "%m.%d%H:%M")
    current_year = datetime.now().year
    formatted_date = date_obj.replace(year=current_year)
    return formatted_date.strftime("%Y-%m-%d %H:%M:%S")

def check_date_is_today(date_str: str, today): 
    date_str = re.sub(r"\([가-힣]\)", "", date_str)
    current_year = datetime.now().year
    formatted_date = f"{current_year}-{date_str.replace('.', '-')}"
    return today == formatted_date

# ---------------------- 비동기 DB 함수 정의 ----------------------

async def initialize_table(table_name: str, async_session):
    async with async_session() as session:
        await session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        await session.execute(text("""
            CREATE TABLE today_games (
                game_date DATETIME NOT NULL,
                away_team VARCHAR(50),
                home_team VARCHAR(50),
                stadium VARCHAR(100),
                PRIMARY KEY (game_date, away_team)
            )
        """))
        await session.commit()

async def insert_data(async_session, data_tuple):
    columns = ["game_date", "away_team", "home_team", "stadium"]
    data = dict(zip(columns, data_tuple))
    query = text("""
        INSERT INTO today_games (
            game_date, away_team, home_team, stadium
        ) VALUES (
            :game_date, :away_team, :home_team, :stadium
        )
    """)
    async with async_session() as session:
        await session.execute(query, data)
        await session.commit()

async def goto_with_retry(page: Page, url: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            await page.goto(url, wait_until="load", timeout=30000)
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"페이지 로드 실패 (최종): {url}, 오류: {e}")
                raise
            print(f"페이지 로드 실패 (재시도 {attempt + 1}/{max_retries}): {url}, 오류: {e}")
            await asyncio.sleep(2)

# ---------------------- 크롤링 로직 ----------------------

async def crawling(page: Page, today):
    engine, async_session = await get_async_db_connection()
    await initialize_table("today_games", async_session)

    url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
    await goto_with_retry(page, url)

    schedule = page.locator('//*[@id="tblScheduleList"]/tbody')
    rows = schedule.locator('tr')
    date = ""

    for i in range(await rows.count()):
        row = rows.nth(i)
        cells = row.locator('td')
        count = await cells.count()

        if count == 9:
            date = await cells.nth(0).inner_text()
            if not check_date_is_today(date, today): continue
            time = await cells.nth(1).inner_text()
            bundle = await cells.nth(2).inner_text()
            parsed = parse_bundle(bundle)
            if not parsed: continue
            away_team, home_team = parsed
            stadium = await cells.nth(7).inner_text()
            timestamp = parse_game_datetime(date + time)
            await insert_data(async_session, (timestamp, away_team, home_team, stadium))
        elif count == 8:
            if not check_date_is_today(date, today): continue
            time = await cells.nth(0).inner_text()
            bundle = await cells.nth(1).inner_text()
            parsed = parse_bundle(bundle)
            if not parsed: continue
            away_team, home_team = parsed
            stadium = await cells.nth(6).inner_text()
            timestamp = parse_game_datetime(date + time)
            await insert_data(async_session, (timestamp, away_team, home_team, stadium))

    await engine.dispose()

# ---------------------- Airflow 실행 함수 ----------------------

def _scrape_today_games_time(**kwargs):
    execution_time = kwargs['execution_date']
    today = execution_time.add(hours=9).to_date_string()
    print(f"기준 날짜: {today}")
    
    async def _run():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await crawling(page, today=today)
            await browser.close()

    asyncio.run(_run())
    return "스크래핑 완료"

# ---------------------- DAG 정의 ----------------------

with DAG(
    dag_id='030_scrape_today_games_time',
    default_args=default_args,
    description='오늘 날짜의 야구 경기 시간을 DB에 저장하는 DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    scrape_today_games_time = PythonOperator(
        task_id='scrape_today_games_time',
        python_callable=_scrape_today_games_time,
    )
    # Trigger DAG 정의
    trigger_schedule_dag_for_get_today_line_up_dag = TriggerDagRunOperator(
        task_id='trigger_schedule_dag_for_get_today_line_up_dag',
        trigger_dag_id='031_schedule_dag_for_get_today_line_up',
        wait_for_completion=False,
    )

    scrape_today_games_time >> trigger_schedule_dag_for_get_today_line_up_dag
