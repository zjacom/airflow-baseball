from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import asyncio
from datetime import datetime
import re

from utils.create_db_connection import get_async_db_connection
from utils.queries import get_query
from utils.table_columns import get_table_columns

from playwright.async_api import async_playwright, Page

# 기본 DAG 인수
default_args = {
    'owner': 'niscom',
    'depends_on_past': False,
}

# ---------------------- 유틸 함수 정의 ----------------------

def parse_bundle(game_result: str):
    match = re.match(r"(\D+)(\d+)vs(\d+)(\D+)", game_result)
    if match:
        away_team, away_score, home_score, home_team = match.groups()
        return (away_team.strip(), int(away_score), home_team.strip(), int(home_score))
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

async def upsert_data(table_name, data_tuple, async_session):
    columns = get_table_columns(table_name=table_name)
    data = dict(zip(columns, data_tuple))

    query = get_query(table_name=table_name)
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

async def crawling(page: Page):
    engine, async_session = await get_async_db_connection()
    # 스케줄 데이터 처리
    url = f"https://www.koreabaseball.com/Schedule/Schedule.aspx"
    await goto_with_retry(page, url)
    
    # 필요한 경우 월 선택 (주석 처리된 부분)
    # await page.select_option('//*[@id="ddlMonth"]', "03")
    # await asyncio.sleep(5)
    
    game_schedule = page.locator('//*[@id="tblScheduleList"]/tbody')
    game_schedule_elements = game_schedule.locator('tr')
    
    date = ""  # 날짜 초기화
    
    for i in range(await game_schedule_elements.count()):
        tr_row = game_schedule_elements.nth(i)
        td_elements = tr_row.locator('td')
        td_count = await td_elements.count()

        if td_count == 9:  # 새로운 날짜가 포함된 행
            date = await td_elements.nth(0).inner_text()
            time = await td_elements.nth(1).inner_text()
            bundle = await td_elements.nth(2).inner_text()
            parsed_bundle = parse_bundle(bundle)
            if parsed_bundle is None:
                continue
            else:
                away_team, away_score, home_team, home_score = parsed_bundle
            stadium = await td_elements.nth(7).inner_text()
            timestamp = parse_game_datetime(date + time)

            data = (timestamp, away_team, away_score, home_team, home_score, stadium)
            await upsert_data(table_name="game_records", data_tuple=data, async_session=async_session)
        elif td_count == 8:  # 같은 날짜의 다른 경기
            time = await td_elements.nth(0).inner_text()
            bundle = await td_elements.nth(1).inner_text()
            parsed_bundle = parse_bundle(bundle)
            if parsed_bundle is None:
                continue
            else:
                away_team, away_score, home_team, home_score = parsed_bundle
            stadium = await td_elements.nth(6).inner_text()
            timestamp = parse_game_datetime(date + time)
            data = (timestamp, away_team, away_score, home_team, home_score, stadium)
            await upsert_data(table_name="game_records", data_tuple=data, async_session=async_session)

    await engine.dispose()

# ---------------------- Airflow 실행 함수 ----------------------

def _scrape_games_result():
    async def _run():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await crawling(page)
            await browser.close()

    asyncio.run(_run())
    return "스크래핑 완료"

# ---------------------- DAG 정의 ----------------------

with DAG(
    dag_id='000_scrape_games_result',
    default_args=default_args,
    description='야구 경기 결과를 DB에 저장하는 DAG',
    schedule_interval='0 20 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    scrape_games_result = PythonOperator(
        task_id='scrape_games_result',
        python_callable=_scrape_games_result,
    )

    trigger_calculate_park_factor_dag = TriggerDagRunOperator(
        task_id='trigger_calculate_park_factor_dag',
        trigger_dag_id='001_park_factor_calculation',
        wait_for_completion=False
    )

    scrape_games_result >> trigger_calculate_park_factor_dag