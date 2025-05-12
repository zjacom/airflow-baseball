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
        await session.execute(text(f"""CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            game_date TIMESTAMP,
            player VARCHAR(50),
            team VARCHAR(50),
            position INT,
            opponent VARCHAR(50),
            stadium VARCHAR(50)
            );"""
        ))
        await session.commit()

async def insert_data(async_session, data_tuple):
    columns = ["game_date", "player", "team", "position", "opponent", "stadium"]
    data = dict(zip(columns, data_tuple))
    query = text("""
        INSERT INTO today_lineup (
            game_date, player, team, position, opponent, stadium
        ) VALUES (
            :game_date, :player, :team, :position, :opponent, :stadium
        )"""
    )
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
    await initialize_table("today_lineup", async_session)

    url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
    await goto_with_retry(page, url)

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
            print(f"{i}번째 요소를 찾거나 클릭하는 중 오류 발생: {e}")
        
        try:
            stadium_element = await page.wait_for_selector(f'//*[@id="contents"]/div[3]/div/div[1]/ul/li[{i}]/div[1]/ul/li[1]')
            game_time_element = await page.wait_for_selector(f'//*[@id="contents"]/div[3]/div/div[1]/ul/li[{i}]/div[1]/ul/li[3]')
            home_pitcher_element = await page.wait_for_selector(f'//*[@id="contents"]/div[3]/div/div[1]/ul/li[{i}]/div[2]/div[2]/div[1]/div[2]/p')
            away_pitcher_element = await page.wait_for_selector(f'//*[@id="contents"]/div[3]/div/div[1]/ul/li[{i}]/div[2]/div[2]/div[3]/div[2]/p')
        except:
            continue

        line_up_button = await page.wait_for_selector('//*[@id="tabPreview"]/li[3]/a')
        await line_up_button.click()

        is_registered_element = await page.wait_for_selector('//*[@id="txtLineUp"]')
        is_registered = await is_registered_element.text_content()

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
        for i in range(1, 10):
            away_hitter_element = await page.wait_for_selector(f'//*[@id="tblAwayLineUp"]/tbody/tr[{i}]')
            away_hitter_td_elements = await away_hitter_element.query_selector_all("td")
            away_hitter = []
            for idx, td in enumerate(away_hitter_td_elements):
                if idx % 2 == 1:
                    continue
                text = await td.inner_text()
                away_hitter.append(text)
            away_hitters.append(tuple(away_hitter))
            
            home_hitter_element = await page.wait_for_selector(f'//*[@id="tblHomeLineUp"]/tbody/tr[{i}]')
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
        
        # 원정 타자 라인업 저장
        for order, name in away_hitters:
            data = (dt, name, away_team, order, home_team, stadium)
            await insert_data(async_session=async_session, data_tuple=data)
        
        # 홈 타자 라인업 저장
        for order, name in home_hitters:
            data = (dt, name, home_team, order, away_team, stadium)
            await insert_data(async_session=async_session, data_tuple=data)

        print(home_pitcher, home_team)
        print(away_pitcher, away_team)
        # 선발 투수 저장
        await insert_data(async_session=async_session, data_tuple=(dt, away_pitcher, home_team, 0, away_team, stadium))
        await insert_data(async_session=async_session, data_tuple=(dt, home_pitcher, away_team, 0, home_team, stadium))

    await engine.dispose()

# ---------------------- Airflow 실행 함수 ----------------------

def _scrape_today_lineup():
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
    dag_id='040_scrape_today_lineup',
    default_args=default_args,
    description='오늘 날짜 라인업을 DB에 저장하는 DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    scrape_today_lineup = PythonOperator(
        task_id='scrape_today_lineup',
        python_callable=_scrape_today_lineup,
    )

    # Trigger DAG 정의
    trigger_calculate_hitter_metrics_with_park_factor_dag = TriggerDagRunOperator(
        task_id='trigger_calculate_hitter_metrics_with_park_factor_dag',
        trigger_dag_id='041_calculate_hitter_metrics_with_park_factor',
        wait_for_completion=False,
    )

    scrape_today_lineup >> trigger_calculate_hitter_metrics_with_park_factor_dag