from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import asyncio
from datetime import datetime
from sqlalchemy import text

from utils.create_db_connection import get_async_db_connection
from playwright.async_api import async_playwright, Page

# 기본 DAG 인수
default_args = {
    'owner': 'niscom',
    'depends_on_past': False,
}

# ---------------------- 유틸 함수 정의 ----------------------

# ---------------------- 비동기 DB 함수 정의 ----------------------

async def insert_data(async_session, data_tuple):
    columns = ["game_date", "away_team", "home_team", "result"]
    data = dict(zip(columns, data_tuple))
    query = text("""
        INSERT INTO game_records_until_5_innings (
            game_date, away_team, home_team, result
        ) VALUES (
            :game_date, :away_team, :home_team, :result
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

        away_team_element = await page.wait_for_selector('//*[@id="lblAwayHitter"]/span/img')
        home_team_element = await page.wait_for_selector('//*[@id="lblHomeHitter"]/span/img')

        away_team_full_name = await away_team_element.get_attribute("alt")
        home_team_full_name = await home_team_element.get_attribute("alt")

        away_team = away_team_full_name.split(" ")[0]
        home_team = home_team_full_name.split(" ")[0]

        datetime_str = f"{date} {game_time}"
        # 문자열을 datetime 객체로 파싱
        dt = datetime.strptime(datetime_str, "%Y.%m.%d %H:%M")

        # game_records 테이블에 dt, away_team, away_score_sum, home_team, home_score_sum 삽입하는 코드
        if away_score_sum > home_score_sum:
            result = -1
        elif away_score_sum == home_score_sum:
            result = 0
        else:
            result = 1
            
        game_data = (dt, away_team, home_team, result)
        await insert_data(async_session=async_session, data_tuple=game_data)
    await engine.dispose()

# ---------------------- Airflow 실행 함수 ----------------------

def _scrape_game_records_until_5_innings():
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
    dag_id='070_scrape_game_result_until_5_innings',
    default_args=default_args,
    description='5이닝까지의 결과를 DB에 저장하는 DAG',
    schedule_interval='50 14 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    scrape_game_records_until_5_innings = PythonOperator(
        task_id='scrape_game_records_until_5_innings',
        python_callable=_scrape_game_records_until_5_innings,
    )

    scrape_game_records_until_5_innings