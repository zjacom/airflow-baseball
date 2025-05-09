from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import asyncio
from typing import Tuple

from utils.create_db_connection import get_async_db_connection
from utils.queries import get_query
from utils.table_columns import get_table_columns

import asyncio
from playwright.async_api import async_playwright, Page

# Playwright 관련 임포트를 PythonOperator 내부에서 실행하기 위해 여기서는 임포트하지 않음

engine, async_session = None, None

# 기본 DAG 인수
default_args = {
    'owner': 'niscom',
    'depends_on_past': False,
}

# ---------------------- 유틸 함수 정의 ----------------------

def str_to_float(str):
    if str == "-":
        return None
    return float(str)

# ---------------------- 비동기 DB 함수 정의 ----------------------

async def upsert_data(table_name: str, data_tuple: Tuple, async_session):
    columns = get_table_columns(table_name=table_name)
    data = dict(zip(columns, data_tuple))
    query = get_query(table_name=table_name)

    async with async_session() as session:
        await session.execute(query, data)
        await session.commit()

# ---------------------- 비동기 함수 정의 ----------------------

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

async def check_player(page, player_id: int) -> bool:
    url = f"https://www.koreabaseball.com/Record/Player/HitterDetail/Basic.aspx?playerId={player_id}"
    await goto_with_retry(page, url)
    
    try:
        image_element = await page.locator('//*[@id="cphContents_cphContents_cphContents_playerProfile_imgProgile"]').get_attribute('src')
        if "no-Image" in image_element:
            return False
        else:
            return True
    except Exception as e:
        print(f"플레이어 {player_id} 확인 중 오류: {e}")
        return False
        
# ---------------------- 크롤링 로직 ----------------------

async def crawling(page, player_id: int, async_session):
    try:
        is_record = await page.locator('//*[@id="contents"]/div[2]/div[2]/div[2]/table/tbody/tr/td').count()
        if is_record == 1:
            return

        player_name = await page.locator('//*[@id="cphContents_cphContents_cphContents_playerProfile_lblName"]').text_content()
        player_name = player_name.strip()

        # 기본 정보 추출
        first_row = page.locator('//*[@id="contents"]/div[2]/div[2]/div[2]/table/tbody/tr').first
        first_td_elements = first_row.locator('td')
        first_td_values = []
        for i in range(await first_td_elements.count()):
            text = await first_td_elements.nth(i).text_content()
            first_td_values.append((text or "").strip())
        
        team_name, avg, games, pa, ab, runs, hits, doubles, triples, hr, _, rbi, sb, cs, sac, sf = first_td_values
        team_name, avg, games, pa, ab, runs, hits, doubles, triples, hr, rbi, sb, cs, sac, sf = (
            team_name.strip(), str_to_float(avg), int(games), int(pa), int(ab), int(runs), int(hits), 
            int(doubles), int(triples), int(hr), int(rbi), int(sb), int(cs), int(sac), int(sf)
        )

        # 추가 정보 추출
        second_row = page.locator('//*[@id="contents"]/div[2]/div[2]/div[3]/table/tbody/tr').first
        second_td_elements = second_row.locator('td')
        second_td_values = []
        for i in range(await second_td_elements.count()):
            text = await second_td_elements.nth(i).text_content()
            second_td_values.append((text or "").strip())
        
        bb, ibb, hbp, so, gdp, slg, obp, errors, sb_percentage, mh, ops, risp, ph_ba = second_td_values
        bb, ibb, hbp, so, gdp, slg, obp, errors, sb_percentage, mh, ops, risp, ph_ba = (
            int(bb), int(ibb), int(hbp), int(so), int(gdp), str_to_float(slg), str_to_float(obp), 
            int(errors), float(sb_percentage) / 100 if sb_percentage != "-" else None, 
            int(mh), str_to_float(ops), str_to_float(risp), str_to_float(ph_ba)
        )

        # hitters 테이블에 데이터 삽입
        data = (player_id, player_name, team_name, avg, games, pa, ab, runs, hits, doubles, triples, hr, 
                rbi, sb, cs, sac, sf, bb, ibb, hbp, so, gdp, slg, obp, errors, sb_percentage, mh, ops, risp, ph_ba)
        await upsert_data(table_name="hitters", data_tuple=data, async_session=async_session)

        # 최근 10경기 데이터 처리
        recent_10_games = page.locator('//*[@id="contents"]/div[2]/div[2]/div[4]/table/tbody').first
        recent_tr_elements = recent_10_games.locator('tr')
        for i in range(await recent_tr_elements.count()):
            row = recent_tr_elements.nth(i)
            td_elements = row.locator('td')
            td_count = await td_elements.count()

            td_values = []
            for j in range(td_count):
                text = await td_elements.nth(j).text_content()
                td_values.append((text or "").strip())
            
            r_date, r_opponent, r_avg, r_pa, r_ab, r_runs, r_hits, r_doubles, r_triples, r_hr, r_rbi, r_sb, r_cs, r_bb, r_hbp, r_so, r_gdp = td_values
            
            # r_date 문자열 변환
            current_year = datetime.now().year
            r_date = f"{current_year}-{r_date.replace('.', '-')}"
            r_opponent = r_opponent.strip()
            r_avg, r_pa, r_ab, r_runs, r_hits, r_doubles, r_triples, r_hr, r_rbi, r_sb, r_cs, r_bb, r_hbp, r_so, r_gdp = (
                str_to_float(r_avg), int(r_pa), int(r_ab), int(r_runs), int(r_hits), int(r_doubles), 
                int(r_triples), int(r_hr), int(r_rbi), int(r_sb), int(r_cs), int(r_bb), 
                int(r_hbp), int(r_so), int(r_gdp)
            )

            # 일자별 기록 테이블에 데이터 삽입
            data = (player_id, r_date, r_opponent, r_avg, r_pa, r_ab, r_runs, r_hits, r_doubles, 
                    r_triples, r_hr, r_rbi, r_sb, r_cs, r_bb, r_hbp, r_so, r_gdp)
            await upsert_data("hitter_games", data_tuple=data, async_session=async_session)

        # 상대별 기록 처리
        url = f"https://www.koreabaseball.com/Record/Player/HitterDetail/Game.aspx?playerId={player_id}"
        await goto_with_retry(page, url)

        case_by_opponent = page.locator('//*[@id="contents"]/div[2]/div[2]/div[1]/table/tbody')
        cbo_tr_elements = case_by_opponent.locator('tr')
        for i in range(await cbo_tr_elements.count()):
            row = cbo_tr_elements.nth(i)
            td_elements = row.locator('td')
            td_count = await td_elements.count()

            td_values = []
            for j in range(td_count):
                text = await td_elements.nth(j).text_content()
                td_values.append((text or "").strip())
            
            o_opponent, o_games, o_avg, o_pa, o_ab, o_runs, o_hits, o_doubles, o_triples, o_hr, o_rbi, o_sb, o_cs, o_bb, o_hbp, o_so, o_gdp = td_values
            o_opponent = o_opponent.strip()
            o_games = int(o_games)
            o_avg = str_to_float(o_avg)
            o_pa, o_ab, o_runs, o_hits, o_doubles, o_triples, o_hr, o_rbi, o_sb, o_cs, o_bb, o_hbp, o_so, o_gdp = (
                int(o_pa), int(o_ab), int(o_runs), int(o_hits), int(o_doubles), int(o_triples), 
                int(o_hr), int(o_rbi), int(o_sb), int(o_cs), int(o_bb), int(o_hbp), int(o_so), int(o_gdp)
            )

            # 상대별 테이블
            data = (player_id, o_opponent, o_games, o_avg, o_pa, o_ab, o_runs, o_hits, o_doubles, 
                    o_triples, o_hr, o_rbi, o_sb, o_cs, o_bb, o_hbp, o_so, o_gdp)
            await upsert_data("hitter_opponents", data_tuple=data, async_session=async_session)

        # 구장별 기록 처리
        case_by_stadium = page.locator('//*[@id="contents"]/div[2]/div[2]/div[2]/table/tbody')
        cbs_tr_elements = case_by_stadium.locator('tr')
        for i in range(await cbs_tr_elements.count()):
            row = cbs_tr_elements.nth(i)
            td_elements = row.locator('td')
            td_count = await td_elements.count()

            td_values = []
            for j in range(td_count):
                text = await td_elements.nth(j).text_content()
                td_values.append((text or "").strip())
            
            s_stadium, s_games, s_avg, s_pa, s_ab, s_runs, s_hits, s_doubles, s_triples, s_hr, s_rbi, s_sb, s_cs, s_bb, s_hbp, s_so, s_gdp = td_values
            s_stadium = s_stadium.strip()
            s_games = int(s_games)
            s_avg = str_to_float(s_avg)
            s_pa, s_ab, s_runs, s_hits, s_doubles, s_triples, s_hr, s_rbi, s_sb, s_cs, s_bb, s_hbp, s_so, s_gdp = (
                int(s_pa), int(s_ab), int(s_runs), int(s_hits), int(s_doubles), int(s_triples), 
                int(s_hr), int(s_rbi), int(s_sb), int(s_cs), int(s_bb), int(s_hbp), int(s_so), int(s_gdp)
            )

            # 구장별 테이블
            data = (player_id, s_stadium, s_games, s_avg, s_pa, s_ab, s_runs, s_hits, s_doubles, 
                    s_triples, s_hr, s_rbi, s_sb, s_cs, s_bb, s_hbp, s_so, s_gdp)
            await upsert_data("hitter_stadiums", data_tuple=data, async_session=async_session)

    except Exception as e:
        print(f"플레이어 {player_id} 데이터 처리 중 오류: {e}")
        raise

# ---------------------- Airflow 실행 함수 ----------------------

def _scrape_hitters_stats(start_id, end_id):
    async def _run(start_id: int, end_id: int):
        engine, async_session = await get_async_db_connection()

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()

                for player_id in range(start_id, end_id + 1):
                    if await check_player(page, player_id):
                        await crawling(page, player_id, async_session)

                await browser.close()
        finally:
            await engine.dispose()

    # asyncio 이벤트 루프 실행
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(_run(start_id, end_id))
    finally:
        loop.close()


# ---------------------- DAG 정의 ----------------------

with DAG(
    dag_id='010_scrape_hitters_stats',
    default_args=default_args,
    description='타자 스탯 데이터를 크롤링하는 DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # TaskGroup으로 크롤링 태스크 묶기
    with TaskGroup("scrape_hitters_group", tooltip="Scrape hitter stats by player ranges") as scrape_tasks:
        player_ranges = [
            (50007, 60000),
            (60001, 70000),
            (70001, 80000),
            (80001, 90000),
            (90001, 99811),
        ]

        for start_id, end_id in player_ranges:
            PythonOperator(
                task_id=f'scrape_players_{start_id}_to_{end_id}',
                python_callable=_scrape_hitters_stats,
                op_kwargs={'start_id': start_id, 'end_id': end_id},
            )

    # Trigger DAG 정의
    trigger_calculate_hitter_wOBA_dag = TriggerDagRunOperator(
        task_id='trigger_calculate_hitter_wOBA_dag',
        trigger_dag_id='011_calculate_hitter_wOBA',
        wait_for_completion=False,
    )

    # TaskGroup 완료 후 Trigger 실행
    scrape_tasks >> trigger_calculate_hitter_wOBA_dag