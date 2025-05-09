from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import List, Tuple, Optional
import mysql.connector
from mysql.connector import pooling

from utils.create_db_connection import get_async_db_connection
from utils.queries import get_query
from utils.table_columns import get_table_columns

from playwright.async_api import async_playwright, Page

# Playwright 관련 임포트를 PythonOperator 내부에서 실행하기 위해 여기서는 임포트하지 않음

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
    url = f"https://www.koreabaseball.com/Record/Player/PitcherDetail/Basic.aspx?playerId={player_id}"
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
        
        team_name, era, games, cg, sho, wins, loses, sv, hld, wpct, tbf, np, ip, hits, doubles, triples, hr = first_td_values
        team_name, era, games, cg, sho, wins, loses, sv, hld, wpct, tbf, np, hits, doubles, triples, hr = (
            team_name.strip(), str_to_float(era), int(games), int(cg), int(sho), int(wins), int(loses), 
            int(sv), int(hld), str_to_float(wpct), int(tbf), int(np), int(hits), int(doubles), int(triples), int(hr)
        )

        # 추가 정보 추출
        second_row = page.locator('//*[@id="contents"]/div[2]/div[2]/div[3]/table/tbody/tr').first
        second_td_elements = second_row.locator('td')
        second_td_values = []
        for i in range(await second_td_elements.count()):
            text = await second_td_elements.nth(i).text_content()
            second_td_values.append((text or "").strip())
        
        sac, sf, bb, ibb, so, wp, bk, runs, er, bsv, whip, avg, qs = second_td_values
        sac, sf, bb, ibb, so, wp, bk, runs, er, bsv, whip, avg, qs = (
            int(sac), int(sf), int(bb), int(ibb), int(so), int(wp), int(bk), 
            int(runs), int(er), int(bsv),
            str_to_float(whip), str_to_float(avg), int(qs)
        )

        # pitchers 테이블에 데이터 삽입
        data = (player_id, player_name, team_name, era, games, cg, sho, wins, loses, sv, hld, wpct, tbf, np, ip, hits, doubles, triples, hr, 
                sac, sf, bb, ibb, so, wp, bk, runs, er, bsv, whip, avg, qs)
        await upsert_data("pitchers", data_tuple=data, async_session=async_session)

        # 최근 10경기 데이터 처리
        recent_10_games = page.locator('//*[@id="contents"]/div[2]/div[2]/div[4]/table/tbody').first
        recent_tr_elements = recent_10_games.locator('tr')
        for i in range(await recent_tr_elements.count()):
            recent_row = recent_tr_elements.nth(i)
            recent_td_elements = recent_row.locator('td')
            recent_td_count = await recent_td_elements.count()

            recent_td_values = []
            for j in range(recent_td_count):
                recent_text = await recent_td_elements.nth(j).text_content()
                recent_td_values.append((recent_text or "").strip())
            
            r_date, r_opponent, r_result, r_era, r_tbf, r_ip, r_hits, r_hr, r_bb, r_hbp, r_so, r_runs, r_er, r_avg = recent_td_values
            
            # r_date 문자열 변환
            current_year = datetime.now().year
            r_date = f"{current_year}-{r_date.replace('.', '-')}"
            r_opponent = r_opponent.strip()
            r_result = r_result.strip()
            if not r_result:
                r_result = None
            r_era, r_tbf, r_hits, r_hr, r_bb, r_hbp, r_so, r_runs, r_er, r_avg = (
                str_to_float(r_era), int(r_tbf), int(r_hits), int(r_hr),
                int(r_bb), int(r_hbp), int(r_so), int(r_runs), int(r_er), str_to_float(r_avg)  
            )

            # 일자별 기록 테이블에 데이터 삽입
            data = (player_id, r_date, r_opponent, r_result, r_era, r_tbf, r_ip,
                    r_hits, r_hr, r_bb, r_hbp, r_so, r_runs, r_er, r_avg)
            await upsert_data("pitcher_games", data_tuple=data, async_session=async_session)

        # 상대별 기록 처리
        url = f"https://www.koreabaseball.com/Record/Player/PitcherDetail/Game.aspx?playerId={player_id}"
        await goto_with_retry(page, url)

        case_by_opponent = page.locator('//*[@id="contents"]/div[2]/div[2]/div[1]/table/tbody')
        cbo_tr_elements = case_by_opponent.locator('tr')
        for i in range(await cbo_tr_elements.count()):
            cbo_row = cbo_tr_elements.nth(i)
            cbo_td_elements = cbo_row.locator('td')
            cbo_td_count = await cbo_td_elements.count()

            cbo_td_values = []
            for j in range(cbo_td_count):
                cbo_text = await cbo_td_elements.nth(j).text_content()
                cbo_td_values.append((cbo_text or "").strip())
            
            o_opponent, o_games, o_era, o_wins, o_loses, o_sv, o_hld, o_wpct, o_tbf, o_ip, o_hits, o_hr, o_bb, o_hbp, o_so, o_runs, o_er, o_avg = cbo_td_values
            o_opponent, o_games, o_era, o_wins, o_loses, o_sv, o_hld, o_wpct, o_tbf, o_hits, o_hr, o_bb, o_hbp, o_so, o_runs, o_er, o_avg = (
                o_opponent.strip(), int(o_games), str_to_float(o_era), int(o_wins), int(o_loses), int(o_sv), int(o_hld),
                str_to_float(o_wpct), int(o_tbf), int(o_hits), int(o_hr), int(o_bb), int(o_hbp), int(o_so), int(o_runs), int(o_er),
                str_to_float(o_avg)
            )

            # 상대별 테이블
            data = (player_id, o_opponent, o_games, o_era, o_wins, o_loses, o_sv, o_hld, o_wpct,
                    o_tbf, o_ip, o_hits, o_hr, o_bb, o_hbp, o_so, o_runs, o_er, o_avg)
            await upsert_data("pitcher_opponents", data_tuple=data, async_session=async_session)

        # 구장별 기록 처리
        case_by_stadium = page.locator('//*[@id="contents"]/div[2]/div[2]/div[2]/table/tbody')
        cbs_tr_elements = case_by_stadium.locator('tr')
        for i in range(await cbs_tr_elements.count()):
            cbs_row = cbs_tr_elements.nth(i)
            cbs_td_elements = cbs_row.locator('td')
            cbs_td_count = await cbs_td_elements.count()

            cbs_td_values = []
            for j in range(cbs_td_count):
                cbs_text = await cbs_td_elements.nth(j).text_content()
                cbs_td_values.append((cbs_text or "").strip())
            
            s_stadium, s_games, s_era, s_wins, s_loses, s_sv, s_hld, s_wpct, s_tbf, s_ip, s_hits, s_hr, s_bb, s_hbp, s_so, s_runs, s_er, s_avg = cbs_td_values
            s_stadium, s_games, s_era, s_wins, s_loses, s_sv, s_hld, s_wpct, s_tbf, s_hits, s_hr, s_bb, s_hbp, s_so, s_runs, s_er, s_avg = (
                s_stadium.strip(), int(s_games), str_to_float(s_era), int(s_wins), int(s_loses), int(s_sv), int(s_hld),
                str_to_float(s_wpct), int(s_tbf), int(s_hits), int(s_hr), int(s_bb), int(s_hbp), int(s_so), int(s_runs), int(s_er),
                str_to_float(s_avg)
            )

            # 구장별 테이블
            data = (player_id, s_stadium, s_games, s_era, s_wins, s_loses, s_sv, s_hld, s_wpct, s_tbf,
                    s_ip, s_hits, s_hr, s_bb, s_hbp, s_so, s_runs, s_er, s_avg)
            await upsert_data("pitcher_stadiums", data_tuple=data, async_session=async_session)
            
    except Exception as e:
        print(f"플레이어 {player_id} 데이터 처리 중 오류: {e}")
        raise

# ---------------------- Airflow 실행 함수 ----------------------

def _scrape_pitchers_stats(start_id, end_id):
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
    dag_id='020_scrape_pitchers_stats',
    default_args=default_args,
    description='투수 스탯 데이터를 크롤링하는 DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # TaskGroup으로 크롤링 태스크 묶기
    with TaskGroup("scrape_pitchers_group", tooltip="Scrape pitcher stats by player ranges") as scrape_tasks:
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
                python_callable=_scrape_pitchers_stats,
                op_kwargs={'start_id': start_id, 'end_id': end_id},
            )

    # Trigger DAG 정의
    trigger_calculate_pitcher_metrics_dag = TriggerDagRunOperator(
        task_id='trigger_calculate_pitcher_metrics_dag',
        trigger_dag_id='021_calculate_pitcher_metrics',
        wait_for_completion=False,
    )

    # TaskGroup 완료 후 Trigger 실행
    scrape_tasks >> trigger_calculate_pitcher_metrics_dag