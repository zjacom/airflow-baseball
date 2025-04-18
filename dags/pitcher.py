from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import List, Tuple, Optional
import mysql.connector
from mysql.connector import pooling

# Playwright 관련 임포트를 PythonOperator 내부에서 실행하기 위해 여기서는 임포트하지 않음

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

    async def execute_upsert(self, query: str, data: Tuple):
        async with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(query, data)
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

    def str_to_float(self, str):
        if str == "-":
            return None
        return float(str)

    def _get_upsert_query(self, table_name: str) -> str:
        """각 테이블별 upsert 쿼리를 반환하는 메서드"""
        queries = {
            "pitchers": """
            INSERT INTO pitchers (
                pitcher_id, player_name, team_name, era, games, cg, sho, wins, losses, sv, hld,
                wpct, tbf, np, ip, hits, doubles, triples, hr, sac, sf, bb, ibb, so, wp, bk,
                runs, er, bsv, whip, avg, qs
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                player_name = VALUES(player_name),
                team_name = VALUES(team_name),
                era = VALUES(era),
                games = VALUES(games),
                cg = VALUES(cg),
                sho = VALUES(sho),
                wins = VALUES(wins),
                losses = VALUES(losses),
                sv = VALUES(sv),
                hld = VALUES(hld),
                wpct = VALUES(wpct),
                tbf = VALUES(tbf),
                np = VALUES(np),
                ip = VALUES(ip),
                hits = VALUES(hits),
                doubles = VALUES(doubles),
                triples = VALUES(triples),
                hr = VALUES(hr),
                sac = VALUES(sac),
                sf = VALUES(sf),
                bb = VALUES(bb),
                ibb = VALUES(ibb),
                so = VALUES(so),
                wp = VALUES(wp),
                bk = VALUES(bk),
                runs = VALUES(runs),
                er = VALUES(er),
                bsv = VALUES(bsv),
                whip = VALUES(whip),
                avg = VALUES(avg),
                qs = VALUES(qs)
            """,
            "pitcher_games": """
            INSERT INTO pitcher_games (
                pitcher_id, game_date, opponent_team, result, era, tbf, ip, hits, hr,
                bb, hbp, so, runs, er, avg
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE 
                opponent_team = VALUES(opponent_team),
                result = VALUES(result),
                era = VALUES(era),
                tbf = VALUES(tbf),
                ip = VALUES(ip),
                hits = VALUES(hits),
                hr = VALUES(hr),
                bb = VALUES(bb),
                hbp = VALUES(hbp),
                so = VALUES(so),
                runs = VALUES(runs),
                er = VALUES(er),
                avg = VALUES(avg)
            """,
            "pitcher_opponents": """
            INSERT INTO pitcher_opponents (
                pitcher_id, opponent_team, games, era, wins, losses, sv, hld, wpct, tbf,
                ip, hits, hr, bb, hbp, so, runs, er, avg
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE 
                games = VALUES(games),
                era = VALUES(era),
                wins = VALUES(wins),
                losses = VALUES(losses),
                sv = VALUES(sv),
                hld = VALUES(hld),
                wpct = VALUES(wpct),
                tbf = VALUES(tbf),
                ip = VALUES(ip),
                hits = VALUES(hits),
                hr = VALUES(hr),
                bb = VALUES(bb),
                hbp = VALUES(hbp),
                so = VALUES(so),
                runs = VALUES(runs),
                er = VALUES(er),
                avg = VALUES(avg)
            """,
            "pitcher_stadiums": """
            INSERT INTO pitcher_stadiums (
                pitcher_id, stadium, games, era, wins, losses, sv, hld, wpct, tbf, ip, 
                hits, hr, bb, hbp, so, runs, er, avg, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
            )
            ON DUPLICATE KEY UPDATE 
                games = VALUES(games),
                era = VALUES(era),
                wins = VALUES(wins),
                losses = VALUES(losses),
                sv = VALUES(sv),
                hld = VALUES(hld),
                wpct = VALUES(wpct),
                tbf = VALUES(tbf),
                ip = VALUES(ip),
                hits = VALUES(hits),
                hr = VALUES(hr),
                bb = VALUES(bb),
                hbp = VALUES(hbp),
                so = VALUES(so),
                runs = VALUES(runs),
                er = VALUES(er),
                avg = VALUES(avg),
                created_at = NOW();
            """
        }
        return queries.get(table_name, "")

    async def upsert_data(self, table_name: str, data: Tuple):
        """데이터 삽입/업데이트 메서드"""
        query = self._get_upsert_query(table_name)
        await self.db_manager.execute_upsert(query, data)

    async def goto_with_retry(self, page, url: str, max_retries: int = 3):
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

    async def check_player(self, page, player_id: int) -> bool:
        url = f"https://www.koreabaseball.com/Record/Player/PitcherDetail/Basic.aspx?playerId={player_id}"
        await self.goto_with_retry(page, url)
        
        try:
            image_element = await page.locator('//*[@id="cphContents_cphContents_cphContents_playerProfile_imgProgile"]').get_attribute('src')
            if "no-Image" in image_element:
                return False
            else:
                return True
        except Exception as e:
            logger.warning(f"플레이어 {player_id} 확인 중 오류: {e}")
            return False

    async def process_player_data(self, page, player_id: int):
        try:
            is_record = await page.locator('//*[@id="contents"]/div[2]/div[2]/div[2]/table/tbody/tr/td').count()
            if is_record == 1:
                return

            self.count += 1
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
                team_name.strip(), self.str_to_float(era), int(games), int(cg), int(sho), int(wins), int(loses), 
                int(sv), int(hld), self.str_to_float(wpct), int(tbf), int(np), int(hits), int(doubles), int(triples), int(hr)
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
                self.str_to_float(whip), self.str_to_float(avg), int(qs)
            )

            # pitchers 테이블에 데이터 삽입
            data = (player_id, player_name, team_name, era, games, cg, sho, wins, loses, sv, hld, wpct, tbf, np, ip, hits, doubles, triples, hr, 
                    sac, sf, bb, ibb, so, wp, bk, runs, er, bsv, whip, avg, qs)
            await self.upsert_data("pitchers", data)

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
                    self.str_to_float(r_era), int(r_tbf), int(r_hits), int(r_hr),
                    int(r_bb), int(r_hbp), int(r_so), int(r_runs), int(r_er), self.str_to_float(r_avg)  
                )

                # 일자별 기록 테이블에 데이터 삽입
                data = (player_id, r_date, r_opponent, r_result, r_era, r_tbf, r_ip,
                        r_hits, r_hr, r_bb, r_hbp, r_so, r_runs, r_er, r_avg)
                await self.upsert_data("pitcher_games", data)

            # 상대별 기록 처리
            url = f"https://www.koreabaseball.com/Record/Player/PitcherDetail/Game.aspx?playerId={player_id}"
            await self.goto_with_retry(page, url)

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
                    o_opponent.strip(), int(o_games), self.str_to_float(o_era), int(o_wins), int(o_loses), int(o_sv), int(o_hld),
                    self.str_to_float(o_wpct), int(o_tbf), int(o_hits), int(o_hr), int(o_bb), int(o_hbp), int(o_so), int(o_runs), int(o_er),
                    self.str_to_float(o_avg)
                )

                # 상대별 테이블
                data = (player_id, o_opponent, o_games, o_era, o_wins, o_loses, o_sv, o_hld, o_wpct,
                        o_tbf, o_ip, o_hits, o_hr, o_bb, o_hbp, o_so, o_runs, o_er, o_avg)
                await self.upsert_data("pitcher_opponents", data)

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
                    s_stadium.strip(), int(s_games), self.str_to_float(s_era), int(s_wins), int(s_loses), int(s_sv), int(s_hld),
                    self.str_to_float(s_wpct), int(s_tbf), int(s_hits), int(s_hr), int(s_bb), int(s_hbp), int(s_so), int(s_runs), int(s_er),
                    self.str_to_float(s_avg)
                )

                # 구장별 테이블
                data = (player_id, s_stadium, s_games, s_era, s_wins, s_loses, s_sv, s_hld, s_wpct, s_tbf,
                        s_ip, s_hits, s_hr, s_bb, s_hbp, s_so, s_runs, s_er, s_avg)
                await self.upsert_data("pitcher_stadiums", data)
                
        except Exception as e:
            logger.error(f"플레이어 {player_id} 데이터 처리 중 오류: {e}")

    async def run(self, start_id: int, end_id: int):
        # Playwright를 여기서 임포트합니다 (동적 임포트)
        from playwright.async_api import async_playwright
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            for player_id in range(start_id, end_id + 1):
                if await self.check_player(page, player_id):
                    await self.process_player_data(page, player_id)

            await browser.close()
            logger.info(f"총 처리된 선수 수: {self.count}")

# Airflow에서 실행할 함수들
def install_playwright():
    """Playwright를 설치하는 함수"""
    import subprocess
    import sys
    
    try:
        import playwright
        logger.info("Playwright가 이미 설치되어 있습니다.")
    except ImportError:
        logger.info("Playwright 설치 중...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright"])
        logger.info("Playwright 설치 완료")
    
    try:
        logger.info("Playwright 브라우저 설치 중...")
        subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
        logger.info("Playwright 브라우저 설치 완료")
    except Exception as e:
        logger.error(f"Playwright 브라우저 설치 중 오류 발생: {e}")
        raise

def scrape_player_range(start_id, end_id, **kwargs):
    """지정된 범위의 선수 ID에 대해 스크래핑을 실행하는 함수"""
    # 비동기 루프 실행
    db_config = {
        "host": Variable.get("baseball_db_host", default_var="116.37.91.221"),
        "user": Variable.get("baseball_db_user", default_var="niscom"),
        "password": Variable.get("baseball_db_password", default_var="niscom"),
        "database": Variable.get("baseball_db_name", default_var="baseball"),
    }
    
    db_manager = DatabaseManager(**db_config)
    scraper = BaseballDataScraper(db_manager)
    
    # asyncio 이벤트 루프 실행
    loop = asyncio.get_event_loop()
    if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(scraper.run(start_id=start_id, end_id=end_id))
    finally:
        loop.close()

# DAG 기본 인자 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'baseball_pitcher_scraping',
    default_args=default_args,
    description='야구 투수 데이터 스크래핑 DAG',
    schedule_interval=None,  # 매일 자정에 실행
    start_date=days_ago(1),
    catchup=False,
)

# Playwright 설치 태스크
install_playwright_task = PythonOperator(
    task_id='install_playwright',
    python_callable=install_playwright,
    dag=dag,
)

# 선수 범위를 여러 구간으로 나누어 병렬 처리
# 예시: 50007-60000, 60001-70000, 70001-80000, 80001-90000, 90001-99811
player_ranges = [
    (50007, 60000),
    (60001, 70000),
    (70001, 80000),
    (80001, 90000),
    (90001, 99811),
]

scrape_tasks = []
for i, (start_id, end_id) in enumerate(player_ranges):
    task = PythonOperator(
        task_id=f'scrape_players_{start_id}_to_{end_id}',
        python_callable=scrape_player_range,
        op_kwargs={'start_id': start_id, 'end_id': end_id},
        dag=dag,
    )
    scrape_tasks.append(task)
    install_playwright_task >> task

# 이 스크립트가 Airflow에 의해 직접 임포트될 때만 DAG 객체를 노출
if __name__ == "__main__":
    dag.cli()