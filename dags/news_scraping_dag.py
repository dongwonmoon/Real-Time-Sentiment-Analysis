import pendulum
import requests
from bs4 import BeautifulSoup
import datetime
import json

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.crawling.crawling import get_urls_from_section, get_news_content

# -------------------------------------------------------------------
# Airflow DAG 정의
# -------------------------------------------------------------------

@dag(
    dag_id="naver_news_scraping_with_db",
    start_date=pendulum.datetime(2025, 7, 14, tz="Asia/Seoul"),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["scraping", "news", "db"],
    doc_md="10분마다 네이버 뉴스를 스크래핑하여 PostgreSQL에 저장합니다."
)
def naver_news_scraping_dag_with_db():

    @task
    def get_all_article_urls() -> list[str]:
        all_urls = []
        for section_id in range(100, 106):
            urls = get_urls_from_section(section_id)
            all_urls.extend(urls)
        unique_urls = list(set(all_urls))
        print(f"Found {len(unique_urls)} unique URLs from Naver.")
        return unique_urls

    @task
    def filter_new_urls(urls: list[str]) -> list[str]:
        """DB를 조회하여 이미 수집된 URL을 제외한 새로운 URL만 반환합니다."""
        if not urls:
            return []
            
        # Airflow Connection을 사용하여 DB에 연결
        hook = PostgresHook(postgres_conn_id="postgres_default")
        
        # 'url' IN ('url1', 'url2', ...) 쿼리를 위해 튜플 형태로 변환
        # SQL Injection을 방지하기 위해 hook.get_records 사용
        sql = "SELECT url FROM naver_news WHERE url = ANY(%s)"
        
        # DB에서 이미 존재하는 URL 목록 조회
        # hook.get_records는 튜플의 리스트를 반환합니다. e.g., [('url1',), ('url2',)]
        existing_urls_tuples = hook.get_records(sql, parameters=(urls,))
        existing_urls = {row[0] for row in existing_urls_tuples}
        
        # 새로운 URL만 필터링
        new_urls = [url for url in urls if url not in existing_urls]
        print(f"Total URLs: {len(urls)}, Existing URLs: {len(existing_urls)}, New URLs to scrape: {len(new_urls)}")
        return new_urls

    @task
    def scrape_and_save_news(url: str):
        """새로운 URL의 기사를 스크래핑하고 DB에 바로 저장합니다."""
        news_data = get_news_content(url)
        
        if not news_data or not news_data.get("title"):
            print(f"Failed to scrape or no title for URL: {url}")
            return

        hook = PostgresHook(postgres_conn_id="postgres_default")
        sql = """
            INSERT INTO naver_news (url, title, summary, content, news_time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING;
        """
        # ON CONFLICT ... DO NOTHING: 혹시라도 동시에 같은 URL을 처리하려 할 때 에러 대신 무시하도록 하는 안전장치
        hook.run(
            sql,
            parameters=(
                news_data["url"],
                news_data["title"],
                news_data["summary"],
                news_data["content"],
                news_data["time"]
            )
        )
        print(f"Successfully scraped and saved: {url}")

    # --- 태스크 의존성 정의 ---
    all_urls = get_all_article_urls()
    new_urls_to_scrape = filter_new_urls(all_urls)
    
    # 동적 태스크 매핑: 새로운 URL에 대해서만 스크래핑 및 저장 태스크 실행
    # 병렬 수행 (.expand)
    scrape_and_save_news.expand(url=new_urls_to_scrape)

# DAG 인스턴스화
naver_news_scraping_dag_with_db()