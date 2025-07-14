import requests
from bs4 import BeautifulSoup
import datetime

def get_urls_from_section(section_id: int):
    """지정된 섹션 ID에서 기사 URL 목록을 가져옵니다."""
    url = f"https://news.naver.com/section/{section_id}"
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() # 오류 발생 시 예외를 발생시킴
        html = BeautifulSoup(response.text, "html.parser")
        return [a["href"] for a in html.select("a.sa_thumb_link._NLOG_IMPRESSION")]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching section {section_id}: {e}")
        return []

def get_news_content(url: str):
    """뉴스 URL에서 제목, 시간, 요약, 본문을 스크래핑합니다."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        html = BeautifulSoup(response.text, "html.parser")

        title = html.select_one("h2#title_area")
        time_span = html.select_one("span.media_end_head_info_datestamp_time._ARTICLE_DATE_TIME")
        article = html.select_one("article#dic_area")
        
        # 데이터가 없는 경우를 대비한 None 처리
        news_data = {"url": url, "title": None, "time": None, "summary": None, "content": None}

        if title:
            news_data["title"] = title.get_text(strip=True)
            
        if time_span:
            time_str = time_span.get_text(strip=True).replace("오후", "PM").replace("오전", "AM")
            news_data["time"] = datetime.datetime.strptime(time_str, "%Y.%m.%d. %p %I:%M").isoformat()
            
        if article:
            summary = article.select_one("strong.media_end_summary")
            if summary:
                news_data["summary"] = summary.get_text(strip=True)
            
            news_data["content"] = article.get_text(strip=True, separator="\n")
            
        return news_data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching article {url}: {e}")
        return None # 실패 시 None 반환

if __name__ == "__main__":
    url = "https://news.naver.com/section/100"
    urls = get_urls_from_section(100)
    
    print(urls[0])
    news = get_news_content(urls[0])
    
    print(news)