import asyncio
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import re
import csv
import os
import random
from urllib.parse import urljoin
import json
import aiohttp
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Economic Times Cloud Scraper")

# ─── GLOBAL CONSTANTS ───
BASE_URL = "https://economictimes.indiatimes.com"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}
RETRY_LIMIT = 3
DELAY_BETWEEN_REQUESTS = 2
PROXIES = [] 

class ScrapeRequest(BaseModel):
    start_date: str  # Format: YYYY-MM-DD
    end_date: str    # Format: YYYY-MM-DD

# ─── ASYNC FETCH ───
async def fetch(session, url):
    for attempt in range(1, RETRY_LIMIT + 1):
        proxy = random.choice(PROXIES) if PROXIES else None
        try:
            async with session.get(url, proxy=proxy, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    print(f"Attempt {attempt}/{RETRY_LIMIT} failed with status {response.status}: {url}")
        except Exception as e:
            print(f"Attempt {attempt}/{RETRY_LIMIT} encountered an error: {e}")

        if attempt < RETRY_LIMIT:
            await asyncio.sleep(2 ** attempt)

    print(f"Failed to fetch {url} after {RETRY_LIMIT} retries.")
    return None

# ─── ARCHIVE URL ───
def construct_archive_url(date):
    reference_date = datetime(2025, 1, 1)
    base_starttime = 45658
    start_time = base_starttime + (date - reference_date).days
    url = f"{BASE_URL}/archivelist/year-{date.year},month-{date.month},starttime-{start_time}.cms"
    print(f" Calculated starttime for {date.strftime('%Y-%m-%d')}: {start_time}")
    return [url]

# ─── PARSE ARCHIVE PAGE ───
async def parse_archive_page(session, date):
    url_formats = construct_archive_url(date)
    for url in url_formats:
        html = await fetch(session, url)
        if not html:
            continue

        soup = BeautifulSoup(html, 'html.parser')
        page_content = soup.select_one("#pageContent ul.content")
        if not page_content:
            print(" No archive section found on the page.")
            return []

        links = page_content.find_all("a", href=True)
        articles = []

        for a in links:
            href = a.get("href")
            text = a.get_text(strip=True)
            if href and text and len(text) > 10:
                if href.startswith("/"):
                    full_url = BASE_URL + href
                elif not href.startswith("http"):
                    full_url = urljoin(BASE_URL, href)
                else:
                    full_url = href

                if "/articleshow/" in full_url and "javascript:" not in full_url:
                    articles.append({
                        "date": date.strftime("%Y-%m-%d"),
                        "headline": text,
                        "link": full_url
                    })

        if articles:
            seen_links = set()
            unique_articles = []
            for article in articles:
                if article["link"] not in seen_links:
                    seen_links.add(article["link"])
                    unique_articles.append(article)

            print(f" Found {len(unique_articles)} unique articles inside archive section.")
            return unique_articles

        print(" Archive section found but no articles.")
        return []

    return []

# ─── FETCH ARTICLE DETAILS ───
async def fetch_article_details(session, articles):
    semaphore = asyncio.Semaphore(3)

    async def fetch_single_article(article):
        async with semaphore:
            await asyncio.sleep(random.uniform(1, 3))
            html = await fetch(session, article["link"])
            for key in ["articleBody", "inLanguage", "keywords", "description",
                       "datePublished", "dateModified", "url", "articleSection", "headline_raw"]:
                article[key] = "NA"

            if html:
                soup = BeautifulSoup(html, 'html.parser')
                json_ld_scripts = soup.find_all('script', type='application/ld+json')

                for script in json_ld_scripts:
                    try:
                        data = json.loads(script.string)
                        if isinstance(data, list):
                            data = data[0]
                        if data.get('@type') == 'NewsArticle':
                            article["articleBody"] = data.get("articleBody", "NA").replace("\n"," ").replace("\\","").strip()
                            article["inLanguage"] = data.get("inLanguage","NA")
                            article["keywords"] = str(data.get("keywords","NA"))
                            article["description"] = data.get("description","NA")
                            article["datePublished"] = data.get("datePublished","NA")
                            article["dateModified"] = data.get("dateModified","NA")
                            main_entity = data.get("mainEntityOfPage","NA")
                            if isinstance(main_entity, dict):
                                article["url"] = main_entity.get("@id","NA")
                            else:
                                article["url"] = data.get("url","NA")
                            article["articleSection"] = data.get("articleSection","NA")
                            article["headline_raw"] = data.get("headline","NA")
                            break
                    except Exception:
                        continue

                if article["articleBody"] == "NA":
                    patterns = {
                        "articleBody": r'"articleBody"\s*:\s*"(.*?)"',
                        "inLanguage": r'"inLanguage"\s*:\s*"(.*?)"',
                        "keywords": r'"keywords"\s*:\s*\[(.*?)\]',
                        "description": r'"description"\s*:\s*"(.*?)"',
                        "datePublished": r'"datePublished"\s*:\s*"(.*?)"',
                        "dateModified": r'"dateModified"\s*:\s*"(.*?)"',
                        "url": r'"mainEntityOfPage"\s*:\s*"(.*?)"',
                        "articleSection": r'"articleSection"\s*:\s*"(.*?)"',
                        "headline_raw": r'"headline"\s*:\s*"(.*?)"'
                    }
                    for key, pattern in patterns.items():
                        match = re.search(pattern, html, re.DOTALL)
                        if match:
                            value = match.group(1)
                            if key == "articleBody":
                                value = value.replace("\n"," ").replace("\\","").strip()
                            elif key == "keywords":
                                value = value.replace('"','')
                            article[key] = value
            return article

    batch_size = 25
    for i in range(0, len(articles), batch_size):
        batch = articles[i:i+batch_size]
        tasks = [fetch_single_article(article) for article in batch]
        await asyncio.gather(*tasks, return_exceptions=True)
        if i + batch_size < len(articles):
            await asyncio.sleep(3)
        print(f"Processed {min(i+batch_size,len(articles))}/{len(articles)} articles")
    return articles

# ─── SAVE TO CSV ───
def save_to_csv(data, filepath):
    try:
        dir_path = os.path.dirname(filepath)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        if not data:
            print(f"No data to save for {filepath}")
            return
        keys = ["date","headline","link","articleBody","inLanguage","keywords",
                "description","datePublished","dateModified","url","articleSection","headline_raw"]
        with open(filepath,"w",newline="",encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)
        print(f" Saved {len(data)} articles to '{filepath}'")
    except Exception as e:
        print(f" Error saving to {filepath}: {e}")

# ─── SCRAPE DATE RANGE ───
async def scrape_date_range(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    base_path = "data"
    os.makedirs(base_path, exist_ok=True)
    total_articles = 0
    current_date = start_date

    timeout = aiohttp.ClientTimeout(total=120, connect=60)
    connector = aiohttp.TCPConnector(limit=5, limit_per_host=3)
    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout, connector=connector) as session:
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n Scraping {date_str}...")
            try:
                articles = await parse_archive_page(session, current_date)
                if articles:
                    detailed_articles = await fetch_article_details(session, articles)
                    daily_file = f"{base_path}/ET_{date_str.replace('-','_')}.csv"
                    save_to_csv(detailed_articles, daily_file)
                    total_articles += len(detailed_articles)
                    print(f" Saved {len(detailed_articles)} articles for {date_str}")
                else:
                    print(f" No articles found for {date_str}")
                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
            except Exception as e:
                print(f" Error processing {date_str}: {e}")
            current_date += timedelta(days=1)
    print(f"\n Scraping completed! Total articles: {total_articles}")
    return total_articles

# ─── FASTAPI ENDPOINTS ───
@app.get("/")
def read_root():
    return {"status": "Service is running!"}

@app.post("/scrape")
async def trigger_scraping(payload: ScrapeRequest, background_tasks: BackgroundTasks):
    try:
        # Runs scraping in background so API call does not timeout
        background_tasks.add_task(scrape_date_range, payload.start_date, payload.end_date)
        return {
            "message": f"Scraping job started for range {payload.start_date} to {payload.end_date}.",
            "status": "Processing in background"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
    url_formats = construct_archive_url(date)
    for url in url_formats:
        html = await fetch(session, url)
        if not html:
            continue

        soup = BeautifulSoup(html, 'html.parser')
        page_content = soup.select_one("#pageContent ul.content")
        if not page_content:
            print(" No archive section found on the page.")
            return []

        links = page_content.find_all("a", href=True)
        articles = []

        for a in links:
            href = a.get("href")
            text = a.get_text(strip=True)
            if href and text and len(text) > 10:
                if href.startswith("/"):
                    full_url = BASE_URL + href
                elif not href.startswith("http"):
                    full_url = urljoin(BASE_URL, href)
                else:
                    full_url = href

                if "/articleshow/" in full_url and "javascript:" not in full_url:
                    articles.append({
                        "date": date.strftime("%Y-%m-%d"),
                        "headline": text,
                        "link": full_url
                    })

        if articles:
            seen_links = set()
            unique_articles = []
            for article in articles:
                if article["link"] not in seen_links:
                    seen_links.add(article["link"])
                    unique_articles.append(article)

            print(f" Found {len(unique_articles)} unique articles inside archive section.")
            return unique_articles

        print(" Archive section found but no articles.")
        return []

    return []

# ─── FETCH ARTICLE DETAILS ───
async def fetch_article_details(session, articles):
    semaphore = asyncio.Semaphore(3)

    async def fetch_single_article(article):
        async with semaphore:
            await asyncio.sleep(random.uniform(1, 3))
            html = await fetch(session, article["link"])
            for key in ["articleBody", "inLanguage", "keywords", "description",
                       "datePublished", "dateModified", "url", "articleSection", "headline_raw"]:
                article[key] = "NA"

            if html:
                soup = BeautifulSoup(html, 'html.parser')
                json_ld_scripts = soup.find_all('script', type='application/ld+json')

                for script in json_ld_scripts:
                    try:
                        data = json.loads(script.string)
                        if isinstance(data, list):
                            data = data[0]
                        if data.get('@type') == 'NewsArticle':
                            article["articleBody"] = data.get("articleBody", "NA").replace("\n"," ").replace("\\","").strip()
                            article["inLanguage"] = data.get("inLanguage","NA")
                            article["keywords"] = str(data.get("keywords","NA"))
                            article["description"] = data.get("description","NA")
                            article["datePublished"] = data.get("datePublished","NA")
                            article["dateModified"] = data.get("dateModified","NA")
                            main_entity = data.get("mainEntityOfPage","NA")
                            if isinstance(main_entity, dict):
                                article["url"] = main_entity.get("@id","NA")
                            else:
                                article["url"] = data.get("url","NA")
                            article["articleSection"] = data.get("articleSection","NA")
                            article["headline_raw"] = data.get("headline","NA")
                            break
                    except Exception:
                        continue

                if article["articleBody"] == "NA":
                    patterns = {
                        "articleBody": r'"articleBody"\s*:\s*"(.*?)"',
                        "inLanguage": r'"inLanguage"\s*:\s*"(.*?)"',
                        "keywords": r'"keywords"\s*:\s*\[(.*?)\]',
                        "description": r'"description"\s*:\s*"(.*?)"',
                        "datePublished": r'"datePublished"\s*:\s*"(.*?)"',
                        "dateModified": r'"dateModified"\s*:\s*"(.*?)"',
                        "url": r'"mainEntityOfPage"\s*:\s*"(.*?)"',
                        "articleSection": r'"articleSection"\s*:\s*"(.*?)"',
                        "headline_raw": r'"headline"\s*:\s*"(.*?)"'
                    }
                    for key, pattern in patterns.items():
                        match = re.search(pattern, html, re.DOTALL)
                        if match:
                            value = match.group(1)
                            if key == "articleBody":
                                value = value.replace("\n"," ").replace("\\","").strip()
                            elif key == "keywords":
                                value = value.replace('"','')
                            article[key] = value
            return article

    batch_size = 25
    for i in range(0, len(articles), batch_size):
        batch = articles[i:i+batch_size]
        tasks = [fetch_single_article(article) for article in batch]
        await asyncio.gather(*tasks, return_exceptions=True)
        if i + batch_size < len(articles):
            await asyncio.sleep(3)
        print(f"Processed {min(i+batch_size,len(articles))}/{len(articles)} articles")
    return articles

# ─── SAVE TO CSV ───
def save_to_csv(data, filepath):
    try:
        dir_path = os.path.dirname(filepath)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        if not data:
            print(f"No data to save for {filepath}")
            return
        keys = ["date","headline","link","articleBody","inLanguage","keywords",
                "description","datePublished","dateModified","url","articleSection","headline_raw"]
        with open(filepath,"w",newline="",encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)
        print(f" Saved {len(data)} articles to '{filepath}'")
    except Exception as e:
        print(f" Error saving to {filepath}: {e}")

# ─── SCRAPE DATE RANGE ───
async def scrape_date_range(start_date_str, end_date_str):
    print("=" * 60, flush=True)
    print(f"Started scraping from {start_date_str} to {end_date_str}", flush=True)
    print("=" * 60, flush=True)

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    base_path = "data"
    os.makedirs(base_path, exist_ok=True)

    total_articles = 0
    current_date = start_date

    timeout = aiohttp.ClientTimeout(total=120, connect=60)
    connector = aiohttp.TCPConnector(limit=5, limit_per_host=3)

    async with aiohttp.ClientSession(
        headers=HEADERS,
        timeout=timeout,
        connector=connector
    ) as session:

        while current_date <= end_date:

            date_str = current_date.strftime("%Y-%m-%d")

            print(f"\nProcessing {date_str}...", flush=True)

            try:
                articles = await parse_archive_page(session, current_date)

                if articles:

                    print(f"Found {len(articles)} archive articles", flush=True)

                    detailed_articles = await fetch_article_details(session, articles)

                    daily_file = f"{base_path}/ET_{date_str.replace('-', '_')}.csv"

                    save_to_csv(detailed_articles, daily_file)

                    total_articles += len(detailed_articles)

                    print(
                        f"Saved {len(detailed_articles)} articles for {date_str}",
                        flush=True,
                    )

                else:
                    print(f"No articles found for {date_str}", flush=True)

                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

            except Exception as e:
                print(f"Error processing {date_str}: {e}", flush=True)

            current_date += timedelta(days=1)

    print("=" * 60, flush=True)
    print(f"Scraping completed. Total articles = {total_articles}", flush=True)
    print("=" * 60, flush=True)

    return total_articles

# ─── FASTAPI ENDPOINTS ───
@app.get("/")
def read_root():
    return {"status": "Service is running!"}

@app.post("/scrape")
async def trigger_scraping(payload: ScrapeRequest):
    try:

        total = await scrape_date_range(
            payload.start_date,
            payload.end_date
        )

        return {
            "status": "Completed",
            "articles_scraped": total,
            "date_range": {
                "start_date": payload.start_date,
                "end_date": payload.end_date
            }
        }

    except Exception as e:
        print(f"Scraping failed: {e}", flush=True)
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
