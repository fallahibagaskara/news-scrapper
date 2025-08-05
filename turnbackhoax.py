import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import socket
from urllib3.exceptions import MaxRetryError, NameResolutionError

# Config
MAX_PAGES = 5  # ~10,000 articles (20 articles per page)
MAX_WORKERS = 10  # Threads for parallel processing
REQUEST_TIMEOUT = 30
DELAY_RANGE = (1, 3)  # Random delay between requests in seconds
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
]

month_translation = {
    'January': 'Januari',
    'February': 'Februari',
    'March': 'Maret',
    'April': 'April',
    'May': 'Mei',
    'June': 'Juni',
    'July': 'Juli',
    'August': 'Agustus',
    'September': 'September',
    'October': 'Oktober',
    'November': 'November',
    'December': 'Desember'
}

def translate_month(timestamp):
    if isinstance(timestamp, str):  # Pastikan ini string
        for eng, indo in month_translation.items():
            timestamp = timestamp.replace(eng, indo)
    return timestamp

def get_random_delay():
    return random.uniform(*DELAY_RANGE)

def get_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    })
    return session

def get_article_data(url, session):
    try:
        time.sleep(get_random_delay())
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract full text
        entry_content = soup.find('div', class_='entry-content')
        full_text = "N/A"
        if entry_content:
            for element in entry_content(['script', 'style', 'figure', 'img', 'nav', 'footer', 'aside', 'form', 'iframe']):
                element.decompose()
            full_text = '\n'.join([p.get_text(strip=True) for p in entry_content.find_all(['p', 'h2', 'h3', 'h4']) if p.get_text(strip=True)])
        
        # Extract tags
        tags = []
        meta_categories = soup.find('span', class_='entry-meta-categories')
        if meta_categories:
            tags = [a.get_text(strip=True) for a in meta_categories.find_all('a')]
        
        return {
            'full_text': full_text,
            'tags': ';'.join(tags) if tags else "N/A",
            'error': None
        }
        
    except Exception as e:
        return {
            'full_text': "N/A",
            'tags': "N/A",
            'error': str(e)
        }

def scrape_page(page_num, session):
    base_url = f"https://turnbackhoax.id/page/{page_num}/"
    try:
        time.sleep(get_random_delay())
        response = session.get(base_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.find_all('article', class_='mh-loop-item')
        
        page_data = []
        article_urls = []
        
        for article in articles:
            try:
                title_tag = article.find('h3', class_='entry-title')
                if not title_tag or not title_tag.find('a'):
                    continue
                    
                url = title_tag.find('a')['href']
                title = title_tag.get_text(strip=True)
                
                date_tag = article.find('span', class_='mh-meta-date')
                timestamp = date_tag.get_text(strip=True) if date_tag else "N/A"
                timestamp = translate_month(timestamp)
                
                author_tag = article.find('span', class_='mh-meta-author')
                author = author_tag.get_text(strip=True) if author_tag else "N/A"
                
                article_urls.append(url)
                
                page_data.append({
                    'Title': title,
                    'Timestamp': timestamp,
                    'FullText': None,
                    'Tags': None,
                    'Author': author,
                    'Url': url
                })
                
            except Exception as e:
                print(f"Error processing article preview on page {page_num}: {e}")
                continue
                
        return page_data, article_urls
        
    except Exception as e:
        print(f"Error scraping page {page_num}: {e}")
        return [], []

def scrape_turnbackhoax():
    all_data = []
    session = get_session()
    
    # Tahap 1: Ambil semua URL artikel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_page = {executor.submit(scrape_page, page_num, session): page_num 
                         for page_num in range(1, MAX_PAGES + 1)}
        
        for future in as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                page_data, article_urls = future.result()
                all_data.extend(page_data)
                
                print(f"✅ Halaman {page_num} selesai. Total artikel: {len(all_data)}")
            except Exception as e:
                 print(f"❌ Gagal di halaman {page_num}: {e}")
    
     # Tahap 2: Scrape konten lengkap
    print("\n🔍 Mulai scraping konten artikel...")
    url_to_index = {article['Url']: idx for idx, article in enumerate(all_data)}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(get_article_data, article['Url'], session): article['Url'] 
                        for article in all_data}
        
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                idx = url_to_index[url]
                
                all_data[idx]['FullText'] = result['full_text']
                all_data[idx]['Tags'] = result['tags']
                
                if result['error']:
                    print(f"Error processing article {url}: {result['error']}")
                
                # Progress tracker
                processed = sum(1 for article in all_data if article['FullText'] is not None)
                if processed % 100 == 0:
                    print(f"📌 Progress: {processed}/{len(all_data)} artikel")
            except Exception as e:
                print(f"⚠️ Gagal proses artikel {url}: {e}")
    
    return pd.DataFrame(all_data)

# Run scraping
print(f"🛠️ Memulai scraping {MAX_PAGES} halaman (~{MAX_PAGES*20} artikel)...")
df = scrape_turnbackhoax()

# Save to Excel in chunks to prevent memory issues
chunk_size = 2000
for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i + chunk_size]
    output_file = f"turnbackhoax_data_part_{i//chunk_size + 1}.xlsx"
    chunk.to_excel(output_file, index=False)
    print(f"{len(chunk)} data tersimpan di {output_file}")

print("🎉 Selesai! ")