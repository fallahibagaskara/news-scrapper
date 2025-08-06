import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from tqdm import tqdm
import logging

# Konfigurasi
MAX_PAGES = 500  # 500 halaman x ~20 artikel = 10.000 data
MAX_WORKERS = 5  # Lebih kecil untuk mengurangi timeout
DELAY_RANGE = (2, 5)  # Delay antara request (detik)
REQUEST_TIMEOUT = (10, 30)  # (connect timeout, read timeout)
MAX_RETRIES = 3  # Jumlah percobaan ulang saat gagal

# Setup logging
logging.basicConfig(
    filename='scraper_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Terjemahan bulan
MONTH_TRANSLATION = {
    'January': 'Januari', 'February': 'Februari', 'March': 'Maret',
    'April': 'April', 'May': 'Mei', 'June': 'Juni',
    'July': 'Juli', 'August': 'Agustus', 'September': 'September',
    'October': 'Oktober', 'November': 'November', 'December': 'Desember'
}

def translate_month(timestamp):
    if isinstance(timestamp, str):
        for eng, indo in MONTH_TRANSLATION.items():
            timestamp = timestamp.replace(eng, indo)
    return timestamp

def get_random_delay():
    return random.uniform(*DELAY_RANGE)

def create_session():
    """Membuat session dengan retry mechanism"""
    session = requests.Session()
    
    # Retry strategy
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[408, 429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    # Header
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
    })
    
    return session

def scrape_article(url, session):
    """Scrape konten artikel individual"""
    try:
        time.sleep(get_random_delay())
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Ekstrak teks lengkap
        entry_content = soup.find('div', class_='entry-content')
        full_text = "N/A"
        if entry_content:
            # Hapus elemen yang tidak diinginkan
            for element in entry_content(['script', 'style', 'figure', 'img', 'nav', 'footer', 'aside', 'form', 'iframe']):
                element.decompose()
            
            # Gabungkan teks dari paragraf dan heading
            paragraphs = []
            for p in entry_content.find_all(['p', 'h2', 'h3', 'h4']):
                text = p.get_text(strip=True)
                if text:
                    paragraphs.append(text)
            full_text = '\n'.join(paragraphs)
        
        # Ekstrak tags
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
        error_msg = f"Error processing {url}: {str(e)}"
        logging.error(error_msg)
        return {
            'full_text': "N/A",
            'tags': "N/A",
            'error': error_msg
        }

def scrape_page(page_num, session):
    """Scrape list artikel dalam satu halaman"""
    try:
        time.sleep(get_random_delay())
        url = f"https://turnbackhoax.id/page/{page_num}/"
        response = session.get(url, timeout=REQUEST_TIMEOUT)
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
                error_msg = f"Error processing article on page {page_num}: {str(e)}"
                logging.error(error_msg)
                continue
                
        return page_data, article_urls
        
    except Exception as e:
        error_msg = f"Error scraping page {page_num}: {str(e)}"
        logging.error(error_msg)
        return [], []

def main():
    print(f"🚀 Memulai scraping {MAX_PAGES} halaman (~{MAX_PAGES*20} artikel)...")
    
    all_data = []
    session = create_session()
    
    # Tahap 1: Scrape semua URL artikel
    print("\n🔍 Mengumpulkan URL artikel...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_page, page_num, session): page_num 
                  for page_num in range(1, MAX_PAGES + 1)}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Halaman"):
            page_num = futures[future]
            try:
                page_data, _ = future.result()
                all_data.extend(page_data)
            except Exception as e:
                logging.error(f"Critical error on page {page_num}: {str(e)}")
    
    print(f"✅ Total URL terkumpul: {len(all_data)}")
    
    # Tahap 2: Scrape konten lengkap
    print("\n📖 Mengambil konten artikel...")
    url_to_index = {article['Url']: idx for idx, article in enumerate(all_data)}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_article, article['Url'], session): article['Url'] 
                 for article in all_data}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Artikel"):
            url = futures[future]
            try:
                result = future.result()
                idx = url_to_index[url]
                all_data[idx]['FullText'] = result['full_text']
                all_data[idx]['Tags'] = result['tags']
            except Exception as e:
                logging.error(f"Error processing result for {url}: {str(e)}")
    
    # Simpan hasil
    print("\n💾 Menyimpan hasil...")
    df = pd.DataFrame(all_data)
    
    # Simpan per chunk
    chunk_size = 2000
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        output_file = f"turnbackhoax_data_part_{i//chunk_size + 1}.xlsx"
        chunk.to_excel(output_file, index=False)
        print(f"Disimpan: {output_file} ({len(chunk)} data)")
    
    # Simpan URL yang error
    error_urls = [article['Url'] for article in all_data if article['FullText'] == "N/A"]
    if error_urls:
        with open('error_urls.txt', 'w') as f:
            f.write('\n'.join(error_urls))
        print(f"\n⚠️ {len(error_urls)} artikel gagal. URL tersimpan di error_urls.txt")
    
    print("\n🎉 Selesai! Semua data telah disimpan.")

if __name__ == "__main__":
    main()