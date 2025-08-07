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
MAX_PAGES = 1  # Mulai dengan 1 halaman dulu untuk testing
MAX_WORKERS = 2  # Kurangi worker untuk menghindari blokir
DELAY_RANGE = (3, 7)  # Tambah delay
REQUEST_TIMEOUT = (15, 30)
MAX_RETRIES = 5
BASE_URL = "https://www.kompas.com/cekfakta/data-dan-fakta"

# Setup logging
logging.basicConfig(
    filename='kompas_scraper_errors.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_random_delay():
    return random.uniform(*DELAY_RANGE)

def create_session():
    """Membuat session dengan retry mechanism"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[408, 429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.kompas.com/',
    })
    
    return session

def scrape_article(url, session):
    """Scrape konten artikel individual"""
    try:
        time.sleep(get_random_delay())
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        if "checkpoint" in response.url.lower():
            raise Exception("Terkena checkpoint/redirect")

        soup = BeautifulSoup(response.text, 'html.parser')

        # Cek apakah ada tombol "Show All" → buka halaman versi lengkap
        show_all_link = soup.find('a', class_='paging__link--show', href=True)
        if show_all_link and 'page=all' in show_all_link['href']:
            show_all_url = show_all_link['href']
            if not show_all_url.startswith('http'):
                show_all_url = f"https://www.kompas.com{show_all_url}"
            time.sleep(get_random_delay())
            response = session.get(show_all_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')  # Replace soup dengan yang baru

        # Ekstrak teks lengkap dari halaman final
        content_div = soup.find('div', class_='read__content')
        full_text = "N/A"
        if content_div:
            for element in content_div(['script', 'style', 'figure', 'img', 'nav', 'footer',
                                        'aside', 'form', 'iframe']):
                element.decompose()

            paragraphs = [p.get_text(strip=True) for p in content_div.find_all('p')]
            full_text = '\n'.join([p for p in paragraphs if p])

        # Ekstrak metadata
        date_tag = soup.find('div', class_='read__time')
        date = date_tag.get_text(strip=True) if date_tag else "N/A"

        author_tag = soup.find('div', class_='read__author')
        author = author_tag.get_text(strip=True) if author_tag else "N/A"

        # 🧩 Tambahkan tag di sini juga (lihat di bawah untuk detail)
        tag_container = soup.find('div', class_='tag__article__wrap')
        tags = []
        if tag_container:
            tag_links = tag_container.find_all('a', class_='tag__article__link')
            tags = [t.get_text(strip=True) for t in tag_links]

        return {
            'full_text': full_text,
            'date': date,
            'author': author,
            'tags': tags,
            'error': None
        }

    except Exception as e:
        error_msg = f"Error processing {url}: {str(e)}"
        logging.error(error_msg)
        return {
            'full_text': "N/A",
            'date': "N/A",
            'author': "N/A",
            'tags': [],
            'error': error_msg
        }

def scrape_page(page_num, session):
    """Scrape list artikel dalam satu halaman"""
    try:
        time.sleep(get_random_delay())
        url = f"{BASE_URL}/{page_num}" if page_num > 1 else BASE_URL
        logging.info(f"Scraping page: {url}")
        
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        if "checkpoint" in response.url.lower():
            raise Exception("Terkena checkpoint/redirect")
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Cari semua artikel - sesuai dengan struktur HTML yang diberikan
        articles = []
        
        # Artikel headline besar
        headline_big = soup.find('div', class_='cekfakta-headlineBig')
        if headline_big:
            articles.append(headline_big)
        
        # Artikel headline kecil
        headline_small = soup.find_all('div', class_='cekfakta-headlineSmall-item')
        if headline_small:
            articles.extend(headline_small)
        
        # Artikel dalam grid
        grid_articles = soup.find_all('div', class_='cekfakta-list')
        if grid_articles:
            articles.extend(grid_articles)
        
        page_data = []
        article_urls = []
        
        for article in articles:
            try:
                # Cari link artikel
                link_tag = article.find('a', class_=lambda x: x and ('cekfakta-headline-link' in x or 'cekfakta-list-link' in x))
                if not link_tag or not link_tag.get('href'):
                    continue
                    
                article_url = link_tag['href']
                if not article_url.startswith('http'):
                    article_url = f"https://www.kompas.com{article_url}"
                
                # Cari judul
                title_tag = article.find(['h1', 'h2', 'h3'], class_=lambda x: x and ('textBig' in x or 'textSmall' in x or 'cekfakta-list-title' in x))
                title = title_tag.get_text(strip=True) if title_tag else "N/A"
                
                # Cari tanggal
                date_tag = article.find('p', class_=lambda x: x and ('text-date' in x or 'cekfakta-text-date' in x))
                date = date_tag.get_text(strip=True) if date_tag else "N/A"
                formatted_date = format_timestamp(date)

                article_urls.append(article_url)
                
                page_data.append({
                    'Title': title,
                    'Timestamp': formatted_date,
                    'Tags': None,
                    'FullText': None,
                    'Author': None,
                    'Url': article_url,
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
    print(f"🚀 Memulai scraping {MAX_PAGES} halaman dari Kompas Cek Fakta...")
    logging.info("Memulai scraping Kompas Cek Fakta")
    
    all_data = []
    session = create_session()
    
    # Test koneksi pertama
    try:
        test_resp = session.get(BASE_URL, timeout=REQUEST_TIMEOUT)
        test_resp.raise_for_status()
        if "checkpoint" in test_resp.url.lower():
            raise Exception("Terkena checkpoint sejak awal")
    except Exception as e:
        print(f"❌ Gagal mengakses Kompas: {str(e)}")
        print("Coba lagi nanti atau periksa apakah Anda terkena blokir")
        return
    
    # Tahap 1: Scrape semua URL artikel
    print("\n🔍 Mengumpulkan URL artikel...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_page, page_num, session): page_num 
                  for page_num in range(1, MAX_PAGES + 1)}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Halaman"):
            page_num = futures[future]
            try:
                page_data, _ = future.result()
                if page_data:
                    all_data.extend(page_data)
                    logging.info(f"Page {page_num}: Found {len(page_data)} articles")
                else:
                    logging.warning(f"Page {page_num}: No articles found")
            except Exception as e:
                logging.error(f"Critical error on page {page_num}: {str(e)}")
    
    if not all_data:
        print("❌ Tidak ada artikel yang berhasil dikumpulkan. Periksa log untuk detail.")
        return
    
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
                all_data[idx]['Author'] = result['author']
                all_data[idx]['Tags'] = ', '.join(result['tags']) if result['tags'] else None
                if all_data[idx]['Timestamp'] == "N/A":
                    all_data[idx]['Timestamp'] = result['date']
            except Exception as e:
                logging.error(f"Error processing result for {url}: {str(e)}")
    
    # Simpan hasil
    print("\n💾 Menyimpan hasil...")
    df = pd.DataFrame(all_data)
    
    # Filter out empty articles
    df = df[df['FullText'] != "N/A"]
    
    if len(df) == 0:
        print("❌ Tidak ada artikel dengan konten yang valid. Periksa log untuk detail.")
        return
    
    output_file = "kompas_cekfakta_data.xlsx"
    df.to_excel(output_file, index=False)
    print(f"Disimpan: {output_file} ({len(df)} data)")
    
    error_urls = [article['Url'] for article in all_data if article['FullText'] == "N/A"]
    if error_urls:
        with open('error_urls_kompas.txt', 'w') as f:
            f.write('\n'.join(error_urls))
        print(f"\n⚠️ {len(error_urls)} artikel gagal. URL tersimpan di error_urls_kompas.txt")
    
    print("\n🎉 Selesai! Data berita valid telah disimpan.")

if __name__ == "__main__":
    main()