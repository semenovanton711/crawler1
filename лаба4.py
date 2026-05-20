import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import Counter
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


STOP_WORDS = set([
    "и", "в", "на", "с", "по", "к", "у", "о", "об", "от", "для", "из", "до", "за", "при", "без",
    "как", "что", "так", "это", "быть", "весь", "или", "но", "а", "же", "тот", "такой", "только",
    "уже", "ещё", "вот", "все", "они", "мы", "вы", "ты", "он", "она", "оно", "они",
    "the", "and", "to", "of", "a", "in", "for", "on", "with", "by", "is", "are", "was", "were",
    "this", "that", "these", "those", "be", "from", "or", "as", "at", "an"
])

MIN_WORD_LEN = 3
MAX_WORD_LEN = 30

visited = set()
queue = []
visited_lock = threading.Lock()
word_counter = Counter()
pages_crawled = 0


def normalize_url(url, base_domain):
    """Нормализует URL и проверяет, относится ли он к целевому домену."""
    parsed = urlparse(url)
    if parsed.netloc == "":

        return None


    if parsed.netloc != base_domain:
        return None


    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    if clean.endswith("/"):
        clean = clean[:-1]
    return clean


def extract_text_from_html(html):
    """Извлекает чистый текст из HTML."""
    soup = BeautifulSoup(html, 'html.parser')


    for script in soup(["script", "style", "nav", "footer", "header", "aside"]):
        script.decompose()

    text = soup.get_text(separator=" ")
    # Очистка от лишних пробелов и символов
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^a-zA-Zа-яА-ЯёЁ0-9\s]', '', text)  # убираем пунктуацию
    return text.lower()


def count_words_in_text(text):
    """Подсчитывает слова в тексте, исключая стоп-слова и короткие слова."""
    words = re.findall(r'\b[a-zA-Zа-яА-ЯёЁ]{3,30}\b', text)
    filtered = [w for w in words if w not in STOP_WORDS and len(w) >= MIN_WORD_LEN]
    return Counter(filtered)


def crawl_page(url, base_domain, depth, max_depth):
    global pages_crawled
    if depth > max_depth:
        return []

    with visited_lock:
        if url in visited:
            return []
        visited.add(url)

    print(f"[*] Crawling: {url} (depth={depth})")

    try:
        response = requests.get(url, timeout=10, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        if response.status_code != 200:
            print(f"   [!] HTTP {response.status_code} - skipped")
            return []
    except Exception as e:
        print(f"   [!] Error: {e}")
        return []


    text = extract_text_from_html(response.text)
    words_count = count_words_in_text(text)

    with visited_lock:
        word_counter.update(words_count)
        pages_crawled += 1


    soup = BeautifulSoup(response.text, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        full_url = urljoin(url, href)
        normalized = normalize_url(full_url, base_domain)
        if normalized and normalized not in visited:
            links.append(normalized)

    return links


def crawl(start_url, max_depth=3, max_workers=5, max_pages=None):
    global queue
    base_domain = urlparse(start_url).netloc
    queue = [(start_url, 0)]
    idx = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        while idx < len(queue):

            while idx < len(queue) and len(futures) < max_workers * 2:
                url, depth = queue[idx]
                idx += 1
                if url in visited:
                    continue
                future = executor.submit(crawl_page, url, base_domain, depth, max_depth)
                futures[future] = (url, depth)


            done = set()
            for future in as_completed(futures):
                url, depth = futures[future]
                try:
                    new_links = future.result()
                    for link in new_links:
                        if link not in [u for u, _ in queue]:
                            queue.append((link, depth + 1))
                except Exception as e:
                    print(f"Error processing {url}: {e}")
                done.add(future)


                if max_pages and pages_crawled >= max_pages:

                    for f in futures:
                        f.cancel()
                    return


                for d in done:
                    del futures[d]
                break


            time.sleep(0.5)

    print(f"\n[+] Crawling finished. Total pages crawled: {pages_crawled}")


def save_results(filename="output.json", top_n=10):
    """Сохраняет топ-N слов в JSON."""
    top_words = word_counter.most_common(top_n)
    result = {
        "total_pages": pages_crawled,
        "total_unique_words": len(word_counter),
        "top_words": top_words
    }
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    print(f"[+] Results saved to {filename}")


if __name__ == "__main__":
    start_url = "https://habr.com/ru/all/"  # главная страница статей хабра
    print("=== Habr.com Crawler (top 10 frequent words) ===\n")
    crawl(start_url, max_depth=2, max_workers=4, max_pages=50)  # глубина 2 (статьи с главной)
    save_results("habr_top10.json", top_n=10)
    print("\n--- TOP 10 WORDS ---")
    for word, count in word_counter.most_common(10):
        print(f"{word}: {count}")