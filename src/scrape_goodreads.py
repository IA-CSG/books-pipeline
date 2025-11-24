# src/scrape_goodreads.py

import json
import os
import re
import asyncio
import time
from typing import List, Dict, Tuple, Optional
from enum import Enum

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

BASE_URL = "https://www.goodreads.com"

# Valores por defecto en el caso de que no existan en el fichero .env
defaultQuery = "data science"
defaultMax_books = 15
defaultUser_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

# Rutas base del proyecto
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
LANDING_DIR = os.path.join(BASE_DIR, "landing")
COVERS_DIR = os.path.join(BASE_DIR, "covers")

class Backend(str, Enum):
    REQUESTS = "requests"
    PLAYWRIGHT = "playwright"

def parse_rating_block(text: str) -> Tuple[Optional[float], Optional[int]]:
        
    # Ejemplo de text: '4.23 avg rating — 1,234 ratings' devuelve (rating_float, ratings_count_int) o (None, None)
    
    if not text:
        return None, None
    try:
        parts = text.strip().split("avg rating")
        rating_str = parts[0].strip()
        rating = float(rating_str)

        # Buscar 'ratings'
        ratings_part = text.split("—")[-1]
        ratings_part = (
            ratings_part.replace("ratings", "")
            .replace("rating", "")
            .replace(",", "")
            .strip()
        )
        ratings_count = int(ratings_part)
        return rating, ratings_count
    except Exception:
        return None, None


# Dada la URL de un libro en Goodreads, intenta extraer ISBN10, ISBN13 y ASIN
# Devuelve (isbn10, isbn13, asin). Si no se encuentran, devuelve None en cada campo.

def fetch_isbn_from_book_page(book_url: str, user_agent: Optional[str] = None,) -> Tuple[Optional[str], Optional[str], Optional[str]]: 

    headers = {"User-Agent": user_agent or "Mozilla/5.0"}

    try:
        resp = requests.get(book_url, headers=headers, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [ERROR] No se pudo obtener la ficha del libro: {book_url} -> {e}")
        return None, None, None

    html = resp.text
    soup = BeautifulSoup(html, "lxml")

    isbn10: Optional[str] = None
    isbn13: Optional[str] = None
    asin: Optional[str] = None

    # Busca el ISBN en el HTML/JSON crudo
    if not (isbn10 and isbn13 and asin):
        full_text = soup.get_text(" ", strip=True)
        raw_html = html  # HTML original

        # Intento específico para JSON tipo ,"isbn":"1491912057","isbn13":"9781491912058"
        if not isbn13:
            m13_json = re.search(r'"isbn13"\s*:\s*"([0-9\-]{13,17})"', raw_html, re.IGNORECASE)
            if m13_json:
                candidate = m13_json.group(1).replace("-", "")
                if len(candidate) == 13 and candidate.isdigit():
                    isbn13 = candidate

        if not isbn10:
            # Buscamos SOLO valores con exactamente 10 caracteres (ISBN10)
            m10_json = re.search(r'"isbn"\s*:\s*"([0-9X]{10})"', raw_html, re.IGNORECASE)
            if m10_json:
                candidate = m10_json.group(1)
                if len(candidate) == 10 and all(c.isdigit() or c == "X" for c in candidate):
                    isbn10 = candidate

        # Intento específico de ASIN en JSON: "asin":"XXXXXXXXXX"
        if not asin:
            m_asin_json = re.search(r'"asin"\s*:\s*"([A-Z0-9]{10})"', raw_html, re.IGNORECASE)
            if m_asin_json:
                asin = m_asin_json.group(1).upper()

    return isbn10, isbn13, asin


def parse_books_from_html(html: str, max_books: int, user_agent: Optional[str], fetch_isbn: bool,) -> List[Dict]:

    # Parseo de la página de resultados de búsqueda de Goodreads.
    # Si fetch_isbn=true, entra a la ficha de cada libro para extraer ISBN10/13/ASIN.

    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table.tableList tr")

    # Para ver todas las clases (por si queremos sacar alguna más además de bookTitle y authorName)
    #classes = set()
    #tags = set()
    #for row in rows:
        #for tag in row.find_all(True):
            #tags.add(tag.name)
            #if "class" in tag.attrs:
                #classes.update(tag["class"])
    #print("TAGS:", tags)
    #print("CLASES:", classes)

    # Para ver el contenido de las filas
    #for i, row in enumerate(rows, start=1):
        #print(f"FILA: {i}")
        #print(row.prettify())
        #break
        
    # Muestra los titulos encontrados por página (máx. hasta max_books)
    for i, row in enumerate(rows[:max_books], start=1):
        title_tag = row.select_one("a.bookTitle")
        if title_tag:
            print(f"{i}. {title_tag.get_text(strip=True)}")
        else:
            print(f"{i}. [SIN TÍTULO]")

    books: List[Dict] = []

    for row in rows:
        if len(books) >= max_books:
            break
                
        title_tag = row.select_one("a.bookTitle")
        author_tag = row.select_one("a.authorName")        
        rating_span = row.select_one("span.minirating")
        cover_img = row.select_one("img.bookCover")

        # Solo se procesan los que son libros (tiene título y autor)
        if not title_tag or not author_tag:
            continue

        title = title_tag.get_text(strip=True)
        author = author_tag.get_text(strip=True)
        book_url = BASE_URL + title_tag.get("href", "")
        cover_url = cover_img["src"] if cover_img else None
        rating, ratings_count = parse_rating_block(rating_span.get_text(strip=True) if rating_span else "")

        if fetch_isbn:
            print(f"  · Obteniendo ISBN/ASIN para: {title!r}")
            isbn10, isbn13, asin = fetch_isbn_from_book_page(book_url, user_agent)
            print(f"    ISBN10: {isbn10} - ISBN13: {isbn13} - ASIN: {asin}")
            time.sleep(0.5)     # pausa entre fichas
        else:
            isbn10, isbn13, asin = None, None, None

        # DESCARGAR PORTADA
        local_cover_path = None
        if cover_url:
            filename = f"{title[:100]}.jpg"
            safe_title = sanitize_filename(filename)
            local_cover_path = os.path.join("covers", safe_title)
            download_image(cover_url, local_cover_path)

        books.append(
            {
                "title": title,
                "author": author,
                "rating": rating,
                "ratings_count": ratings_count,
                "book_url": book_url,
                "cover_url": cover_url,                
                "isbn10": isbn10,
                "isbn13": isbn13,
                "asin": asin,
                "cover_local_path": local_cover_path,
            }
        )

    return books

# ------------------------------------------------------------
# DESCARGAR PORTADAS
# ------------------------------------------------------------

def download_image(url: str, dest_path: str):
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            f.write(r.content)
    except Exception as e:
        print(f"[ERROR] No se pudo descargar imagen {url}: {e}")

# Sustituye caracteres prohibidos en nomobres de fichero por "_" 
def sanitize_filename(name: str) -> str:    
    return re.sub(r'[ \\/*?:"<>|]', "_", name)

# ------------------------------------------------------------
#  ELEGIR BACKEND
# ------------------------------------------------------------

def scrape_goodreads_search(query: str, max_books: int = defaultMax_books, user_agent: Optional[str] = None, backend: Backend = Backend.REQUESTS, fetch_isbn: bool = False,) -> List[Dict]:
    if backend == Backend.REQUESTS:
        return scrape_goodreads_requests(query, max_books, user_agent, fetch_isbn)
    elif backend == Backend.PLAYWRIGHT:
        return scrape_goodreads_playwright(query, max_books, user_agent, fetch_isbn)
    else:
        raise ValueError(f"Backend no soportado: {backend}")
    
# ------------------------------------------------------------------------------
#  BACKEND Requests + BeautifulSoup
# ------------------------------------------------------------------------------

def scrape_goodreads_requests(query: str, max_books: int = defaultMax_books, user_agent: Optional[str] = None, fetch_isbn: bool = False,) -> List[Dict]:
    headers = {"User-Agent": user_agent or "Mozilla/5.0"}
    books: List[Dict] = []
    page = 1

    while len(books) < max_books:
        remaining = max_books - len(books)  # 👈 lo que falta por completar

        query_param = query.replace(" ", "+")
        url = f"{BASE_URL}/search?q={query_param}&page={page}"

        print(f"[Requests] Llamando a Goodreads (page={page}, remaining={remaining}): {url}")

        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"[ERROR HTTP] Goodreads devolvió {resp.status_code}: {e}")
            break
        except requests.exceptions.RequestException as e:
            print(f"[ERROR RED] No se pudo conectar a Goodreads: {e}")
            break

        # le pasamos solo lo que falta
        page_books = parse_books_from_html( resp.text, remaining, user_agent, fetch_isbn,)

        if not page_books:
            print("[INFO] No se encontraron más libros en esta página, fin de paginación.")
            break

        for b in page_books:
            if len(books) >= max_books:
                break
            books.append(b)

        page += 1

        time.sleep(0.5)     # Pequeña pausa para no saturar Goodreads

    return books



# ------------------------------------------------------------
#  BACKEND Playwright + BeautifulSoup
# ------------------------------------------------------------

def scrape_goodreads_playwright(query: str, max_books: int = defaultMax_books, user_agent: Optional[str] = None, fetch_isbn: bool = False,) -> List[Dict]:    

    return asyncio.run(
        _scrape_goodreads_playwright_async(query=query, max_books=max_books, user_agent=user_agent, fetch_isbn=fetch_isbn,)
    )

async def _scrape_goodreads_playwright_async(query: str, max_books: int = defaultMax_books, user_agent: Optional[str] = None, fetch_isbn: bool = False,) -> List[Dict]:

    query_param = query.replace(" ", "+")
    url = f"{BASE_URL}/search?q={query_param}"

    print(f"[Playwright] Llamando a Goodreads: {url}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(user_agent=user_agent or "Mozilla/5.0")
        await page.goto(url, wait_until="networkidle")
        html = await page.content()
        await browser.close()

    return parse_books_from_html(html, max_books, user_agent, fetch_isbn)

# ------------------------------------------------------------
#  MAIN
# ------------------------------------------------------------

def main():
    load_dotenv()
    query = os.getenv("GOODREADS_SEARCH_QUERY", defaultQuery)
    max_books = int(os.getenv("GOODREADS_MAX_BOOKS", defaultMax_books))
    user_agent = os.getenv("GOODREADS_USER_AGENT", defaultUser_agent)
    backend_str = os.getenv("GOODREADS_BACKEND", "requests").lower()
    fetch_isbn_flag = os.getenv("GOODREADS_FETCH_ISBN", "true").lower() == "true"

    if backend_str == "playwright":
        backend = Backend.PLAYWRIGHT        
    else:
        backend = Backend.REQUESTS    

    print(f"Backend: {backend.value}")
    print(f"Fetch_isbn: {fetch_isbn_flag}")
    print(f"Buscando en Goodreads: '{query}' (máx. {max_books} libros)")

    books = scrape_goodreads_search(query=query, max_books=max_books, user_agent=user_agent, backend=backend, fetch_isbn=fetch_isbn_flag,)

    os.makedirs(LANDING_DIR, exist_ok=True)
    output_path = os.path.join(LANDING_DIR, "goodreads_books.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(books, f, ensure_ascii=False, indent=2)

    print(f"\nGuardados {len(books)} libros en {output_path}")


if __name__ == "__main__":
    main()
