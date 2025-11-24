# src/enrich_googlebooks.py

import csv
import json
import os
import time
from typing import Dict, Any, List, Optional

import requests
from dotenv import load_dotenv

GOOGLE_BOOKS_API_URL = "https://www.googleapis.com/books/v1/volumes"

# Utilidades de ruta base del proyecto (subimos desde src/ a la raíz)
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
LANDING_DIR = os.path.join(BASE_DIR, "landing")

def build_query(book: Dict[str, Any]) -> str:
    # Construye la query para Google Books.
    # Prioriza ISBN13 > ISBN10, y si no hay, usa título + autor.

    # ISBN
    isbn13 = book.get("isbn13")
    isbn10 = book.get("isbn10")
    asin = book.get("asin")

    if isbn13:
        return f"isbn:{isbn13}"
    if isbn10:
        return f"isbn:{isbn10}"
    if asin:
        return f"isbn:{asin}"

    # Título + autor
    title = book.get("title", "")
    author = book.get("author", "")
    q_parts = []
    if title:
        q_parts.append(f'intitle:"{title}"')
    if author:
        q_parts.append(f'inauthor:"{author}"')

    return "+".join(q_parts) if q_parts else ""


def call_google_books_api(query: str, api_key: Optional[str] = None, max_retries: int = 3, backoff_seconds: float = 2.0,) -> Optional[Dict[str, Any]]:   
    
    # Llama a Google Books con reintentos simples en caso de 503/429.
    # Devuelve el primer item o None si no hay resultados / fallo.

    if not query:
        return None

    params = {"q": query, "maxResults": 1}
    if api_key:
        params["key"] = api_key

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(GOOGLE_BOOKS_API_URL, params=params, timeout=15)
        except requests.exceptions.RequestException as e:
            print(f"[ERROR RED] Fallo de red llamando a Google Books: {e}")
            return None

        status = resp.status_code

        # OK
        if status == 200:
            data = resp.json()
            if data.get("totalItems", 0) == 0 or not data.get("items"):
                return None
            return data["items"][0]

        # Errores temporales: reintentamos
        if status in (503, 429) and attempt < max_retries:
            print(f"[AVISO] Google Books devolvió {status}. Reintentando ({attempt}/{max_retries})...")
            time.sleep(backoff_seconds * attempt)
            continue

        # Otros errores HTTP
        print(f"[ERROR HTTP] Google Books devolvió {status}: {resp.text[:200]}...")
        return None

    # Si sale del bucle sin éxito
    return None


def extract_book_fields(item: Dict[str, Any], original_book: Dict[str, Any],) -> Dict[str, Any]:
    volume_info = item.get("volumeInfo", {})
    sale_info = item.get("saleInfo", {})

    # ISBNs
    isbn10 = None
    isbn13 = None
    asin = None
    for iden in volume_info.get("industryIdentifiers", []):
        if iden.get("type") == "ISBN_10":
            isbn10 = iden.get("identifier")
        elif iden.get("type") == "ISBN_13":
            isbn13 = iden.get("identifier")
        elif iden.get("type") == "ASIN":
            asin = iden.get("identifier")

    # Precio
    price_amount = None
    price_currency = None
    list_price = sale_info.get("listPrice") or sale_info.get("retailPrice")
    if list_price:
        price_amount = list_price.get("amount")
        price_currency = list_price.get("currencyCode")

    authors = volume_info.get("authors", []) or []
    categories = volume_info.get("categories", []) or []

    return {
        "gb_id": item.get("id"),
        "original_title": original_book.get("title"),
        "original_author": original_book.get("author"),
        "title": volume_info.get("title"),
        "subtitle": volume_info.get("subtitle"),
        "authors": "|".join(authors) if authors else None,
        "publisher": volume_info.get("publisher"),
        "pub_date": volume_info.get("publishedDate"),
        "language": volume_info.get("language"),
        "categories": "|".join(categories) if categories else None,
        "isbn13": isbn13,
        "isbn10": isbn10,
        "asin": asin,
        "price_amount": price_amount,
        "price_currency": price_currency,
    }


def main():
    load_dotenv()
    api_key = os.getenv("GOOGLE_BOOKS_API_KEY")

    if not api_key:
        print("[AVISO] GOOGLE_BOOKS_API_KEY no encontrado en el .env")

    # Leer JSON de Goodreads desde landing/ relativo a la raíz del proyecto
    input_path = os.path.join(LANDING_DIR, "goodreads_books.json")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"No se encuentra el fichero de entrada: {input_path}")

    with open(input_path, "r", encoding="utf-8") as f:
        goodreads_books = json.load(f)

    rows: List[Dict[str, Any]] = []

    for book in goodreads_books:
        title = book.get("title")
        author = book.get("author")
        query = build_query(book)

        print(f"\nProcesando: '{title}' de '{author}'")
        print(f"  → Query Google Books: {query!r}")

        try:
            item = call_google_books_api(query, api_key)            
        except Exception as e:
            # catch-all por si algo raro se escapa
            print(f"[ERROR] Excepción inesperada para '{title}': {e}")
            continue
        
        time.sleep(0.3)  # pequeña pausa de buena práctica

        if not item:
            print(f"  → Sin resultados para '{title}'")
            continue

        enriched = extract_book_fields(item, book)
        rows.append(enriched)
        print("  → Enriquecido correctamente")

    # Guardar CSV en landing/
    os.makedirs(LANDING_DIR, exist_ok=True)
    output_path = os.path.join(LANDING_DIR, "googlebooks_books.csv")

    fieldnames = [
        "gb_id",
        "original_title",
        "original_author",
        "title",
        "subtitle",
        "authors",
        "publisher",
        "pub_date",
        "language",
        "categories",
        "isbn13",
        "isbn10",
        "asin",
        "price_amount",
        "price_currency",
    ]

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"\nGuardados {len(rows)} registros enriquecidos en {output_path}")

if __name__ == "__main__":
    main()
