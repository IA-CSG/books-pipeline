import re
import requests
from bs4 import BeautifulSoup

def debug_goodreads(book_url: str, user_agent: str | None = None) -> None:
    headers = {"User-Agent": user_agent or "Mozilla/5.0"}

    print(f"[DEBUG] Fetching book page: {book_url}")
    resp = requests.get(book_url, headers=headers, timeout=15)
    resp.raise_for_status()

    html = resp.text
    soup = BeautifulSoup(html, "lxml")

    # 1) Guardar el HTML completo para verlo en el navegador
    with open("debug_goodreads.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("[DEBUG] HTML guardado en debug_goodreads.html (para abrir en el navegador).")

    # 2) Mostrar TODOS los isbn que aparezcan en el HTML crudo
    print("\n[DEBUG] Buscando todas las apariciones de \"isbn\" en el HTML crudo:")
    for match in re.finditer(r'"isbn"\s*:\s*"([^"]+)"', html):
        print("   - isbn:", match.group(1))

    print("\n[DEBUG] Buscando todas las apariciones de \"isbn13\" en el HTML crudo:")
    for match in re.finditer(r'"isbn13"\s*:\s*"([^"]+)"', html):
        print("   - isbn13:", match.group(1))

    # 3) Ver filas del #bookDataBox
    data_rows = soup.select("#bookDataBox .clearFloats")
    print(f"\n[DEBUG] Filas en #bookDataBox: {len(data_rows)}")
    for row in data_rows:
        heading = row.select_one(".infoBoxRowTitle")
        value = row.select_one(".infoBoxRowItem")
        if not heading or not value:
            continue

        label = heading.get_text(strip=True)
        text = value.get_text(" ", strip=True)
        print(f"   · label={label!r} | text={text!r}")


if __name__ == "__main__":
    url = "https://www.goodreads.com/book/show/36722689-data-science?from_search=true&from_srp=true&qid=O4wqDnw7h2&rank=9"
    debug_goodreads(url)
