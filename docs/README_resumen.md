\newpage
# Resumen Books Pipeline (Goodreads → Google Books → Parquet)
**(README_resumen.md)**

---

## 1. Objetivo
Este proyecto implementa un pipeline completo de **Extracción → Enriquecimiento → Integración** para consolidar datos de libros a partir de:

-   **Scraping de Goodreads**
-   **Google Books API**
-   **Integración con normalización semántica y deduplicación**

El resultado final produce:

- Un **modelo canónico limpio en Parquet**
- Un archivo de **detalle por fuente**
- Un conjunto de **métricas de calidad**.

---

## 2. Tecnologías utilizadas

- Python
- Requests
- BeautifulSoup4
- LXML
- python-dotenv
- Playwright (opcional)
- pandas

---

## 3. Estructura del repositorio

```
books-pipeline/                         Proyecto completo del pipeline de libros
   │
   ├─ landing/                          Datos “crudos” recién extraídos de las fuentes
   │  ├─ goodreads_books.json           Datos sin procesar obtenidos de Goodreads
   │  └─ googlebooks_books.csv          Datos sin procesar obtenidos de Google Books
   │
   ├─ staging/                          Datos transformados parcialmente (limpios pero no finales)
   │  └─ book_staging.parquet           Dataset unificado y normalizado previo al estándar
   │
   ├─ standard/                         Datos finales listos para análisis
   │  ├─ dim_book.parquet               Dimensión de libros estandarizada
   │  └─ book_source_detail.parquet     Detalles de origen por libro (trazabilidad)
   │
   ├─ covers/                           Carpeta para almacenar portadas descargadas
   │
   ├─ docs/                             Documentación del pipeline
   │  ├─ schema.md                      Descripción de los esquemas de datos
   │  └─ quality_metrics.json           Métricas de calidad del pipeline/dataset
   │
   ├─ src/                              Código fuente del pipeline
   │   ├─ scrape_goodreads.py           Script para extraer datos de Goodreads
   │   ├─ enrich_googlebooks.py         Script para enriquecer datos usando Google Books
   │   └─ integrate_pipeline.py         Script principal que orquesta todo el pipeline
   │
   ├─ README.md                         Descripción general del proyecto, cómo usarlo
   ├─ requirements.txt                  Lista de dependencias de Python necesarias
   ├─ .env.example                      Fichero de ejemplo de variables de entorno (API keys, ...)
   └─ .env                              Variables de entorno (API keys, ...)
```

---

## 4. Flujo del pipeline

1. **Scraping (Goodreads → JSON)**  
   Script: `src/scrape_goodreads.py`
    - Lanza una búsqueda en Goodreads (`GOODREADS_SEARCH_QUERY`).
    - Uso de User-Agent personalizado y pausas opcionales para un scraping responsable.
    - Extrae: título, autor, rating, número de valoraciones y URL del libro.
    - Genera: `landing/goodreads_books.json`.

2. **Enriquecimiento (Google Books → CSV)**  
   Script: `src/enrich_googlebooks.py`
   - Para cada libro de Goodreads, llama a Google Books API.
   - Prioriza `isbn13` > `isbn10` > combinación `título + autor`.
   - Extrae: título, autores, editorial, fecha de publicación, idioma, categorías, ISBNs, precio/moneda.
   - Genera: `landing/googlebooks_books.csv` (`;` y UTF-8).

   - Extrae título, autores, editorial, fecha, idioma, categorías, precios, ISBNs.  

3. **Integración y estandarización (JSON + CSV → Parquet)**  
   Script: `src/integrate_pipeline.py`
    - Lee los ficheros de `landing/`.
    - *Los ficheros de `landing/` no se modifican; `staging/` y `standard/` contienen las salidas del pipeline.*
    - Normaliza fechas (ISO-8601), idioma (BCP-47) y moneda (ISO-4217).
    - Genera un `book_id` canónico (ISBN13 o hash).
    - Deduplica registros aplicando reglas de supervivencia (ISBN13 > precio > fuente > longitud).
    - Salidas:
        - `standard/dim_book.parquet` → tabla canónica de libros.
        - `standard/book_source_detail.parquet` → detalle por fuente y registro.
        - `docs/quality_metrics.json` → métricas de calidad (nulos, duplicados, formatos, etc.).
        - `docs/schema.md` → descripción de los esquemas de datos
 
---

## 5. Instalación y configuración

### 5.1. Instalar dependencias

``` bash
    pip install -r requirements.txt
    playwright install
```

### 5.2. Crear archivo `.env`

Puedes copiarlo desde `.env.ejemplo`.
Ejemplo:

        GOOGLE_BOOKS_API_KEY=TU_API_KEY_AQUI
        GOODREADS_BACKEND=requests/playwright	
        GOODREADS_FETCH_ISBN=true/false
        GOODREADS_SEARCH_QUERY=big data
        GOODREADS_MAX_BOOKS=25
        GOODREADS_USER_AGENT=Mozilla/5.0 (Windows NT 10.0; Win64; x64)

---

### 5.3. Ejecución

1. Scraping Goodreads → JSON

```bash
        python src/scrape_goodreads.py
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Salidas generadas:

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- `landing/goodreads_books.json`

2. Enriquecimiento Google Books API → CSV

```bash
        python src/enrich_googlebooks.py
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Salidas generadas:

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- `landing/googlebooks_books.csv`

3. Integración y estandarización → Parquet

```bash
        python src/integrate_pipeline.py
```
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Salidas generadas:

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- `standard/dim_book.parquet`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- `standard/book_source_detail.parquet`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- `staging/books_staging.parquet`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- `docs/quality_metrics.json`

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- `docs/schema.md`

---

## 6. Publicación web (GitHub)
- https://github.com/IA-CSG/books-pipeline