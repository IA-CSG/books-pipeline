# Esquema de tablas

## dim_book

| columna | tipo | nullability | ejemplo |
|---------|------|-------------|---------|
| book_id | object | NOT NULL | 037424ccc6228d8d2d682491a4f1d39eaec6605b |
| titulo | object | NOT NULL | Big Data in Practice: How 45 Successf... |
| titulo_normalizado | object | NOT NULL | big data in practice: how 45 successf... |
| autor_principal | object | NOT NULL | Bernard Marr |
| autores | object | NOT NULL | [] |
| editorial | object | NULL (40.0%) | Dey Street Books |
| anio_publicacion | float64 | NULL (32.0%) | 2017.0 |
| fecha_publicacion | object | NULL (32.0%) | 2017-05-09 |
| idioma | object | NULL (32.0%) | en |
| isbn10 | object | NULL (12.0%) | 0062390856 |
| isbn13 | object | NULL (12.0%) | 9780062390851 |
| asin | string | NOT NULL | B01DCOYDUS |
| paginas | float64 | NULL (100.0%) |  |
| formato | object | NULL (100.0%) |  |
| categorias | object | NOT NULL | [] |
| precio | float64 | NULL (100.0%) |  |
| moneda | object | NULL (100.0%) |  |
| fuente_ganadora | object | NOT NULL | goodreads |
| ts_ultima_act | object | NOT NULL | 2025-11-24T15:01:03.101196+00:00 |

## book_source_detail

| columna | tipo | nullability | ejemplo |
|---------|------|-------------|---------|
| titulo | object | NOT NULL | Big Data in Practice: How 45 Successf... |
| autor_principal | object | NOT NULL | Bernard Marr |
| rating | float64 | NULL (40.5%) | 3.41 |
| ratings_count | float64 | NULL (40.5%) | 295.0 |
| book_url | object | NULL (40.5%) | https://www.goodreads.com/book/show/2... |
| isbn10 | object | NULL (7.1%) | 0062390856 |
| isbn13 | object | NULL (7.1%) | 9780062390851 |
| asin | string | NULL (40.5%) | B01DCOYDUS |
| source_name | object | NOT NULL | goodreads |
| source_file | object | NOT NULL | goodreads_books.json |
| row_number | int64 | NOT NULL | 1 |
| autores | object | NOT NULL |  |
| editorial | object | NULL (64.3%) | Dey Street Books |
| fecha_publicacion_raw | object | NULL (59.5%) | 2017-05-09 |
| idioma_raw | object | NULL (59.5%) | en |
| categorias | object | NOT NULL |  |
| precio | float64 | NULL (100.0%) |  |
| moneda | object | NULL (100.0%) |  |
| titulo_normalizado | object | NOT NULL | big data in practice: how 45 successf... |
| idioma | object | NULL (59.5%) | en |
| fecha_publicacion | object | NULL (59.5%) | 2017-05-09 |
| autores_list | object | NOT NULL | [] |
| categorias_list | object | NOT NULL | [] |
| anio_publicacion | float64 | NULL (59.5%) | 2017.0 |
| longitud_titulo | int64 | NOT NULL | 106 |
| autor_normalizado | object | NOT NULL | bernard marr |
| editorial_normalizada | object | NOT NULL |  |
| book_id | object | NOT NULL | 037424ccc6228d8d2d682491a4f1d39eaec6605b |
| has_isbn13 | bool | NOT NULL | False |
| has_precio | bool | NOT NULL | False |
| prioridad_fuente | int64 | NOT NULL | 2 |
| error_codes | object | NOT NULL | [] |
| has_error | bool | NOT NULL | False |
| source_id | int64 | NOT NULL | 1 |
| book_id_candidato | object | NOT NULL | 037424ccc6228d8d2d682491a4f1d39eaec6605b |
| ts_ingesta | object | NOT NULL | 2025-11-24T15:01:03.102473+00:00 |

