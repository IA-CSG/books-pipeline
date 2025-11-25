# Esquema de tablas

## dim_book

| columna | tipo | nullability | ejemplo |
|---------|------|-------------|---------|
| book_id | object | NOT NULL | 037424ccc6228d8d2d682491a4f1d39eaec6605b |
| titulo | object | NOT NULL | Big Data in Practice: How 45 Successf... |
| titulo_normalizado | object | NOT NULL | big data in practice: how 45 successf... |
| autor_principal | object | NOT NULL | Bernard Marr |
| autores | object | NOT NULL | [] |
| editorial | object | NULL (37.5%) | Dey Street Books |
| anio_publicacion | float64 | NULL (29.2%) | 2017.0 |
| fecha_publicacion | object | NULL (29.2%) | 2017-05-09 |
| idioma | object | NULL (29.2%) | en |
| isbn10 | object | NULL (12.5%) | 0062390856 |
| isbn13 | object | NULL (12.5%) | 9780062390851 |
| asin | string | NOT NULL | B01DCOYDUS |
| paginas | float64 | NULL (100.0%) |  |
| formato | object | NULL (100.0%) |  |
| categorias | object | NOT NULL | [] |
| precio | float64 | NULL (100.0%) |  |
| moneda | object | NULL (100.0%) |  |
| fuente_ganadora | object | NOT NULL | goodreads |
| ts_ultima_act | object | NOT NULL | 2025-11-24T16:12:07.921555+00:00 |

## book_source_detail

| columna | tipo | nullability | ejemplo |
|---------|------|-------------|---------|
| titulo | object | NOT NULL | Big Data in Practice: How 45 Successf... |
| autor_principal | object | NOT NULL | Bernard Marr |
| rating | float64 | NULL (41.9%) | 3.41 |
| ratings_count | float64 | NULL (41.9%) | 295.0 |
| book_url | object | NULL (41.9%) | https://www.goodreads.com/book/show/2... |
| isbn10 | object | NULL (7.0%) | 0062390856 |
| isbn13 | object | NULL (7.0%) | 9780062390851 |
| asin | string | NULL (41.9%) | B01DCOYDUS |
| source_name | object | NOT NULL | goodreads |
| source_file | object | NOT NULL | goodreads_books.json |
| row_number | int64 | NOT NULL | 1 |
| autores | object | NOT NULL |  |
| editorial | object | NULL (62.8%) | Dey Street Books |
| fecha_publicacion_raw | object | NULL (58.1%) | 2017-05-09 |
| idioma_raw | object | NULL (58.1%) | en |
| categorias | object | NOT NULL |  |
| precio | float64 | NULL (100.0%) |  |
| moneda | object | NULL (100.0%) |  |
| titulo_normalizado | object | NOT NULL | big data in practice: how 45 successf... |
| idioma | object | NULL (58.1%) | en |
| fecha_publicacion | object | NULL (58.1%) | 2017-05-09 |
| autores_list | object | NOT NULL | [] |
| categorias_list | object | NOT NULL | [] |
| anio_publicacion | float64 | NULL (58.1%) | 2017.0 |
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
| ts_ingesta | object | NOT NULL | 2025-11-24T16:12:07.924512+00:00 |

