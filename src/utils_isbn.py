# src/utils_isbn.py

import re
from typing import Optional


# utils_isbn.py
import re
from typing import Optional


def clean_isbn(isbn: Optional[str]) -> Optional[str]:
    if isbn is None:
        return None
    # Convertimos siempre a str
    s = str(isbn)
    # El "string dtype" de pandas puede meter <NA>, mejor tratarlo
    if s.upper() in ("<NA>", "NAN", "NONE", ""):
        return None
    s = re.sub(r"[^0-9Xx]", "", s)
    return s or None


def is_isbn10(isbn: Optional[str]) -> bool:
    isbn = clean_isbn(isbn)
    return bool(isbn) and len(isbn) == 10


def is_isbn13(isbn: Optional[str]) -> bool:
    isbn = clean_isbn(isbn)
    return bool(isbn) and len(isbn) == 13


def normalize_isbn13(isbn: Optional[str]) -> Optional[str]:
    """
    Devuelve un ISBN-13 canónico:
      - Solo dígitos
      - Longitud EXACTA 13
    Si no cumple, devuelve None.
    """
    cleaned = clean_isbn(isbn)
    if not cleaned:
        return None
    s = str(cleaned).strip()
    if len(s) == 13 and s.isdigit():
        return s
    return None


def to_isbn13(isbn10: Optional[str]) -> Optional[str]:
    """
    Conversión aproximada ISBN-10 → ISBN-13 (prefijo 978 + recalcular dígito control).
    No se valida que el ISBN10 original sea correcto.
    """
    isbn10 = clean_isbn(isbn10)
    if not isbn10 or len(isbn10) != 10:
        return None

    core = "978" + isbn10[:-1]
    total = 0
    for i, char in enumerate(core):
        n = int(char)
        total += n if i % 2 == 0 else n * 3
    check = (10 - (total % 10)) % 10
    return core + str(check)
