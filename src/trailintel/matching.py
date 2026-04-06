from __future__ import annotations

import re
import unicodedata
from difflib import SequenceMatcher


def deaccent_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_search_text(text: str) -> str:
    return " ".join(deaccent_text(text).split())


def search_name_variants(text: str) -> list[str]:
    base = " ".join(text.split())
    if not base:
        return []

    variants = [base]
    deaccented = normalize_search_text(base)
    if deaccented and deaccented.casefold() != base.casefold():
        variants.append(deaccented)
    return variants


def canonical_name(text: str) -> str:
    # Normalize accents and punctuation to improve exact matching stability.
    no_marks = deaccent_text(text)
    alnum = re.sub(r"[^A-Za-z0-9\s]", " ", no_marks)
    return " ".join(alnum.lower().split())


def name_tokens(text: str) -> list[str]:
    canonical = canonical_name(text)
    if not canonical:
        return []
    return canonical.split()


def match_score(query: str, candidate: str) -> float:
    q_tokens = name_tokens(query)
    c_tokens = name_tokens(candidate)
    if not q_tokens or not c_tokens:
        return 0.0
    q = " ".join(q_tokens)
    c = " ".join(c_tokens)
    ratio = SequenceMatcher(a=q, b=c).ratio()
    q_set = set(q_tokens)
    c_set = set(c_tokens)
    overlap = len(q_set.intersection(c_set)) / max(len(q_set), 1)
    return (ratio * 0.7) + (overlap * 0.3)


def is_strong_person_name_match(query: str, candidate: str) -> bool:
    query_tokens = [token for token in name_tokens(query) if len(token) >= 2]
    candidate_tokens = [token for token in name_tokens(candidate) if len(token) >= 2]
    if len(query_tokens) < 2 or len(candidate_tokens) < 2:
        return False

    query_first = query_tokens[0]
    query_last = query_tokens[-1]
    candidate_first = candidate_tokens[0]
    candidate_last = candidate_tokens[-1]

    if len(query_last) >= 4 and query_last == candidate_last:
        return True

    if (
        query_first == candidate_first
        and len(query_last) >= 4
        and len(candidate_last) >= 4
        and SequenceMatcher(a=query_last, b=candidate_last).ratio() >= 0.88
    ):
        return True

    query_set = {token for token in query_tokens if len(token) >= 3}
    candidate_set = {token for token in candidate_tokens if len(token) >= 3}
    return len(query_set.intersection(candidate_set)) >= 2
