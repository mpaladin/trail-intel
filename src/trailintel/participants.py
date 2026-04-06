from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

NAME_KEYS = ("name", "full_name", "fullname", "athlete", "runner", "participant")
STOPWORDS = {
    "rank",
    "bib",
    "time",
    "gender",
    "age",
    "country",
    "distance",
    "results",
    "result",
    "runner",
    "runners",
    "participant",
    "participants",
    "athlete",
    "athletes",
    "male",
    "female",
    "total",
    "finishers",
}
NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z'\-\.]{1,}(?:\s+[A-Za-z][A-Za-z'\-\.]{1,})+$")
YAKA_HOST_SUFFIX = "yaka-inscription.com"
YAKA_FRONT_API_BASE = "https://front-api.yaka-inscription.com"
YAKA_HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "x-context": "default",
    # The front API returns 403 to default python-requests user agents.
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}
NJUKO_HOST_SUFFIX = "njuko.com"
NJUKO_FRONT_API_BASE = "https://front-api.njuko.com"
NJUKO_HEADERS = {
    "accept": "application/json,text/plain,*/*",
    "x-context": "default",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}
RACERESULT_HOST_SUFFIX = "raceresult.com"
RACERESULT_DATA_PATH_MARKER = "/rrpublish/data/list"
RACERESULT_PARTICIPANTS_LIST_MARKER = "/participants/list"
RACERESULT_HEADERS = {
    "accept": "application/json,text/plain,*/*",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}
WEDOSPORT_HOST_SUFFIX = "wedosport.net"
WEDOSPORT_LIST_PATH_MARKER = "/lista-iscritti/"
GRANDRAID_HOST_SUFFIX = "grandraid-reunion.com"
GRANDRAID_LIST_PATH_MARKER = "/listes-des-inscrits/"
GENERIC_BROWSER_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}


def normalize_name(name: str) -> str:
    cleaned = re.sub(r"\s+", " ", name.strip())
    cleaned = cleaned.strip(",.;:()[]{}")
    return cleaned


def looks_like_name(text: str) -> bool:
    candidate = normalize_name(text)
    if not candidate or len(candidate) > 60:
        return False
    lower = candidate.lower()
    if any(word in STOPWORDS for word in lower.split()):
        return False
    return bool(NAME_PATTERN.match(candidate))


def dedupe_names(names: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw in names:
        name = normalize_name(raw)
        key = name.casefold()
        if not name or key in seen:
            continue
        seen.add(key)
        result.append(name)
    return result


def _from_csv_text(text: str) -> list[str]:
    rows = list(csv.DictReader(text.splitlines()))
    if rows and rows[0]:
        keys = tuple(rows[0].keys())
        target = next((k for k in keys if k and k.strip().lower() in NAME_KEYS), None)
        if target:
            return dedupe_names(row.get(target, "") for row in rows)
    reader = csv.reader(text.splitlines())
    first_col = [row[0] for row in reader if row]
    return dedupe_names(first_col)


def _extract_names_from_json(value: object) -> list[str]:
    names: list[str] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                names.append(item)
            elif isinstance(item, dict):
                for key in NAME_KEYS:
                    if key in item and isinstance(item[key], str):
                        names.append(item[key])
                        break
    elif isinstance(value, dict):
        for key in ("participants", "runners", "athletes"):
            if key in value:
                names.extend(_extract_names_from_json(value[key]))
    return dedupe_names(names)


def _extract_names_from_html(html: str, selector: str | None = None) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    if selector:
        scoped = [element.get_text(" ", strip=True) for element in soup.select(selector)]
        names = [text for text in scoped if looks_like_name(text)]
        return dedupe_names(names)

    candidates: list[str] = []

    for row in soup.select("tr"):
        texts = [cell.get_text(" ", strip=True) for cell in row.select("th,td")]
        if not texts:
            continue
        for text in texts:
            if looks_like_name(text):
                candidates.append(text)
                break

    for node in soup.select("li, .participant, .runner, .athlete, .name"):
        text = node.get_text(" ", strip=True)
        if looks_like_name(text):
            candidates.append(text)

    # JSON-LD or inline JSON fragments with athlete names.
    for script in soup.select("script[type='application/ld+json']"):
        try:
            parsed = json.loads(script.get_text(strip=True))
        except json.JSONDecodeError:
            continue
        candidates.extend(_extract_names_from_json(parsed))

    if not candidates:
        text_nodes = [s.strip() for s in soup.stripped_strings]
        candidates.extend(text for text in text_nodes if looks_like_name(text))

    return dedupe_names(candidates)


def _competition_matches_name(competition: dict[str, object], target: str) -> bool:
    normalized_target = target.strip().casefold()
    if not normalized_target:
        return False

    competition_id = competition.get("_id")
    if isinstance(competition_id, str) and competition_id.casefold() == normalized_target:
        return True

    names = competition.get("name")
    if isinstance(names, list):
        for item in names:
            if not isinstance(item, dict):
                continue
            label = item.get("translation")
            if isinstance(label, str) and normalized_target in label.casefold():
                return True
    return False


def _looks_like_person_name_permissive(text: str) -> bool:
    candidate = normalize_name(text)
    if not candidate or len(candidate) > 80:
        return False

    tokens = [token for token in candidate.split() if any(ch.isalpha() for ch in token)]
    if len(tokens) < 2:
        return False
    if any(token.casefold() in STOPWORDS for token in tokens):
        return False
    return True


def _normalize_raceresult_name(raw: str) -> str:
    text = normalize_name(raw)
    if not text:
        return ""

    if "," in text:
        last_name, _, first_name = text.partition(",")
        last_name = normalize_name(last_name)
        first_name = normalize_name(first_name)
        swapped = normalize_name(f"{first_name} {last_name}")
        if swapped:
            return swapped
    return text


def _extract_raceresult_name_from_value(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = normalize_name(value)
    if not text:
        return None
    if text.startswith("[img:"):
        return None

    candidate = _normalize_raceresult_name(text)
    if not candidate or not _looks_like_person_name_permissive(candidate):
        return None
    return candidate


def _iter_raceresult_rows(value: object) -> Iterable[list[object]]:
    if isinstance(value, list):
        if value and all(not isinstance(item, (list, dict)) for item in value):
            yield value
            return
        for item in value:
            yield from _iter_raceresult_rows(item)
    elif isinstance(value, dict):
        for nested in value.values():
            yield from _iter_raceresult_rows(nested)


def _best_raceresult_name_from_row(
    row: list[object],
    *,
    preferred_index: int | None,
) -> str | None:
    if preferred_index is not None and preferred_index < len(row):
        candidate = _extract_raceresult_name_from_value(row[preferred_index])
        if candidate:
            return candidate

    comma_candidates: list[str] = []
    permissive_candidates: list[str] = []
    strict_candidates: list[str] = []

    for cell in row:
        candidate = _extract_raceresult_name_from_value(cell)
        if not candidate:
            continue
        if "," in str(cell):
            comma_candidates.append(candidate)
        if looks_like_name(candidate):
            strict_candidates.append(candidate)
        permissive_candidates.append(candidate)

    if comma_candidates:
        return comma_candidates[0]
    if strict_candidates:
        return strict_candidates[0]
    if permissive_candidates:
        return permissive_candidates[0]
    return None


def _parse_raceresult_payload(payload: object) -> list[str]:
    if not isinstance(payload, dict):
        return []

    error_message = payload.get("error")
    if isinstance(error_message, str) and error_message.strip():
        raise ValueError(f"RaceResult error: {error_message.strip()}")

    data_fields = payload.get("DataFields")
    preferred_name_index: int | None = None
    if isinstance(data_fields, list):
        for idx, field in enumerate(data_fields):
            if not isinstance(field, str):
                continue
            upper = field.upper()
            if "LFNAME" in upper or "MOSTRANOME" in upper:
                preferred_name_index = idx
                break

    names: list[str] = []
    for row in _iter_raceresult_rows(payload.get("data")):
        candidate = _best_raceresult_name_from_row(row, preferred_index=preferred_name_index)
        if candidate:
            names.append(candidate)
    return dedupe_names(names)


def _raceresult_is_list_path(path: str) -> bool:
    lower = path.casefold()
    return RACERESULT_DATA_PATH_MARKER in lower or RACERESULT_PARTICIPANTS_LIST_MARKER in lower


def _raceresult_event_id_from_path(path: str) -> str | None:
    lower = path.casefold()
    if _raceresult_is_list_path(lower):
        return None
    parts = [part for part in path.split("/") if part]
    if len(parts) >= 2 and parts[1].casefold() == "participants":
        return parts[0]
    return None


def _fetch_raceresult_json(
    url: str,
    *,
    timeout: int,
    params: dict[str, object] | None = None,
) -> object:
    response = requests.get(url, headers=RACERESULT_HEADERS, timeout=timeout, params=params)
    response.raise_for_status()
    return response.json()


def _select_raceresult_list(config: dict[str, object]) -> dict[str, object] | None:
    tab_config = config.get("TabConfig")
    if not isinstance(tab_config, dict):
        return None
    lists = tab_config.get("Lists")
    if not isinstance(lists, list):
        return None
    for item in lists:
        if not isinstance(item, dict):
            continue
        mode = str(item.get("Mode") or "").strip()
        fmt = str(item.get("Format") or "")
        if mode == "" and ("V" in fmt or "P" in fmt):
            return item
    return None


def _raceresult_group_filter_param(
    group_filters: object,
    competition_name: str,
) -> str | None:
    if not isinstance(group_filters, list):
        return None
    target = " ".join(competition_name.split()).casefold()
    if not target:
        return None

    candidates: list[tuple[int, int, str]] = []
    for idx, group in enumerate(group_filters):
        if not isinstance(group, dict):
            continue
        values = group.get("Values")
        if not isinstance(values, list):
            continue
        type_value = group.get("Type")
        priority = 0 if type_value == 1 else 1
        for value in values:
            if not isinstance(value, str) or not value.strip():
                continue
            normalized = " ".join(value.split()).casefold()
            if target in normalized or normalized in target:
                candidates.append((priority, idx, value))
                break

    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    _, match_index, match_value = candidates[0]
    filter_values = ["<Ignore>" for _ in group_filters]
    if match_index < len(filter_values):
        filter_values[match_index] = match_value
    return "\f".join(filter_values)


def _fetch_raceresult_from_base(
    url: str,
    *,
    timeout: int,
    competition_name: str | None,
    event_id: str,
) -> list[str]:
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    base_origin = f"{scheme}://{parsed.netloc}"
    config_url = f"{base_origin}/{event_id}/participants/config"
    config_payload = _fetch_raceresult_json(config_url, timeout=timeout, params={"lang": "en"})
    if not isinstance(config_payload, dict):
        raise ValueError("RaceResult config response was invalid.")

    list_entry = _select_raceresult_list(config_payload)
    if not list_entry:
        raise ValueError("RaceResult config did not contain a published list.")

    key = str(config_payload.get("key") or "").strip()
    list_name = str(list_entry.get("Name") or "").strip()
    if not key or not list_name:
        raise ValueError("RaceResult config missing key or list name.")

    server = config_payload.get("server")
    if isinstance(server, str) and server.strip():
        server_base = server.strip()
        if not server_base.startswith("http"):
            server_base = f"https://{server_base}"
    else:
        server_base = base_origin
    server_base = server_base.rstrip("/")

    contest = str(list_entry.get("Contest") or "0")
    list_url = f"{server_base}/{event_id}/participants/list"
    params: dict[str, object] = {
        "key": key,
        "listname": list_name,
        "page": "participants",
        "contest": contest,
        "r": "all",
        "l": "0",
        "openedGroups": "{}",
        "term": "",
    }
    payload = _fetch_raceresult_json(list_url, timeout=timeout, params=params)
    if competition_name:
        filter_param = _raceresult_group_filter_param(
            payload.get("groupFilters") if isinstance(payload, dict) else None,
            competition_name,
        )
        if filter_param:
            params_with_filter = dict(params)
            params_with_filter["f"] = filter_param
            payload = _fetch_raceresult_json(list_url, timeout=timeout, params=params_with_filter)

    return _parse_raceresult_payload(payload)

def _fetch_raceresult_participants(
    url: str,
    *,
    timeout: int,
    competition_name: str | None,
) -> list[str] | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host.endswith(RACERESULT_HOST_SUFFIX):
        return None
    if _raceresult_is_list_path(parsed.path):
        payload = _fetch_raceresult_json(url, timeout=timeout)
        return _parse_raceresult_payload(payload)

    event_id = _raceresult_event_id_from_path(parsed.path)
    if event_id:
        return _fetch_raceresult_from_base(
            url,
            timeout=timeout,
            competition_name=competition_name,
            event_id=event_id,
        )

    return None


def _build_wedosport_header_map(table: Tag) -> dict[str, int]:
    header_map: dict[str, int] = {}
    for idx, header in enumerate(table.select("thead th")):
        data_name = (header.get("data-name") or "").strip().casefold()
        label = header.get_text(" ", strip=True).strip().casefold()
        for key in (data_name, label):
            if key and key not in header_map:
                header_map[key] = idx
                break
    return header_map


def _fetch_wedosport_participants(
    url: str,
    *,
    timeout: int,
    competition_name: str | None,
) -> list[str] | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host.endswith(WEDOSPORT_HOST_SUFFIX):
        return None
    if WEDOSPORT_LIST_PATH_MARKER not in parsed.path.casefold():
        return None

    response = requests.get(url, headers=GENERIC_BROWSER_HEADERS, timeout=timeout)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", id="classifica") or soup.find("table")
    if table is None:
        return []

    header_map = _build_wedosport_header_map(table)
    distance_idx = header_map.get("distanza") or header_map.get("distance")
    last_name_idx = header_map.get("cognome") or header_map.get("surname")
    first_name_idx = header_map.get("nome") or header_map.get("name")

    header_count = len(table.select("thead th"))
    if distance_idx is None and header_count:
        distance_idx = 0
    if last_name_idx is None and header_count >= 3:
        last_name_idx = 2
    if first_name_idx is None and header_count >= 4:
        first_name_idx = 3

    normalized_target = ""
    if competition_name:
        normalized_target = " ".join(competition_name.split()).casefold()

    names: list[str] = []
    for row in table.select("tbody tr"):
        cells = row.find_all("td")
        if not cells:
            continue

        if normalized_target and distance_idx is not None and distance_idx < len(cells):
            distance_text = cells[distance_idx].get_text(" ", strip=True)
            normalized_distance = " ".join(distance_text.split()).casefold()
            if normalized_target not in normalized_distance:
                continue

        if last_name_idx is None or first_name_idx is None:
            continue
        if last_name_idx >= len(cells) or first_name_idx >= len(cells):
            continue

        last_name = normalize_name(cells[last_name_idx].get_text(" ", strip=True))
        first_name = normalize_name(cells[first_name_idx].get_text(" ", strip=True))
        if not last_name or not first_name:
            continue
        full_name = normalize_name(f"{first_name} {last_name}")
        if _looks_like_person_name_permissive(full_name):
            names.append(full_name)

    return dedupe_names(names)


def _extract_grandraid_names(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    names: list[str] = []
    for item in soup.select("ol.result-list.custom-result-list li:not(.bold) span.title"):
        name = normalize_name(item.get_text(" ", strip=True))
        if name and _looks_like_person_name_permissive(name):
            names.append(name)
    return dedupe_names(names)


def _grandraid_next_page_url(html: str, current_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    next_link = soup.select_one("nav.pagination .pagination-item.next a[href]")
    if next_link is None:
        return None
    href = str(next_link.get("href") or "").strip()
    if not href:
        return None
    return urljoin(current_url, href.split("#", 1)[0])


def _fetch_grandraid_participants(
    url: str,
    *,
    timeout: int,
    competition_name: str | None,
) -> list[str] | None:
    _ = competition_name  # Race selection is encoded in the URL query (for example, type_course=GRR).

    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host.endswith(GRANDRAID_HOST_SUFFIX):
        return None
    if GRANDRAID_LIST_PATH_MARKER not in parsed.path.casefold():
        return None

    names: list[str] = []
    current_url = url
    seen_urls: set[str] = set()

    while current_url and current_url not in seen_urls:
        seen_urls.add(current_url)
        response = requests.get(current_url, headers=GENERIC_BROWSER_HEADERS, timeout=timeout)
        response.raise_for_status()
        html = response.text
        names.extend(_extract_grandraid_names(html))
        current_url = _grandraid_next_page_url(html, current_url)

    return dedupe_names(names)


def _fetch_yaka_participants(
    url: str,
    *,
    timeout: int,
    competition_name: str | None,
) -> list[str] | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host.endswith(YAKA_HOST_SUFFIX):
        return None

    slug = next((part for part in parsed.path.split("/") if part), "")
    if not slug:
        return []

    edition_url = f"{YAKA_FRONT_API_BASE}/edition/url/{slug}"
    edition_response = requests.get(edition_url, headers=YAKA_HEADERS, timeout=timeout)
    edition_response.raise_for_status()
    edition = edition_response.json()

    edition_id = edition.get("_id")
    if not isinstance(edition_id, str):
        return []

    competition_ids: set[str] = set()
    competitions = edition.get("competitions")
    if isinstance(competitions, list):
        competition_ids = {
            item.get("_id")
            for item in competitions
            if isinstance(item, dict) and isinstance(item.get("_id"), str)
        }
        competition_ids = {item for item in competition_ids if item}

        if competition_name:
            filtered = {
                item.get("_id")
                for item in competitions
                if isinstance(item, dict)
                and isinstance(item.get("_id"), str)
                and _competition_matches_name(item, competition_name)
            }
            filtered = {item for item in filtered if item}
            if not filtered:
                raise ValueError(
                    f"No Yaka competition matched '{competition_name}' for {slug}."
                )
            competition_ids = filtered

    registrations_url = f"{YAKA_FRONT_API_BASE}/registrations/{edition_id}/_search/%7B%7D"
    registrations_response = requests.get(registrations_url, headers=YAKA_HEADERS, timeout=timeout)
    registrations_response.raise_for_status()
    payload = registrations_response.json()
    if not isinstance(payload, list):
        return []

    names: list[str] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        competition_id = row.get("competition")
        if competition_ids and competition_id not in competition_ids:
            continue
        first_name = row.get("firstname")
        last_name = row.get("lastname")
        if not isinstance(first_name, str) or not isinstance(last_name, str):
            continue
        full_name = normalize_name(f"{first_name} {last_name}")
        if looks_like_name(full_name):
            names.append(full_name)
    return dedupe_names(names)


def _fetch_njuko_participants(
    url: str,
    *,
    timeout: int,
    competition_name: str | None,
) -> list[str] | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host.endswith(NJUKO_HOST_SUFFIX):
        return None

    slug = next((part for part in parsed.path.split("/") if part), "")
    if not slug:
        return []

    edition_url = f"{NJUKO_FRONT_API_BASE}/edition/url/{slug}"
    edition_response = requests.get(edition_url, headers=NJUKO_HEADERS, timeout=timeout)
    edition_response.raise_for_status()
    edition = edition_response.json()

    edition_id = edition.get("_id") if isinstance(edition, dict) else None
    if not isinstance(edition_id, str) or not edition_id:
        return []

    competition_ids: set[str] = set()
    competitions = edition.get("competitions") if isinstance(edition, dict) else None
    if isinstance(competitions, list):
        competition_ids = {
            item.get("_id")
            for item in competitions
            if isinstance(item, dict) and isinstance(item.get("_id"), str)
        }
        competition_ids = {item for item in competition_ids if item}

        if competition_name:
            filtered = {
                item.get("_id")
                for item in competitions
                if isinstance(item, dict)
                and isinstance(item.get("_id"), str)
                and _competition_matches_name(item, competition_name)
            }
            filtered = {item for item in filtered if item}
            if not filtered:
                raise ValueError(
                    f"No Njuko competition matched '{competition_name}' for {slug}."
                )
            competition_ids = filtered

    registrations_url = f"{NJUKO_FRONT_API_BASE}/registrations/{edition_id}/_search/%7B%7D"
    registrations_response = requests.get(registrations_url, headers=NJUKO_HEADERS, timeout=timeout)
    registrations_response.raise_for_status()
    payload = registrations_response.json()
    if not isinstance(payload, list):
        return []

    names: list[str] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        competition_id = row.get("competition")
        if competition_ids and competition_id not in competition_ids:
            continue
        first_name = row.get("firstname")
        last_name = row.get("lastname")
        if not isinstance(first_name, str) or not isinstance(last_name, str):
            continue
        full_name = normalize_name(f"{first_name} {last_name}")
        if full_name and any(ch.isalpha() for ch in full_name):
            names.append(full_name)
    return dedupe_names(names)


def load_participants_file(path: str | Path) -> list[str]:
    path = Path(path)
    text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _from_csv_text(text)
    if suffix == ".json":
        return _extract_names_from_json(json.loads(text))
    return dedupe_names(line for line in text.splitlines() if looks_like_name(line))


def fetch_participants_from_url(
    url: str,
    *,
    selector: str | None = None,
    competition_name: str | None = None,
    timeout: int = 20,
) -> list[str]:
    yaka_names = _fetch_yaka_participants(
        url,
        timeout=timeout,
        competition_name=competition_name,
    )
    if yaka_names is not None:
        return yaka_names

    njuko_names = _fetch_njuko_participants(
        url,
        timeout=timeout,
        competition_name=competition_name,
    )
    if njuko_names is not None:
        return njuko_names

    raceresult_names = _fetch_raceresult_participants(
        url,
        timeout=timeout,
        competition_name=competition_name,
    )
    if raceresult_names is not None:
        return raceresult_names

    wedosport_names = _fetch_wedosport_participants(
        url,
        timeout=timeout,
        competition_name=competition_name,
    )
    if wedosport_names is not None:
        return wedosport_names

    grandraid_names = _fetch_grandraid_participants(
        url,
        timeout=timeout,
        competition_name=competition_name,
    )
    if grandraid_names is not None:
        return grandraid_names

    response = requests.get(url, timeout=timeout, headers=GENERIC_BROWSER_HEADERS)
    response.raise_for_status()
    content_type = response.headers.get("content-type", "").lower()
    content = response.text

    if "application/json" in content_type or url.lower().endswith(".json"):
        return _extract_names_from_json(response.json())
    if "text/csv" in content_type or url.lower().endswith(".csv"):
        return _from_csv_text(content)
    return _extract_names_from_html(content, selector=selector)


def load_itra_overrides(path: str | Path) -> dict[str, float]:
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    if source.suffix.lower() == ".json":
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return {normalize_name(k): float(v) for k, v in parsed.items()}
        raise ValueError("ITRA overrides JSON must be an object of name->score.")

    rows = csv.DictReader(text.splitlines())
    if not rows.fieldnames:
        raise ValueError("ITRA overrides CSV must include headers.")
    name_key = next((k for k in rows.fieldnames if k and k.strip().lower() in NAME_KEYS), None)
    score_key = next(
        (
            k
            for k in rows.fieldnames
            if k and k.strip().lower() in {"itra", "itra_score", "score", "itra points"}
        ),
        None,
    )
    if not name_key or not score_key:
        raise ValueError("ITRA overrides CSV needs name and score columns.")

    mapping: dict[str, float] = {}
    for row in rows:
        name = normalize_name(row.get(name_key, ""))
        score_raw = row.get(score_key, "")
        if not name or not score_raw:
            continue
        mapping[name] = float(score_raw)
    return mapping
