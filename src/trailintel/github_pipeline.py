from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import sys
import unicodedata
from urllib.parse import urlparse

DEFAULT_SCORE_THRESHOLD = 680.0
DEFAULT_TOP = 100
DEFAULT_STRATEGY = "participant-first"
FIELD_ALIASES = {
    "race name": "race_name",
    "race url": "race_url",
    "competition name": "competition_name",
    "competition / distance": "competition_name",
    "competition": "competition_name",
    "score threshold": "score_threshold",
    "top": "top",
    "strategy": "strategy",
    "notes": "notes",
}


@dataclass(slots=True)
class ReportRequest:
    race_name: str
    race_url: str
    competition_name: str = ""
    score_threshold: float = DEFAULT_SCORE_THRESHOLD
    top: int = DEFAULT_TOP
    strategy: str = DEFAULT_STRATEGY
    notes: str = ""

    @property
    def race_slug(self) -> str:
        return build_race_slug(self)


@dataclass(slots=True)
class PublishResult:
    race_slug: str
    report_dir: str
    latest_dir: str
    report_url: str
    latest_url: str
    csv_url: str
    json_url: str


def _clean_issue_value(value: str) -> str:
    text = value.strip()
    if not text or text == "_No response_":
        return ""
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    filtered = [line for line in lines if not line.lstrip().startswith("<!--")]
    return "\n".join(filtered).strip()


def _parse_issue_sections(body: str) -> dict[str, str]:
    sections: dict[str, str] = {}
    current_label: str | None = None
    buffer: list[str] = []

    def flush() -> None:
        nonlocal buffer, current_label
        if current_label is None:
            buffer = []
            return
        raw_value = "\n".join(buffer)
        cleaned_value = _clean_issue_value(raw_value)
        if cleaned_value:
            sections[current_label] = cleaned_value
        buffer = []

    for line in body.splitlines():
        if line.startswith("### "):
            flush()
            label = line[4:].strip().casefold()
            current_label = FIELD_ALIASES.get(label)
            continue
        if current_label is not None:
            buffer.append(line)
    flush()
    return sections


def parse_issue_form(body: str) -> ReportRequest:
    sections = _parse_issue_sections(body)
    race_name = sections.get("race_name", "").strip()
    race_url = sections.get("race_url", "").strip()
    if not race_name or not race_url:
        raise ValueError("Issue form is missing Race Name or Race URL.")

    strategy = sections.get("strategy", DEFAULT_STRATEGY).strip() or DEFAULT_STRATEGY
    if strategy not in {"participant-first", "catalog-first"}:
        raise ValueError(f"Unsupported strategy: {strategy}")

    score_threshold_text = sections.get("score_threshold", str(DEFAULT_SCORE_THRESHOLD)).strip()
    top_text = sections.get("top", str(DEFAULT_TOP)).strip()
    try:
        score_threshold = float(score_threshold_text)
    except ValueError as exc:
        raise ValueError(f"Invalid score threshold: {score_threshold_text}") from exc
    try:
        top = int(top_text)
    except ValueError as exc:
        raise ValueError(f"Invalid top value: {top_text}") from exc
    if top < 1:
        raise ValueError("Top value must be at least 1.")

    return ReportRequest(
        race_name=race_name,
        race_url=race_url,
        competition_name=sections.get("competition_name", "").strip(),
        score_threshold=score_threshold,
        top=top,
        strategy=strategy,
        notes=sections.get("notes", "").strip(),
    )


def normalize_slug_text(value: str) -> str:
    ascii_text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_text.lower()).strip("-")
    return slug or "race-report"


def _normalized_words(value: str) -> set[str]:
    return {token for token in normalize_slug_text(value).split("-") if token}


def build_race_slug(request: ReportRequest) -> str:
    race_name = request.race_name.strip()
    competition_name = request.competition_name.strip()
    pieces = [race_name]
    if competition_name:
        race_words = _normalized_words(race_name)
        competition_words = _normalized_words(competition_name)
        if not competition_words or not competition_words.issubset(race_words):
            pieces.append(competition_name)
    base = " ".join(piece for piece in pieces if piece)
    if base:
        return normalize_slug_text(base)
    parsed = urlparse(request.race_url)
    fallback = f"{parsed.netloc} {parsed.path}".strip()
    return normalize_slug_text(fallback)


def build_publish_paths(request: ReportRequest, *, published_at: datetime) -> tuple[str, str]:
    slug = build_race_slug(request)
    stamp = published_at.astimezone(UTC).strftime("%Y%m%d-%H%M%S")
    return f"reports/{slug}/{stamp}", f"reports/{slug}/latest"


def build_cli_args(
    request: ReportRequest,
    *,
    site_dir: str | Path,
) -> list[str]:
    args = [
        "--race-name",
        request.race_name,
        "--race-url",
        request.race_url,
        "--strategy",
        request.strategy,
        "--score-threshold",
        str(request.score_threshold),
        "--top",
        str(request.top),
        "--site-dir",
        str(site_dir),
    ]
    if request.competition_name:
        args.extend(["--competition-name", request.competition_name])
    return args


def publish_report_bundle(
    *,
    source_dir: str | Path,
    pages_root: str | Path,
    request: ReportRequest,
    published_at: datetime | None = None,
    base_url: str | None = None,
) -> PublishResult:
    from trailintel.site import publish_bundle_to_site

    timestamp = (published_at or datetime.now(UTC)).astimezone(UTC)
    report_dir, latest_dir = build_publish_paths(request, published_at=timestamp)
    slug = build_race_slug(request)
    relative_paths = publish_bundle_to_site(
        source_dir=source_dir,
        site_root=pages_root,
        report_dir=report_dir,
        latest_dir=latest_dir,
        published_metadata={
            "published_at": timestamp.isoformat(),
            "race_slug": slug,
            "race_name": request.race_name,
            "race_url": request.race_url,
            "competition_name": request.competition_name,
        },
    )

    root = (base_url or "").rstrip("/")
    if root:
        report_url = f"{root}/{relative_paths['timestamp_report']}"
        latest_url = f"{root}/{relative_paths['latest_report']}"
        csv_url = f"{root}/{relative_paths['timestamp_csv']}"
        json_url = f"{root}/{relative_paths['timestamp_json']}"
    else:
        report_url = relative_paths["timestamp_report"]
        latest_url = relative_paths["latest_report"]
        csv_url = relative_paths["timestamp_csv"]
        json_url = relative_paths["timestamp_json"]

    return PublishResult(
        race_slug=slug,
        report_dir=report_dir,
        latest_dir=latest_dir,
        report_url=report_url,
        latest_url=latest_url,
        csv_url=csv_url,
        json_url=json_url,
    )


def _load_issue_body(args: argparse.Namespace) -> str:
    if args.issue_body_file:
        return Path(args.issue_body_file).read_text(encoding="utf-8")
    return args.issue_body


def _write_json(path: str | None, payload: dict[str, object]) -> None:
    if path:
        Path(path).write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    else:
        json.dump(payload, sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")


def _cmd_parse_issue(args: argparse.Namespace) -> int:
    request = parse_issue_form(_load_issue_body(args))
    payload = asdict(request)
    payload["race_slug"] = request.race_slug
    _write_json(args.output_json, payload)
    return 0


def _cmd_publish_site(args: argparse.Namespace) -> int:
    request = ReportRequest(
        race_name=args.race_name,
        race_url=args.race_url,
        competition_name=args.competition_name or "",
        score_threshold=float(args.score_threshold),
        top=int(args.top),
        strategy=args.strategy,
        notes=args.notes or "",
    )
    published_at = (
        datetime.fromisoformat(args.published_at.replace("Z", "+00:00"))
        if args.published_at
        else datetime.now(UTC)
    )
    result = publish_report_bundle(
        source_dir=args.source_dir,
        pages_root=args.pages_root,
        request=request,
        published_at=published_at,
        base_url=args.base_url,
    )
    _write_json(args.output_json, asdict(result))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m trailintel.github_pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parse_issue = subparsers.add_parser("parse-issue", help="Parse a GitHub issue form into structured JSON.")
    parse_issue.add_argument("--issue-body", default="", help="Raw issue body text.")
    parse_issue.add_argument("--issue-body-file", help="Path to a file containing the issue body.")
    parse_issue.add_argument("--output-json", help="Optional JSON output path.")
    parse_issue.set_defaults(func=_cmd_parse_issue)

    publish_site = subparsers.add_parser("publish-site", help="Copy a static report bundle into a Pages worktree.")
    publish_site.add_argument("--source-dir", required=True)
    publish_site.add_argument("--pages-root", required=True)
    publish_site.add_argument("--race-name", required=True)
    publish_site.add_argument("--race-url", required=True)
    publish_site.add_argument("--competition-name")
    publish_site.add_argument("--score-threshold", default=str(DEFAULT_SCORE_THRESHOLD))
    publish_site.add_argument("--top", default=str(DEFAULT_TOP))
    publish_site.add_argument("--strategy", default=DEFAULT_STRATEGY)
    publish_site.add_argument("--notes")
    publish_site.add_argument("--published-at")
    publish_site.add_argument("--base-url")
    publish_site.add_argument("--output-json", help="Optional JSON output path.")
    publish_site.set_defaults(func=_cmd_publish_site)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
