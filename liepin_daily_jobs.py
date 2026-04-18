#!/usr/bin/env python3
"""
抓取猎聘公开 SEO 职位页，筛选“互联网公司”的内容安全/风控相关运营岗位，
并按本地 SQLite 状态库输出“今日新增”结果。

默认只抓公开页面，不处理登录、验证码或任何反爬绕过逻辑。
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import sqlite3
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from lxml import html


DEFAULT_CONFIG: dict[str, Any] = {
    "source_urls": [
        "https://www.liepin.com/zpneironganquan/",
        "https://www.liepin.com/zpneironganquanyunying/",
        "https://www.liepin.com/zpfengkong/",
        "https://www.liepin.com/zpfengkongyunying/",
    ],
    "max_pages_per_source": 2,
    "timeout_seconds": 20,
    "list_request_interval_seconds": 1.0,
    "detail_request_interval_seconds": 1.0,
    "company_tag_keywords": ["互联网"],
    "title_domain_keywords": ["内容安全", "风控", "风险控制"],
    "required_title_keywords": ["运营"],
    "excluded_company_keywords": ["汇聚众多行业名企"],
    "output_dir": "liepin_output",
    "database_path": "liepin_output/liepin_jobs.sqlite3",
    "site_dir": "liepin_site",
    "recent_update_window_days": 14,
    "user_agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

TZ = ZoneInfo("Asia/Shanghai")


def normalize_text(value: str) -> str:
    return re.sub(r"[ \t\r\f\v]+", " ", value.replace("\xa0", " ")).strip()


def normalize_multiline_text(chunks: list[str]) -> str:
    lines = [normalize_text(chunk) for chunk in chunks]
    lines = [line for line in lines if line]
    return "\n".join(lines).strip()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_config(config_path: Path | None, script_dir: Path) -> dict[str, Any]:
    config = dict(DEFAULT_CONFIG)
    if config_path and config_path.exists():
        user_config = json.loads(config_path.read_text(encoding="utf-8"))
        config.update(user_config)

    output_dir = Path(config["output_dir"])
    if not output_dir.is_absolute():
        output_dir = script_dir / output_dir
    config["output_dir"] = output_dir

    database_path = Path(config["database_path"])
    if not database_path.is_absolute():
        database_path = script_dir / database_path
    config["database_path"] = database_path

    site_dir = Path(config["site_dir"])
    if not site_dir.is_absolute():
        site_dir = script_dir / site_dir
    config["site_dir"] = site_dir
    return config


def make_page_url(base_url: str, page_index: int) -> str:
    base = base_url.rstrip("/") + "/"
    if page_index == 0:
        return base
    return urljoin(base, f"pn{page_index}/")


def extract_digits(text: str) -> str:
    match = re.search(r"(\d+)(?:\.shtml)?/?$", text)
    return match.group(1) if match else ""


def build_session(user_agent: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": "https://www.liepin.com/",
        }
    )
    return session


def fetch_text(
    session: requests.Session,
    url: str,
    timeout_seconds: int,
    retries: int = 3,
) -> str:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=timeout_seconds)
            response.raise_for_status()
            return response.text
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == retries:
                break
            time.sleep(1.5 * attempt)
    raise RuntimeError(f"请求失败: {url} -> {last_error}") from last_error


def parse_listing_page(page_html: str, page_url: str, source_url: str) -> list[dict[str, Any]]:
    tree = html.fromstring(page_html)
    cards = tree.xpath('//div[contains(@class, "job-list-item")]')
    jobs: list[dict[str, Any]] = []

    for card in cards:
        job_anchor = card.xpath('.//a[@data-nick="job-detail-job-info"]')
        if not job_anchor:
            continue

        anchor = job_anchor[0]
        job_url = normalize_text(anchor.get("href", ""))
        job_id = normalize_text(anchor.get("data-jobId", "")) or extract_digits(job_url)
        job_kind = normalize_text(anchor.get("data-jobKind", "")) or "unknown"
        title = normalize_text("".join(anchor.xpath('.//div[@title][1]/@title')))
        location = normalize_text(
            "".join(anchor.xpath('.//div[contains(@class, "job-dq-box")]//span[contains(@class, "ellipsis-1")]/text()'))
        )
        salary = normalize_text("".join(anchor.xpath('.//span[contains(@class, "job-salary")]/text()')))
        labels = [normalize_text(text) for text in anchor.xpath('.//span[contains(@class, "labels-tag")]/text()')]
        labels = [text for text in labels if text]
        company_name = normalize_text("".join(card.xpath('.//span[contains(@class, "company-name")]/text()')))
        company_tags = [normalize_text(text) for text in card.xpath('.//div[contains(@class, "company-tags-box")]/span/text()')]
        company_tags = [text for text in company_tags if text]

        jobs.append(
            {
                "job_key": f"{job_kind}:{job_id}" if job_id else job_url,
                "job_id": job_id,
                "job_kind": job_kind,
                "job_url": job_url,
                "title": title,
                "location": location,
                "salary": salary,
                "experience": labels[0] if len(labels) >= 1 else "",
                "education": labels[1] if len(labels) >= 2 else "",
                "benefits": " | ".join(labels[2:]) if len(labels) > 2 else "",
                "company_name": company_name,
                "company_industry": company_tags[0] if len(company_tags) >= 1 else "",
                "company_stage": company_tags[1] if len(company_tags) >= 2 else "",
                "company_size": company_tags[2] if len(company_tags) >= 3 else "",
                "source_url": source_url,
                "page_url": page_url,
            }
        )

    return jobs


def title_contains_any(title: str, keywords: list[str]) -> list[str]:
    return [keyword for keyword in keywords if keyword and keyword in title]


def company_is_excluded(company_name: str, excluded_keywords: list[str]) -> bool:
    return any(keyword and keyword in company_name for keyword in excluded_keywords)


def is_target_job(job: dict[str, Any], config: dict[str, Any]) -> tuple[bool, str]:
    title = job["title"]
    company_name = job["company_name"]
    company_industry = job["company_industry"]

    if company_is_excluded(company_name, config["excluded_company_keywords"]):
        return False, "公司命中排除词"

    domain_hits = title_contains_any(title, config["title_domain_keywords"])
    role_hits = title_contains_any(title, config["required_title_keywords"])
    industry_hits = title_contains_any(company_industry, config["company_tag_keywords"])

    if not domain_hits:
        return False, "标题未命中领域词"
    if not role_hits:
        return False, "标题未命中运营词"
    if not industry_hits:
        return False, "公司标签未命中互联网"

    match_reason = (
        f"标题领域词={','.join(domain_hits)}; "
        f"标题岗位词={','.join(role_hits)}; "
        f"公司标签={','.join(industry_hits)}"
    )
    return True, match_reason


def parse_detail_page(page_html: str) -> dict[str, str]:
    tree = html.fromstring(page_html)
    update_time = normalize_text("".join(tree.xpath('//span[contains(@class, "update-time")]/text()')))
    recruit_count = normalize_text("".join(tree.xpath('//span[contains(@class, "recruit-cnt")]/text()')))
    detail_labels = [normalize_text(text) for text in tree.xpath('//section[contains(@class, "job-apply-container-desc")]//div[contains(@class, "labels")]/span/text()')]
    detail_labels = [text for text in detail_labels if text]
    job_intro = normalize_multiline_text(tree.xpath('//dd[@data-selector="job-intro-content"]//text()'))
    company_intro = normalize_multiline_text(
        tree.xpath('//section[contains(@class, "company-intro-container")]//div[contains(@class, "inner")]//text()')
    )
    return {
        "detail_update_time": update_time,
        "detail_recruit_count": recruit_count,
        "detail_labels": " | ".join(detail_labels),
        "job_intro": job_intro,
        "company_intro": company_intro,
    }


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            job_key TEXT PRIMARY KEY,
            job_id TEXT,
            job_kind TEXT,
            job_url TEXT,
            title TEXT,
            location TEXT,
            salary TEXT,
            experience TEXT,
            education TEXT,
            benefits TEXT,
            company_name TEXT,
            company_industry TEXT,
            company_stage TEXT,
            company_size TEXT,
            source_url TEXT,
            page_url TEXT,
            match_reason TEXT,
            detail_update_time TEXT,
            detail_recruit_count TEXT,
            detail_labels TEXT,
            job_intro TEXT,
            company_intro TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_time TEXT PRIMARY KEY,
            run_date TEXT NOT NULL,
            matched_count INTEGER NOT NULL,
            current_run_new_count INTEGER NOT NULL,
            today_new_count INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_jobs (
            run_date TEXT NOT NULL,
            job_key TEXT NOT NULL,
            PRIMARY KEY (run_date, job_key),
            FOREIGN KEY (job_key) REFERENCES jobs(job_key)
        )
        """
    )


def upsert_job(conn: sqlite3.Connection, job: dict[str, Any], run_time: str) -> bool:
    existing = conn.execute(
        "SELECT first_seen_at FROM jobs WHERE job_key = ?",
        (job["job_key"],),
    ).fetchone()
    is_new = existing is None
    first_seen_at = run_time if is_new else existing[0]

    conn.execute(
        """
        INSERT INTO jobs (
            job_key, job_id, job_kind, job_url, title, location, salary,
            experience, education, benefits, company_name, company_industry,
            company_stage, company_size, source_url, page_url, match_reason,
            detail_update_time, detail_recruit_count, detail_labels, job_intro,
            company_intro, first_seen_at, last_seen_at
        )
        VALUES (
            :job_key, :job_id, :job_kind, :job_url, :title, :location, :salary,
            :experience, :education, :benefits, :company_name, :company_industry,
            :company_stage, :company_size, :source_url, :page_url, :match_reason,
            :detail_update_time, :detail_recruit_count, :detail_labels, :job_intro,
            :company_intro, :first_seen_at, :last_seen_at
        )
        ON CONFLICT(job_key) DO UPDATE SET
            job_id = excluded.job_id,
            job_kind = excluded.job_kind,
            job_url = excluded.job_url,
            title = excluded.title,
            location = excluded.location,
            salary = excluded.salary,
            experience = excluded.experience,
            education = excluded.education,
            benefits = excluded.benefits,
            company_name = excluded.company_name,
            company_industry = excluded.company_industry,
            company_stage = excluded.company_stage,
            company_size = excluded.company_size,
            source_url = excluded.source_url,
            page_url = excluded.page_url,
            match_reason = excluded.match_reason,
            detail_update_time = excluded.detail_update_time,
            detail_recruit_count = excluded.detail_recruit_count,
            detail_labels = excluded.detail_labels,
            job_intro = excluded.job_intro,
            company_intro = excluded.company_intro,
            first_seen_at = excluded.first_seen_at,
            last_seen_at = excluded.last_seen_at
        """,
        {**job, "first_seen_at": first_seen_at, "last_seen_at": run_time},
    )
    return is_new


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    fieldnames = [
        "job_key",
        "job_id",
        "job_kind",
        "title",
        "company_name",
        "company_industry",
        "company_stage",
        "company_size",
        "location",
        "salary",
        "experience",
        "education",
        "benefits",
        "detail_update_time",
        "detail_recruit_count",
        "detail_labels",
        "match_reason",
        "source_url",
        "page_url",
        "job_url",
        "first_seen_at",
        "last_seen_at",
        "job_intro",
        "company_intro",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_summary(
    path: Path,
    run_date: str,
    crawled_pages: list[str],
    all_jobs: list[dict[str, Any]],
    first_seen_new_jobs: list[dict[str, Any]],
    snapshot_new_jobs: list[dict[str, Any]],
    recent_update_jobs: list[dict[str, Any]],
    today_updated_jobs: list[dict[str, Any]],
    recent_window_days: int,
) -> None:
    lines = [
        f"# 猎聘增量抓取日报 - {run_date}",
        "",
        f"- 当前命中岗位数：{len(all_jobs)}",
        f"- 今日首次发现岗位数：{len(first_seen_new_jobs)}",
        f"- 相对昨日新增岗位数：{len(snapshot_new_jobs)}",
        f"- 今日更新岗位数：{len(today_updated_jobs)}",
        f"- 近{recent_window_days}天更新岗位数：{len(recent_update_jobs)}",
        f"- 抓取页数：{len(crawled_pages)}",
        "",
        "## 已抓取页面",
    ]
    lines.extend(f"- {url}" for url in crawled_pages)
    lines.append("")

    lines.append("## 相对昨日新增")
    if not snapshot_new_jobs:
        lines.append("- 今天相对昨天没有新增进入快照的目标岗位。")
    else:
        for job in snapshot_new_jobs:
            lines.append(
                "- {company_name} | {title} | {location} | {salary} | {detail_update_time} | {job_url}".format(
                    **job
                )
            )

    lines.append("")
    lines.append(f"## 近{recent_window_days}天更新")
    if not recent_update_jobs:
        lines.append(f"- 最近{recent_window_days}天没有命中的新鲜岗位。")
    else:
        for job in recent_update_jobs[:50]:
            lines.append(
                "- {company_name} | {title} | {location} | {salary} | {detail_update_time} | {job_url}".format(
                    **job
                )
            )
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def dedupe_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for job in jobs:
        key = job["job_key"]
        if key not in merged:
            merged[key] = dict(job)
            continue
        existing = merged[key]
        if job["source_url"] not in existing["source_url"]:
            existing["source_url"] = f"{existing['source_url']} | {job['source_url']}"
        if job["page_url"] not in existing["page_url"]:
            existing["page_url"] = f"{existing['page_url']} | {job['page_url']}"
    return list(merged.values())


def load_jobs_first_seen_on(conn: sqlite3.Connection, date_prefix: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM jobs
        WHERE first_seen_at LIKE ?
        ORDER BY company_name, title
        """,
        (f"{date_prefix}%",),
    ).fetchall()
    return [dict(row) for row in rows]


def parse_update_date(update_label: str, run_day: date) -> date | None:
    label = normalize_text(update_label)
    if not label:
        return None
    if "今日更新" in label:
        return run_day

    match = re.search(r"(\d+)天前更新", label)
    if match:
        return run_day - timedelta(days=int(match.group(1)))

    match = re.search(r"(\d+)月(\d+)日更新", label)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        year = run_day.year
        candidate = date(year, month, day)
        if candidate > run_day + timedelta(days=1):
            candidate = date(year - 1, month, day)
        return candidate
    return None


def job_priority_score(title: str) -> int:
    score = 0
    rules = [
        ("产品运营", 6),
        ("策略运营", 6),
        ("风控运营", 5),
        ("风险运营", 5),
        ("评估", 5),
        ("治理", 4),
        ("内容安全", 4),
        ("风控", 3),
        ("风险", 2),
    ]
    for keyword, weight in rules:
        if keyword in title:
            score += weight
    return score


def sort_jobs_for_display(rows: list[dict[str, Any]], run_day: date) -> list[dict[str, Any]]:
    def sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
        update_date = parse_update_date(row.get("detail_update_time", ""), run_day)
        days_since = (run_day - update_date).days if update_date else 9999
        return (
            days_since,
            -job_priority_score(row.get("title", "")),
            row.get("company_name", ""),
            row.get("title", ""),
            row.get("location", ""),
        )

    return sorted(rows, key=sort_key)


def load_snapshot_keys(conn: sqlite3.Connection, run_date: str) -> set[str]:
    rows = conn.execute(
        "SELECT job_key FROM daily_jobs WHERE run_date = ?",
        (run_date,),
    ).fetchall()
    return {row[0] for row in rows}


def load_previous_run_date(conn: sqlite3.Connection, run_date: str) -> str | None:
    row = conn.execute(
        """
        SELECT MAX(run_date)
        FROM daily_jobs
        WHERE run_date < ?
        """,
        (run_date,),
    ).fetchone()
    return row[0] if row and row[0] else None


def load_latest_run_stats(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT run_date, run_time, matched_count
        FROM runs
        ORDER BY run_time DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    return {"run_date": row[0], "run_time": row[1], "matched_count": row[2]}


def guard_snapshot_health(
    current_count: int,
    latest_stats: dict[str, Any] | None,
    config: dict[str, Any],
) -> None:
    minimum_count = int(config.get("min_allowed_snapshot_count", 0))
    minimum_ratio = float(config.get("min_allowed_snapshot_ratio", 0))

    if latest_stats is None:
        if current_count < minimum_count:
            raise RuntimeError(
                f"抓取结果过低：当前 {current_count} < 最低保护阈值 {minimum_count}。"
            )
        return

    previous_count = int(latest_stats["matched_count"])
    ratio_floor = int(previous_count * minimum_ratio)
    required_minimum = max(minimum_count, ratio_floor)
    if current_count < required_minimum:
        raise RuntimeError(
            "抓取结果疑似异常，已停止覆盖线上数据："
            f" 当前 {current_count}，上次 {previous_count}，"
            f" 最低允许 {required_minimum}。"
        )


def pick_snapshot_new_jobs(
    matched_jobs: list[dict[str, Any]],
    previous_snapshot_keys: set[str],
    run_day: date,
) -> list[dict[str, Any]]:
    rows = [job for job in matched_jobs if job["job_key"] not in previous_snapshot_keys]
    return sort_jobs_for_display(rows, run_day)


def pick_recent_update_jobs(
    matched_jobs: list[dict[str, Any]],
    run_day: date,
    window_days: int,
) -> list[dict[str, Any]]:
    rows = []
    for job in matched_jobs:
        update_date = parse_update_date(job.get("detail_update_time", ""), run_day)
        if update_date is None:
            continue
        days_since = (run_day - update_date).days
        if 0 <= days_since <= window_days:
            rows.append({**job, "_update_date": update_date.isoformat(), "_days_since_update": days_since})
    return sort_jobs_for_display(rows, run_day)


def pick_today_updated_jobs(
    matched_jobs: list[dict[str, Any]],
    run_day: date,
) -> list[dict[str, Any]]:
    rows = []
    for job in matched_jobs:
        update_date = parse_update_date(job.get("detail_update_time", ""), run_day)
        if update_date == run_day:
            rows.append({**job, "_update_date": update_date.isoformat(), "_days_since_update": 0})
    return sort_jobs_for_display(rows, run_day)


def refresh_daily_snapshot(conn: sqlite3.Connection, run_date: str, jobs: list[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM daily_jobs WHERE run_date = ?", (run_date,))
    conn.executemany(
        "INSERT OR IGNORE INTO daily_jobs (run_date, job_key) VALUES (?, ?)",
        [(run_date, job["job_key"]) for job in jobs],
    )


def record_run(
    conn: sqlite3.Connection,
    run_time: str,
    run_date: str,
    matched_count: int,
    current_run_new_count: int,
    today_new_count: int,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO runs (
            run_time, run_date, matched_count, current_run_new_count, today_new_count
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (run_time, run_date, matched_count, current_run_new_count, today_new_count),
    )


def load_run_history(conn: sqlite3.Connection, limit_days: int = 30) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT r.run_date, r.run_time, r.matched_count, r.current_run_new_count, r.today_new_count
        FROM runs r
        INNER JOIN (
            SELECT run_date, MAX(run_time) AS latest_run_time
            FROM runs
            GROUP BY run_date
        ) x
        ON x.run_date = r.run_date AND x.latest_run_time = r.run_time
        ORDER BY r.run_date DESC
        LIMIT ?
        """,
        (limit_days,),
    ).fetchall()
    return [dict(row) for row in rows]


def load_daily_snapshot_dates(conn: sqlite3.Connection, limit_days: int = 30) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT run_date
        FROM daily_jobs
        ORDER BY run_date DESC
        LIMIT ?
        """,
        (limit_days,),
    ).fetchall()
    return [row[0] for row in rows]


def load_daily_snapshots(
    conn: sqlite3.Connection,
    limit_days: int = 30,
) -> dict[str, list[dict[str, Any]]]:
    snapshots: dict[str, list[dict[str, Any]]] = {}
    for run_date in load_daily_snapshot_dates(conn, limit_days=limit_days):
        rows = conn.execute(
            """
            SELECT j.*
            FROM daily_jobs dj
            INNER JOIN jobs j ON j.job_key = dj.job_key
            WHERE dj.run_date = ?
            ORDER BY j.company_name, j.title
            """,
            (run_date,),
        ).fetchall()
        snapshots[run_date] = [dict(row) for row in rows]
    return snapshots


def top_counts(rows: list[dict[str, Any]], field: str, limit: int = 8) -> list[dict[str, Any]]:
    counter = Counter(row.get(field, "") for row in rows if row.get(field, ""))
    return [{"label": label, "count": count} for label, count in counter.most_common(limit)]


def enrich_history_with_snapshot_deltas(
    history: list[dict[str, Any]],
    daily_snapshots: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    ordered_dates = sorted(daily_snapshots.keys())
    snapshot_key_map = {
        run_date: {row["job_key"] for row in rows}
        for run_date, rows in daily_snapshots.items()
    }
    previous_date_map: dict[str, str | None] = {}
    for index, run_date in enumerate(ordered_dates):
        previous_date_map[run_date] = ordered_dates[index - 1] if index > 0 else None

    for row in history:
        run_date = row["run_date"]
        previous_date = previous_date_map.get(run_date)
        current_keys = snapshot_key_map.get(run_date, set())
        previous_keys = snapshot_key_map.get(previous_date, set()) if previous_date else set()
        enriched.append(
            {
                **row,
                "snapshot_new_count": len(current_keys - previous_keys) if previous_date else len(current_keys),
            }
        )
    return enriched


def json_for_script(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False).replace("</", "<\\/")


def write_dashboard(
    path: Path,
    run_time: str,
    crawled_pages: list[str],
    all_jobs: list[dict[str, Any]],
    first_seen_new_jobs: list[dict[str, Any]],
    snapshot_new_jobs: list[dict[str, Any]],
    recent_update_jobs: list[dict[str, Any]],
    today_updated_jobs: list[dict[str, Any]],
    latest_snapshot_name: str,
    latest_new_jobs_name: str,
    latest_summary_name: str,
    history: list[dict[str, Any]],
    daily_snapshots: dict[str, list[dict[str, Any]]],
    recent_window_days: int,
) -> None:
    ensure_dir(path.parent)

    dashboard_payload = {
        "firstSeenNewJobs": first_seen_new_jobs,
        "snapshotNewJobs": snapshot_new_jobs,
        "recentUpdateJobs": recent_update_jobs,
        "todayUpdatedJobs": today_updated_jobs,
        "allJobs": all_jobs,
        "history": history,
        "dailySnapshots": daily_snapshots,
        "recentWindowDays": recent_window_days,
        "sources": crawled_pages,
        "stats": {
            "matchedCount": len(all_jobs),
            "firstSeenNewCount": len(first_seen_new_jobs),
            "snapshotNewCount": len(snapshot_new_jobs),
            "recentUpdateCount": len(recent_update_jobs),
            "todayUpdatedCount": len(today_updated_jobs),
            "runTime": run_time,
            "topCompanies": top_counts(all_jobs, "company_name"),
            "topCities": top_counts(all_jobs, "location"),
        },
        "downloads": {
            "latestSnapshot": latest_snapshot_name,
            "latestNewJobs": latest_new_jobs_name,
            "latestSummary": latest_summary_name,
        },
    }

    dashboard_html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>猎聘岗位增量看板</title>
  <style>
    :root {{
      --bg: #f4efe6;
      --panel: rgba(255, 252, 246, 0.88);
      --ink: #1c2430;
      --muted: #6a7482;
      --line: rgba(28, 36, 48, 0.12);
      --accent: #146356;
      --accent-2: #d76a32;
      --accent-soft: rgba(20, 99, 86, 0.12);
      --shadow: 0 18px 60px rgba(28, 36, 48, 0.08);
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "PingFang SC", "Noto Sans SC", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(215, 106, 50, 0.14), transparent 28%),
        radial-gradient(circle at top right, rgba(20, 99, 86, 0.12), transparent 30%),
        linear-gradient(180deg, #f7f2ea 0%, #efe7d9 100%);
      min-height: 100vh;
    }}
    .shell {{
      max-width: 1280px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }}
    .hero {{
      background: linear-gradient(135deg, rgba(255,255,255,0.86), rgba(255,248,238,0.74));
      border: 1px solid rgba(255,255,255,0.7);
      border-radius: 28px;
      box-shadow: var(--shadow);
      padding: 28px;
      backdrop-filter: blur(10px);
    }}
    .eyebrow {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(20, 99, 86, 0.08);
      color: var(--accent);
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0.04em;
    }}
    h1 {{
      margin: 16px 0 8px;
      font-size: clamp(30px, 5vw, 52px);
      line-height: 1;
      letter-spacing: -0.03em;
    }}
    .subtitle {{
      margin: 0;
      max-width: 820px;
      color: var(--muted);
      font-size: 16px;
      line-height: 1.7;
    }}
    .links {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 22px;
    }}
    .links a {{
      color: var(--ink);
      text-decoration: none;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.74);
      padding: 10px 14px;
      border-radius: 14px;
      font-weight: 600;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(12, minmax(0, 1fr));
      gap: 18px;
      margin-top: 22px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid rgba(255,255,255,0.68);
      border-radius: 24px;
      box-shadow: var(--shadow);
      padding: 20px;
      backdrop-filter: blur(8px);
    }}
    .stat {{
      grid-column: span 3;
    }}
    .stat-label {{
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 8px;
    }}
    .stat-value {{
      font-size: clamp(28px, 4vw, 42px);
      font-weight: 700;
      letter-spacing: -0.04em;
    }}
    .stat-note {{
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
    }}
    .panel-wide {{
      grid-column: span 8;
    }}
    .panel-side {{
      grid-column: span 4;
    }}
    .panel-full {{
      grid-column: span 12;
    }}
    .panel-title {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      margin-bottom: 16px;
    }}
    .panel-title h2 {{
      margin: 0;
      font-size: 20px;
      letter-spacing: -0.02em;
    }}
    .muted {{
      color: var(--muted);
      font-size: 13px;
    }}
    .chips {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }}
    .chip {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 12px;
      border-radius: 999px;
      background: rgba(255,255,255,0.78);
      border: 1px solid var(--line);
      font-size: 13px;
    }}
    .chip strong {{
      color: var(--accent-2);
      font-size: 14px;
    }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 16px;
    }}
    .tabs {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }}
    .tab {{
      border: 0;
      border-radius: 999px;
      padding: 10px 14px;
      font: inherit;
      cursor: pointer;
      background: rgba(255,255,255,0.78);
      color: var(--ink);
      border: 1px solid var(--line);
    }}
    .tab.active {{
      background: linear-gradient(135deg, var(--accent), #1a7b6a);
      color: #fff;
      border-color: transparent;
    }}
    .search {{
      min-width: min(360px, 100%);
      flex: 1;
      display: flex;
      justify-content: flex-end;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .search input {{
      width: min(420px, 100%);
      border-radius: 14px;
      border: 1px solid var(--line);
      padding: 12px 14px;
      font: inherit;
      background: rgba(255,255,255,0.82);
      color: var(--ink);
    }}
    .search select {{
      border-radius: 14px;
      border: 1px solid var(--line);
      padding: 12px 14px;
      font: inherit;
      background: rgba(255,255,255,0.82);
      color: var(--ink);
    }}
    .filter-row {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 14px;
    }}
    .filter-row select {{
      border-radius: 14px;
      border: 1px solid var(--line);
      padding: 10px 12px;
      font: inherit;
      background: rgba(255,255,255,0.82);
      color: var(--ink);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    thead th {{
      text-align: left;
      padding: 12px 10px;
      font-size: 12px;
      color: var(--muted);
      border-bottom: 1px solid var(--line);
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    tbody td {{
      padding: 16px 10px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
      font-size: 14px;
      line-height: 1.6;
    }}
    tbody tr:hover {{
      background: rgba(20, 99, 86, 0.04);
    }}
    .title-link {{
      color: var(--ink);
      font-weight: 700;
      text-decoration: none;
    }}
    .meta-line {{
      color: var(--muted);
      font-size: 12px;
      margin-top: 4px;
    }}
    .source-list {{
      margin: 0;
      padding-left: 18px;
      color: var(--muted);
    }}
    .source-list li + li {{
      margin-top: 8px;
    }}
    .source-list a {{
      color: var(--accent);
    }}
    .history-list {{
      display: flex;
      flex-direction: column;
      gap: 10px;
    }}
    .history-item {{
      width: 100%;
      text-align: left;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.78);
      border-radius: 16px;
      padding: 14px;
      cursor: pointer;
      font: inherit;
      color: var(--ink);
    }}
    .history-item.active {{
      background: linear-gradient(135deg, rgba(20,99,86,0.16), rgba(215,106,50,0.14));
      border-color: rgba(20,99,86,0.24);
    }}
    .history-top {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      font-weight: 700;
    }}
    .history-bottom {{
      margin-top: 8px;
      color: var(--muted);
      font-size: 12px;
      display: flex;
      justify-content: space-between;
      gap: 12px;
    }}
    .empty {{
      padding: 40px 0;
      text-align: center;
      color: var(--muted);
    }}
    .footer {{
      margin-top: 18px;
      color: var(--muted);
      font-size: 12px;
    }}
    @media (max-width: 980px) {{
      .stat,
      .panel-wide,
      .panel-full,
      .panel-side {{
        grid-column: span 12;
      }}
      .toolbar {{
        align-items: stretch;
      }}
      .search {{
        justify-content: stretch;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="eyebrow">Liepin Watchboard</div>
      <h1>内容安全 / 风控 运营岗位看板</h1>
      <p class="subtitle">
        每次自动运行后，这个页面会同步更新。默认优先展示最近更新的岗位内容，同时保留相对昨日新增、首次发现新增、当前全量快照和历史日期快照。
      </p>
      <div class="links">
        <a href="liepin_recent_updates_latest.csv">下载最近更新 CSV</a>
        <a href="liepin_snapshot_new_latest.csv">下载相对昨日新增 CSV</a>
        <a href="{latest_new_jobs_name}">下载首次发现新增 CSV</a>
        <a href="{latest_snapshot_name}">下载当前快照 CSV</a>
        <a href="{latest_summary_name}">查看摘要 Markdown</a>
      </div>
    </section>

    <section class="grid">
      <article class="card stat">
        <div class="stat-label">当前命中岗位</div>
        <div class="stat-value" id="matched-count">0</div>
        <div class="stat-note">公开页过滤后的当前可见岗位数</div>
      </article>
      <article class="card stat">
        <div class="stat-label">近14天更新</div>
        <div class="stat-value" id="recent-update-count">0</div>
        <div class="stat-note">你每天最值得先看的新鲜岗位池</div>
      </article>
      <article class="card stat">
        <div class="stat-label">今日更新</div>
        <div class="stat-value" id="today-updated-count">0</div>
        <div class="stat-note">职位详情页显示为“今日更新”的岗位</div>
      </article>
      <article class="card stat">
        <div class="stat-label">相对昨日新增</div>
        <div class="stat-value" id="snapshot-new-count">0</div>
        <div class="stat-note">今天快照里有、昨天快照里没有</div>
      </article>
      <article class="card stat">
        <div class="stat-label">最后更新时间</div>
        <div class="stat-value" style="font-size:22px; line-height:1.35" id="run-time">-</div>
        <div class="stat-note">自动任务成功后这里会刷新</div>
      </article>

      <article class="card panel-side">
        <div class="panel-title">
          <h2>历史日期</h2>
          <div class="muted">每日最新快照</div>
        </div>
        <div class="history-list" id="history-list"></div>
      </article>

      <article class="card panel-wide">
        <div class="panel-title">
          <h2>岗位明细</h2>
          <div class="muted" id="result-meta">-</div>
        </div>
        <div class="toolbar">
          <div class="tabs">
            <button class="tab active" data-view="recent">最近更新</button>
            <button class="tab" data-view="snapshot">相对昨日新增</button>
            <button class="tab" data-view="first_seen">首次发现新增</button>
            <button class="tab" data-view="all">当前全量快照</button>
            <button class="tab" data-view="history">历史日期快照</button>
          </div>
          <div class="search">
            <select id="history-date-select" disabled>
              <option value="">选择历史日期</option>
            </select>
            <select id="sort-select">
              <option value="fresh">按新鲜度排序</option>
              <option value="salary_desc">按薪资从高到低</option>
              <option value="salary_asc">按薪资从低到高</option>
              <option value="company">按公司排序</option>
              <option value="location">按地点排序</option>
              <option value="first_seen_desc">按首次发现时间排序</option>
            </select>
            <input id="search" type="search" placeholder="搜索公司、岗位、城市、行业标签" />
          </div>
        </div>
        <div class="filter-row">
          <select id="company-filter">
            <option value="">全部公司</option>
          </select>
          <select id="city-filter">
            <option value="">全部地点</option>
          </select>
          <select id="domain-filter">
            <option value="">全部方向</option>
            <option value="内容安全">内容安全</option>
            <option value="风控">风控</option>
            <option value="风险">风险</option>
            <option value="评估">评估</option>
          </select>
          <select id="freshness-filter">
            <option value="all">全部更新时间</option>
            <option value="today">仅今日更新</option>
            <option value="7">近7天更新</option>
            <option value="14">近14天更新</option>
            <option value="older">14天前更新</option>
          </select>
        </div>
        <div style="overflow:auto">
          <table>
            <thead>
              <tr>
                <th>岗位</th>
                <th>公司</th>
                <th>地点 / 薪资</th>
                <th>新鲜度</th>
              </tr>
            </thead>
            <tbody id="job-table-body"></tbody>
          </table>
        </div>
        <div class="empty" id="empty-state" hidden>没有匹配到结果。</div>
        <div class="footer">点击岗位标题可直接打开猎聘职位页。</div>
      </article>

      <article class="card panel-side">
        <div class="panel-title">
          <h2>高频公司</h2>
          <div class="muted">当前快照 Top 8</div>
        </div>
        <div class="chips" id="company-chips"></div>
      </article>

      <article class="card panel-side">
        <div class="panel-title">
          <h2>高频城市</h2>
          <div class="muted">当前快照 Top 8</div>
        </div>
        <div class="chips" id="city-chips"></div>
      </article>

      <article class="card panel-full">
        <div class="panel-title">
          <h2>历史趋势</h2>
          <div class="muted">每天最后一次成功执行的结果</div>
        </div>
        <div style="overflow:auto">
          <table>
            <thead>
              <tr>
                <th>日期</th>
                <th>当日快照存量</th>
                <th>相对昨日新增</th>
                <th>当日最后刷新时间</th>
              </tr>
            </thead>
            <tbody id="history-table-body"></tbody>
          </table>
        </div>
      </article>

      <article class="card panel-wide">
        <div class="panel-title">
          <h2>抓取来源</h2>
          <div class="muted">公开 SEO 页</div>
        </div>
        <ol class="source-list" id="source-list"></ol>
      </article>
    </section>
  </div>

  <script>
    const DATA = {json_for_script(dashboard_payload)};
    const RUN_DATE = DATA.stats.runTime.slice(0, 10);
    const state = {{
      view: "recent",
      query: "",
      historyDate: DATA.history.length ? DATA.history[0].run_date : "",
      sort: "fresh",
      filters: {{
        company: "",
        city: "",
        domain: "",
        freshness: "all"
      }}
    }};

    const parseUpdateDate = (label) => {{
      if (!label) return null;
      if (label.includes("今日更新")) return new Date(`${{RUN_DATE}}T00:00:00`);

      const daysAgo = label.match(/(\\d+)天前更新/);
      if (daysAgo) {{
        const d = new Date(`${{RUN_DATE}}T00:00:00`);
        d.setDate(d.getDate() - Number(daysAgo[1]));
        return d;
      }}

      const monthDay = label.match(/(\\d+)月(\\d+)日更新/);
      if (monthDay) {{
        const d = new Date(`${{RUN_DATE}}T00:00:00`);
        const candidate = new Date(d.getFullYear(), Number(monthDay[1]) - 1, Number(monthDay[2]));
        if (candidate.getTime() > d.getTime() + 24 * 3600 * 1000) {{
          candidate.setFullYear(candidate.getFullYear() - 1);
        }}
        return candidate;
      }}
      return null;
    }};

    const daysSinceUpdate = (label) => {{
      const parsed = parseUpdateDate(label);
      if (!parsed) return 9999;
      const runDate = new Date(`${{RUN_DATE}}T00:00:00`);
      return Math.floor((runDate.getTime() - parsed.getTime()) / (24 * 3600 * 1000));
    }};

    const parseSalaryValue = (label) => {{
      if (!label) return -1;
      const match = label.match(/(\\d+(?:\\.\\d+)?)\\s*-\\s*(\\d+(?:\\.\\d+)?)k/i);
      if (match) return Number(match[2]);
      const single = label.match(/(\\d+(?:\\.\\d+)?)k/i);
      if (single) return Number(single[1]);
      return -1;
    }};

    const titlePriority = (title) => {{
      let score = 0;
      [
        ["产品运营", 6],
        ["策略运营", 6],
        ["风控运营", 5],
        ["风险运营", 5],
        ["评估", 5],
        ["治理", 4],
        ["内容安全", 4],
        ["风控", 3],
        ["风险", 2]
      ].forEach(([keyword, weight]) => {{
        if ((title || "").includes(keyword)) score += weight;
      }});
      return score;
    }};

    const formatMeta = (rows) => {{
      const label = state.view === "recent"
        ? `近${{DATA.recentWindowDays}}天更新`
        : state.view === "snapshot"
          ? "相对昨日新增"
          : state.view === "first_seen"
            ? "首次发现新增"
        : state.view === "all"
          ? "当前全量快照"
          : `历史日期快照：${{state.historyDate || "未选择"}}`;
      return `${{label}} · 共 ${{rows.length}} 条`;
    }};

    const matchRow = (row, query) => {{
      if (!query) return true;
      const haystack = [
        row.title,
        row.company_name,
        row.location,
        row.salary,
        row.company_industry,
        row.company_stage,
        row.company_size,
        row.detail_update_time
      ].join(" ").toLowerCase();
      return haystack.includes(query);
    }};

    const rowMatchesFilters = (row) => {{
      if (state.filters.company && row.company_name !== state.filters.company) return false;
      if (state.filters.city && row.location !== state.filters.city) return false;
      if (state.filters.domain && !(row.title || "").includes(state.filters.domain)) return false;

      const age = daysSinceUpdate(row.detail_update_time || "");
      if (state.filters.freshness === "today" && age !== 0) return false;
      if (state.filters.freshness === "7" && age > 7) return false;
      if (state.filters.freshness === "14" && age > 14) return false;
      if (state.filters.freshness === "older" && age <= 14) return false;
      return true;
    }};

    const sortRows = (rows) => {{
      const sorted = [...rows];
      sorted.sort((a, b) => {{
        if (state.sort === "salary_desc") {{
          return parseSalaryValue(b.salary) - parseSalaryValue(a.salary);
        }}
        if (state.sort === "salary_asc") {{
          return parseSalaryValue(a.salary) - parseSalaryValue(b.salary);
        }}
        if (state.sort === "company") {{
          return (a.company_name || "").localeCompare(b.company_name || "", "zh-Hans-CN");
        }}
        if (state.sort === "location") {{
          return (a.location || "").localeCompare(b.location || "", "zh-Hans-CN");
        }}
        if (state.sort === "first_seen_desc") {{
          return (b.first_seen_at || "").localeCompare(a.first_seen_at || "");
        }}

        const freshness = daysSinceUpdate(a.detail_update_time || "") - daysSinceUpdate(b.detail_update_time || "");
        if (freshness !== 0) return freshness;

        const priority = titlePriority(b.title || "") - titlePriority(a.title || "");
        if (priority !== 0) return priority;

        return (a.company_name || "").localeCompare(b.company_name || "", "zh-Hans-CN");
      }});
      return sorted;
    }};

    const getRows = () => {{
      let base = DATA.recentUpdateJobs;
      if (state.view === "snapshot") {{
        base = DATA.snapshotNewJobs;
      }}
      if (state.view === "first_seen") {{
        base = DATA.firstSeenNewJobs;
      }}
      if (state.view === "all") {{
        base = DATA.allJobs;
      }}
      if (state.view === "history") {{
        base = DATA.dailySnapshots[state.historyDate] || [];
      }}
      const query = state.query.trim().toLowerCase();
      return sortRows(base.filter((row) => matchRow(row, query) && rowMatchesFilters(row)));
    }};

    const renderRows = () => {{
      const rows = getRows();
      const body = document.getElementById("job-table-body");
      const empty = document.getElementById("empty-state");
      document.getElementById("result-meta").textContent = formatMeta(rows);

      if (!rows.length) {{
        body.innerHTML = "";
        empty.hidden = false;
        return;
      }}

      empty.hidden = true;
      body.innerHTML = rows.map((row) => `
        <tr>
          <td>
            <a class="title-link" href="${{row.job_url}}" target="_blank" rel="noreferrer">${{row.title || "-"}}</a>
            <div class="meta-line">${{row.experience || "经验不限"}} / ${{row.education || "学历不限"}}</div>
          </td>
          <td>
            <div><strong>${{row.company_name || "-"}}</strong></div>
            <div class="meta-line">${{[row.company_industry, row.company_stage, row.company_size].filter(Boolean).join(" / ")}}</div>
          </td>
          <td>
            <div>${{row.location || "-"}}</div>
            <div class="meta-line">${{row.salary || "-"}}</div>
          </td>
          <td>
            <div>${{row.detail_update_time || "-"}}</div>
            <div class="meta-line">${{row.first_seen_at ? `首次发现：${{row.first_seen_at}}` : ""}}</div>
          </td>
        </tr>
      `).join("");
    }};

    const renderHistoryControls = () => {{
      const list = document.getElementById("history-list");
      const select = document.getElementById("history-date-select");
      const options = DATA.history.map((row) => `
        <option value="${{row.run_date}}" ${{row.run_date === state.historyDate ? "selected" : ""}}>
          ${{row.run_date}} · 存量${{row.matched_count}} · 昨日差异${{row.snapshot_new_count || 0}}
        </option>
      `).join("");

      select.innerHTML = `<option value="">选择历史日期</option>${{options}}`;
      select.disabled = state.view !== "history" || !DATA.history.length;

      list.innerHTML = DATA.history.map((row) => `
        <button class="history-item ${{row.run_date === state.historyDate ? "active" : ""}}" data-date="${{row.run_date}}">
          <div class="history-top">
            <span>${{row.run_date}}</span>
            <span>${{row.matched_count}} 条</span>
          </div>
          <div class="history-bottom">
            <span>昨日差异 ${{row.snapshot_new_count || 0}}</span>
            <span>${{row.run_time.split("T")[1] || row.run_time}}</span>
          </div>
        </button>
      `).join("");

      list.querySelectorAll("[data-date]").forEach((button) => {{
        button.addEventListener("click", () => {{
          state.historyDate = button.dataset.date;
          state.view = "history";
          document.querySelectorAll(".tab").forEach((tab) => tab.classList.toggle("active", tab.dataset.view === "history"));
          renderHistoryControls();
          renderRows();
        }});
      }});
    }};

    const renderHistoryTable = () => {{
      const body = document.getElementById("history-table-body");
      body.innerHTML = DATA.history.map((row) => `
        <tr>
          <td>${{row.run_date}}</td>
          <td>${{row.matched_count}}</td>
          <td>${{row.snapshot_new_count || 0}}</td>
          <td>${{row.run_time}}</td>
        </tr>
      `).join("");
    }};

    const renderChips = (targetId, items) => {{
      const el = document.getElementById(targetId);
      el.innerHTML = items.map((item) => `
        <div class="chip"><span>${{item.label}}</span><strong>${{item.count}}</strong></div>
      `).join("");
    }};

    const uniqueOptions = (rows, field) => {{
      return [...new Set(rows.map((row) => row[field]).filter(Boolean))].sort((a, b) => a.localeCompare(b, "zh-Hans-CN"));
    }};

    const renderFilterOptions = () => {{
      const companySelect = document.getElementById("company-filter");
      const citySelect = document.getElementById("city-filter");
      const baseRows = DATA.allJobs;
      const companyOptions = uniqueOptions(baseRows, "company_name");
      const cityOptions = uniqueOptions(baseRows, "location");
      companySelect.innerHTML = `<option value="">全部公司</option>` + companyOptions.map((v) =>
        `<option value="${{v}}" ${{v === state.filters.company ? "selected" : ""}}>${{v}}</option>`
      ).join("");
      citySelect.innerHTML = `<option value="">全部地点</option>` + cityOptions.map((v) =>
        `<option value="${{v}}" ${{v === state.filters.city ? "selected" : ""}}>${{v}}</option>`
      ).join("");
      document.getElementById("domain-filter").value = state.filters.domain;
      document.getElementById("freshness-filter").value = state.filters.freshness;
      document.getElementById("sort-select").value = state.sort;
    }};

    const renderSources = () => {{
      const el = document.getElementById("source-list");
      el.innerHTML = DATA.sources.map((url) => `<li><a href="${{url}}" target="_blank" rel="noreferrer">${{url}}</a></li>`).join("");
    }};

    const renderHeader = () => {{
      document.getElementById("matched-count").textContent = DATA.stats.matchedCount;
      document.getElementById("recent-update-count").textContent = DATA.stats.recentUpdateCount;
      document.getElementById("today-updated-count").textContent = DATA.stats.todayUpdatedCount;
      document.getElementById("snapshot-new-count").textContent = DATA.stats.snapshotNewCount;
      document.getElementById("run-time").textContent = DATA.stats.runTime;
      renderChips("company-chips", DATA.stats.topCompanies);
      renderChips("city-chips", DATA.stats.topCities);
      renderSources();
      renderFilterOptions();
      renderHistoryControls();
      renderHistoryTable();
    }};

    document.querySelectorAll(".tab").forEach((button) => {{
      button.addEventListener("click", () => {{
        state.view = button.dataset.view;
        document.querySelectorAll(".tab").forEach((tab) => tab.classList.toggle("active", tab === button));
        renderHistoryControls();
        renderRows();
      }});
    }});

    document.getElementById("history-date-select").addEventListener("change", (event) => {{
      state.historyDate = event.target.value;
      renderHistoryControls();
      renderRows();
    }});

    document.getElementById("search").addEventListener("input", (event) => {{
      state.query = event.target.value;
      renderRows();
    }});

    document.getElementById("sort-select").addEventListener("change", (event) => {{
      state.sort = event.target.value;
      renderRows();
    }});

    document.getElementById("company-filter").addEventListener("change", (event) => {{
      state.filters.company = event.target.value;
      renderRows();
    }});

    document.getElementById("city-filter").addEventListener("change", (event) => {{
      state.filters.city = event.target.value;
      renderRows();
    }});

    document.getElementById("domain-filter").addEventListener("change", (event) => {{
      state.filters.domain = event.target.value;
      renderRows();
    }});

    document.getElementById("freshness-filter").addEventListener("change", (event) => {{
      state.filters.freshness = event.target.value;
      renderRows();
    }});

    renderHeader();
    renderRows();
  </script>
</body>
</html>
"""
    path.write_text(dashboard_html, encoding="utf-8")


def run(config: dict[str, Any], override_max_pages: int | None = None) -> dict[str, Any]:
    ensure_dir(config["output_dir"])
    ensure_dir(config["database_path"].parent)
    ensure_dir(config["site_dir"])

    run_dt = datetime.now(TZ)
    run_time = run_dt.isoformat(timespec="seconds")
    run_day = run_dt.date().isoformat()
    run_date = run_dt.strftime("%Y%m%d")
    max_pages = override_max_pages or int(config["max_pages_per_source"])
    recent_window_days = int(config["recent_update_window_days"])

    session = build_session(config["user_agent"])
    crawled_pages: list[str] = []
    matched_jobs: list[dict[str, Any]] = []

    for source_url in config["source_urls"]:
        for page_index in range(max_pages):
            page_url = make_page_url(source_url, page_index)
            page_html = fetch_text(session, page_url, config["timeout_seconds"])
            crawled_pages.append(page_url)
            page_jobs = parse_listing_page(page_html, page_url=page_url, source_url=source_url)
            if not page_jobs:
                break

            for job in page_jobs:
                ok, reason = is_target_job(job, config)
                if not ok:
                    continue
                job["match_reason"] = reason
                matched_jobs.append(job)

            time.sleep(float(config["list_request_interval_seconds"]))

    matched_jobs = dedupe_jobs(matched_jobs)

    guard_conn = sqlite3.connect(config["database_path"])
    try:
        init_db(guard_conn)
        guard_snapshot_health(
            current_count=len(matched_jobs),
            latest_stats=load_latest_run_stats(guard_conn),
            config=config,
        )
    finally:
        guard_conn.close()

    for index, job in enumerate(matched_jobs):
        detail_html = fetch_text(session, job["job_url"], config["timeout_seconds"])
        job.update(parse_detail_page(detail_html))
        if index < len(matched_jobs) - 1:
            time.sleep(float(config["detail_request_interval_seconds"]))

    conn = sqlite3.connect(config["database_path"])
    conn.row_factory = sqlite3.Row
    init_db(conn)

    current_run_new_jobs: list[dict[str, Any]] = []
    try:
        previous_run_date = load_previous_run_date(conn, run_day)
        previous_snapshot_keys = load_snapshot_keys(conn, previous_run_date) if previous_run_date else set()

        for job in matched_jobs:
            is_new = upsert_job(conn, job, run_time)
            job["first_seen_at"] = run_time if is_new else conn.execute(
                "SELECT first_seen_at FROM jobs WHERE job_key = ?",
                (job["job_key"],),
            ).fetchone()[0]
            job["last_seen_at"] = run_time
            if is_new:
                current_run_new_jobs.append(job)
        first_seen_new_jobs = load_jobs_first_seen_on(conn, run_day)
        snapshot_new_jobs = pick_snapshot_new_jobs(matched_jobs, previous_snapshot_keys, run_dt.date())
        recent_update_jobs = pick_recent_update_jobs(matched_jobs, run_dt.date(), recent_window_days)
        today_updated_jobs = pick_today_updated_jobs(matched_jobs, run_dt.date())
        refresh_daily_snapshot(conn, run_day, matched_jobs)
        record_run(
            conn,
            run_time=run_time,
            run_date=run_day,
            matched_count=len(matched_jobs),
            current_run_new_count=len(current_run_new_jobs),
            today_new_count=len(first_seen_new_jobs),
        )
        conn.commit()
        daily_snapshots = load_daily_snapshots(conn, limit_days=30)
        history = enrich_history_with_snapshot_deltas(
            load_run_history(conn, limit_days=30),
            daily_snapshots,
        )
    finally:
        conn.close()

    matched_jobs.sort(key=lambda item: (item["company_name"], item["title"]))
    current_run_new_jobs.sort(key=lambda item: (item["company_name"], item["title"]))
    first_seen_new_jobs = sort_jobs_for_display(first_seen_new_jobs, run_dt.date())

    snapshot_path = config["output_dir"] / f"liepin_snapshot_{run_date}.csv"
    new_jobs_path = config["output_dir"] / f"liepin_new_jobs_{run_date}.csv"
    summary_path = config["output_dir"] / f"liepin_summary_{run_date}.md"
    snapshot_new_path = config["output_dir"] / f"liepin_snapshot_new_{run_date}.csv"
    recent_updates_path = config["output_dir"] / f"liepin_recent_updates_{run_date}.csv"
    latest_snapshot_path = config["output_dir"] / "liepin_snapshot_latest.csv"
    latest_new_jobs_path = config["output_dir"] / "liepin_new_jobs_latest.csv"
    latest_summary_path = config["output_dir"] / "liepin_summary_latest.md"
    latest_snapshot_new_path = config["output_dir"] / "liepin_snapshot_new_latest.csv"
    latest_recent_updates_path = config["output_dir"] / "liepin_recent_updates_latest.csv"
    dashboard_path = config["output_dir"] / "liepin_dashboard.html"
    site_index_path = config["site_dir"] / "index.html"
    site_snapshot_path = config["site_dir"] / latest_snapshot_path.name
    site_new_jobs_path = config["site_dir"] / latest_new_jobs_path.name
    site_summary_path = config["site_dir"] / latest_summary_path.name
    site_snapshot_new_path = config["site_dir"] / latest_snapshot_new_path.name
    site_recent_updates_path = config["site_dir"] / latest_recent_updates_path.name

    write_csv(snapshot_path, matched_jobs)
    write_csv(new_jobs_path, first_seen_new_jobs)
    write_csv(snapshot_new_path, snapshot_new_jobs)
    write_csv(recent_updates_path, recent_update_jobs)
    write_summary(
        summary_path,
        run_date,
        crawled_pages,
        matched_jobs,
        first_seen_new_jobs,
        snapshot_new_jobs,
        recent_update_jobs,
        today_updated_jobs,
        recent_window_days,
    )
    write_csv(latest_snapshot_path, matched_jobs)
    write_csv(latest_new_jobs_path, first_seen_new_jobs)
    write_csv(latest_snapshot_new_path, snapshot_new_jobs)
    write_csv(latest_recent_updates_path, recent_update_jobs)
    write_summary(
        latest_summary_path,
        run_date,
        crawled_pages,
        matched_jobs,
        first_seen_new_jobs,
        snapshot_new_jobs,
        recent_update_jobs,
        today_updated_jobs,
        recent_window_days,
    )
    write_dashboard(
        dashboard_path,
        run_time=run_time,
        crawled_pages=crawled_pages,
        all_jobs=matched_jobs,
        first_seen_new_jobs=first_seen_new_jobs,
        snapshot_new_jobs=snapshot_new_jobs,
        recent_update_jobs=recent_update_jobs,
        today_updated_jobs=today_updated_jobs,
        latest_snapshot_name=latest_snapshot_path.name,
        latest_new_jobs_name=latest_new_jobs_path.name,
        latest_summary_name=latest_summary_path.name,
        history=history,
        daily_snapshots=daily_snapshots,
        recent_window_days=recent_window_days,
    )
    shutil.copy2(latest_snapshot_path, site_snapshot_path)
    shutil.copy2(latest_new_jobs_path, site_new_jobs_path)
    shutil.copy2(latest_summary_path, site_summary_path)
    shutil.copy2(latest_snapshot_new_path, site_snapshot_new_path)
    shutil.copy2(latest_recent_updates_path, site_recent_updates_path)
    shutil.copy2(dashboard_path, site_index_path)

    return {
        "run_time": run_time,
        "snapshot_path": str(snapshot_path),
        "new_jobs_path": str(new_jobs_path),
        "summary_path": str(summary_path),
        "latest_snapshot_path": str(latest_snapshot_path),
        "latest_new_jobs_path": str(latest_new_jobs_path),
        "latest_summary_path": str(latest_summary_path),
        "latest_snapshot_new_path": str(latest_snapshot_new_path),
        "latest_recent_updates_path": str(latest_recent_updates_path),
        "dashboard_path": str(dashboard_path),
        "site_index_path": str(site_index_path),
        "matched_count": len(matched_jobs),
        "current_run_new_count": len(current_run_new_jobs),
        "today_new_count": len(first_seen_new_jobs),
        "snapshot_new_count": len(snapshot_new_jobs),
        "recent_update_count": len(recent_update_jobs),
        "today_updated_count": len(today_updated_jobs),
        "crawled_pages": crawled_pages,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="抓取猎聘公开职位页，输出内容安全/风控相关运营岗位的每日新增结果。"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("liepin_jobs_config.json"),
        help="可选 JSON 配置文件路径。",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="覆盖配置中的每个来源最多抓取页数。",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    config_path = args.config if args.config else None
    config = load_config(config_path, script_dir)

    result = run(config, override_max_pages=args.max_pages)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
