"""
telegram_proposal_bot.py  (v5)
1인 무인 외주 자동화 시스템 — 텔레그램 봇

[전체 흐름]
1. 서버(Render / Railway / VPS)에서 24시간 구동, RSS로 일감을 정기 수집
2. 새 공고마다 Claude가 "엄격한 필터"로 분석 (비용 절감을 위해 가벼운 JSON 판정 1회)
     - 시간제(Hourly)는 100% 제외, '건당 고정가(Fixed-Price)'만 통과
     - 순수 코드 구현 / API 연동 / 디버깅만 통과 (잡부·재택알바·고객센터·번역·디자인·글쓰기는 폐기)
     - 예상 3시간 이내, 코드 1개로 끝나는 '아주 간단한 작업'만 통과
   ※ 조건은 Claude의 판정에만 맡기지 않고 코드에서 한 번 더 강제로 검사함
3. 통과한 공고만 "🚨 [개발 외주 꿀알감 포착]" 분석 리포트로 알림
     (프로젝트 요약 / 시간·비용 효율 / 구현 완성도·리스크 / 영문 제안서 초안 3~4문장)
   + [✅ 수주 승인 / 제안서 복사] [⏭️ 패스하기] 버튼
4. [수주 승인 / 제안서 복사] 클릭 시
     (1) 공고가 이미 마감(Closed/Assigned)됐는지 확인 → 마감이면 알림창을 "자동 패스"로 수정하고 버튼 제거
     (2) 살아 있으면 파이썬 소스 코드를 생성하고, 알림에 있던 제안서와 함께
         ```text / ```python 코드 블록으로 각각 전송 (스마트폰에서 꾹 눌러 복사)
5. 중복 알림 방지(SQLite에 공고 링크 해시 기록) + 공고 고유번호(ID)로 버튼/결과물 1:1 매칭
6. 자연어 현황 대화: "오늘 몇 개 수집됐어?", "현황 알려줘" 처럼 물어보면 통계 기반으로 답변

※ 브라우저 자동 로그인/자동 제출(Selenium/Playwright)은 넣지 않음 → 계정 정지 위험 차단. 제출은 사람이 직접 복사·붙여넣기.
※ AI가 만든 코드는 서버에서 실행하지 않음. 문법 검사(ast)만 수행.

[필수 환경 변수]  (괄호 안은 이전 이름 — 둘 중 아무거나 써도 됨)
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID   (TELEGRAM_ADMIN_CHAT_ID)
  CLAUDE_API_KEY     (ANTHROPIC_API_KEY)
  RSS 주소 최소 하나 (쉼표로 여러 개):
    UPWORK_RSS_URLS / FIVERR_RSS_URLS
    EXTRA_RSS_URLS    "라벨|URL,라벨|URL"

[선택 환경 변수]
  ANTHROPIC_MODEL         기본 claude-sonnet-5-5  (제안서/코드 생성용)
  FILTER_MODEL            공고 판정용 모델. 비워두면 ANTHROPIC_MODEL 사용 (더 저렴한 모델로 바꾸면 비용 절감)
  POLL_INTERVAL_SEC       기본 300
  MAX_ANALYZE_PER_CYCLE   기본 15   (한 주기에 Claude로 분석할 최대 공고 수, 비용 제어)
  MAX_EST_HOURS           기본 3    (이 시간을 넘는 작업은 제외)
  ALLOW_UNKNOWN_PAYMENT   기본 false (true면 결제 방식이 공고에 안 적힌 경우도 통과. 시간제로 확인된 건 항상 제외)
  EFF_HIGH_USD_PER_HR     기본 80   (환산 시급이 이 이상이면 '초고효율')
  EFF_MIN_USD_PER_HR      기본 30   (환산 시급이 이 미만이면 '비추천')
  SEED_ON_FIRST_RUN       기본 true
  DB_PATH                 기본 proposal_bot.db (Render 영구 디스크: /data/proposal_bot.db)
  PROPOSAL_PROFILE        영문 제안서에 쓸 스튜디오 소개
  PORT                    Render가 자동 주입

[requirements.txt]
  python-telegram-bot[job-queue]>=21.0
  feedparser>=6.0
  anthropic>=0.40
"""

import ast
import asyncio
import hashlib
import html
import io
import json
import logging
import os
import re
import sqlite3
import sys
import threading
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import feedparser
from anthropic import AsyncAnthropic
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest, Conflict
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ----------------------------------------------------------------------------
# 로깅 / 환경 변수
# ----------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)  # 토큰이 URL로 로그에 찍히는 것 방지
log = logging.getLogger("proposal-bot")


def env_first(*names: str) -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


def require_any(*names: str) -> str:
    value = env_first(*names)
    if not value:
        log.error("환경 변수 %s 중 하나가 필요합니다.", " 또는 ".join(names))
        sys.exit(1)
    return value


def split_urls(name: str) -> list[str]:
    return [u.strip() for u in os.environ.get(name, "").split(",") if u.strip()]


TELEGRAM_BOT_TOKEN = require_any("TELEGRAM_BOT_TOKEN")
CLAUDE_API_KEY = require_any("CLAUDE_API_KEY", "ANTHROPIC_API_KEY")
try:
    ADMIN_CHAT_ID = int(require_any("TELEGRAM_CHAT_ID", "TELEGRAM_ADMIN_CHAT_ID"))
except ValueError:
    log.error("TELEGRAM_CHAT_ID 는 숫자여야 합니다.")
    sys.exit(1)


def build_feeds() -> dict[str, list[str]]:
    feeds: dict[str, list[str]] = {}
    for label, env_name in (("Upwork", "UPWORK_RSS_URLS"), ("Fiverr", "FIVERR_RSS_URLS")):
        for url in split_urls(env_name):
            feeds.setdefault(label, []).append(url)
    for pair in split_urls("EXTRA_RSS_URLS"):
        label, sep, url = pair.partition("|")
        if sep and label.strip() and url.strip():
            feeds.setdefault(label.strip(), []).append(url.strip())
    return feeds


FEEDS = build_feeds()
if not FEEDS:
    log.error("RSS 주소가 없습니다. UPWORK_RSS_URLS / FIVERR_RSS_URLS / EXTRA_RSS_URLS 중 하나를 설정하세요.")
    sys.exit(1)

MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5-5").strip() or "claude-sonnet-5-5"
FILTER_MODEL = os.environ.get("FILTER_MODEL", "").strip() or MODEL
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SEC", "300"))
MAX_ANALYZE_PER_CYCLE = int(os.environ.get("MAX_ANALYZE_PER_CYCLE", "15"))
MAX_EST_HOURS = float(os.environ.get("MAX_EST_HOURS", "3"))
ALLOW_UNKNOWN_PAYMENT = os.environ.get("ALLOW_UNKNOWN_PAYMENT", "false").lower() == "true"
EFF_HIGH_USD_PER_HR = float(os.environ.get("EFF_HIGH_USD_PER_HR", "80"))
EFF_MIN_USD_PER_HR = float(os.environ.get("EFF_MIN_USD_PER_HR", "30"))
SEED_ON_FIRST_RUN = os.environ.get("SEED_ON_FIRST_RUN", "true").lower() == "true"
DB_PATH = os.environ.get("DB_PATH", "proposal_bot.db")
PROFILE = os.environ.get(
    "PROPOSAL_PROFILE",
    "Twomin Studio is an indie developer studio that builds mobile and desktop applications. "
    "Strengths: practical, concise problem solving, clear communication, and fast delivery of small, "
    "well-scoped tasks.",
)
USER_AGENT = "Mozilla/5.0 (compatible; ProposalBot/4.0)"
KST = timezone(timedelta(hours=9))

claude = AsyncAnthropic(api_key=CLAUDE_API_KEY, timeout=180.0, max_retries=2)

STATE = {
    "started_at": None,
    "last_poll_at": None,
    "last_poll_alerts": 0,
    "last_poll_filtered": 0,
    "feed_errors": 0,
}
_poll_lock = asyncio.Lock()


def now_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def today_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


# ----------------------------------------------------------------------------
# SQLite — 중복 방지(seen) + 공고 고유번호 매핑(jobs)
# ----------------------------------------------------------------------------
_db_lock = threading.Lock()


@contextmanager
def db():
    with _db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _ensure_column(c: sqlite3.Connection, table: str, column: str, decl: str) -> None:
    cols = [r["name"] for r in c.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def init_db() -> None:
    with db() as c:
        c.execute(
            """CREATE TABLE IF NOT EXISTS seen (
                   uid TEXT PRIMARY KEY,
                   platform TEXT,
                   title TEXT,
                   link TEXT,
                   seeded INTEGER DEFAULT 0,
                   ts TEXT)"""
        )
        _ensure_column(c, "seen", "verdict", "TEXT")  # seeded / passed / filtered
        _ensure_column(c, "seen", "reason", "TEXT")  # 필터 제외 사유
        c.execute(
            """CREATE TABLE IF NOT EXISTS jobs (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   uid TEXT,
                   platform TEXT,
                   title TEXT,
                   link TEXT,
                   payment_type TEXT,
                   budget_usd REAL,
                   time_est TEXT,
                   summary_ko TEXT,
                   judgement_ko TEXT,
                   post_text TEXT,
                   status TEXT DEFAULT 'pending',
                   message_id INTEGER,
                   proposal TEXT,
                   code TEXT,
                   created_at TEXT,
                   decided_at TEXT)"""
        )
        _ensure_column(c, "jobs", "completion_pct", "INTEGER")
        _ensure_column(c, "jobs", "review_minutes", "INTEGER")
        _ensure_column(c, "jobs", "tech_stack", "TEXT")
        # 재시작 도중 '처리 중'에 멈춘 공고는 다시 승인할 수 있도록 되돌림
        c.execute("UPDATE jobs SET status='pending' WHERE status='processing'")


def seen_count() -> int:
    with db() as c:
        return c.execute("SELECT COUNT(*) FROM seen").fetchone()[0]


def is_seen(uid: str) -> bool:
    with db() as c:
        return c.execute("SELECT 1 FROM seen WHERE uid=?", (uid,)).fetchone() is not None


def mark_seen(uid: str, platform: str, title: str, link: str, verdict: str, reason: str = "") -> None:
    with db() as c:
        c.execute(
            "INSERT OR IGNORE INTO seen(uid,platform,title,link,seeded,ts,verdict,reason) VALUES (?,?,?,?,?,?,?,?)",
            (uid, platform, title, link, 1 if verdict == "seeded" else 0, now_str(), verdict, reason),
        )


def insert_job(uid: str, platform: str, title: str, link: str, a: dict, post_text: str, proposal: str) -> int:
    pct = to_float(a.get("completion_pct"))
    mins = to_float(a.get("review_minutes"))
    with db() as c:
        cur = c.execute(
            """INSERT INTO jobs(uid,platform,title,link,payment_type,budget_usd,summary_ko,judgement_ko,
                                post_text,proposal,completion_pct,review_minutes,tech_stack,status,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, 'pending', ?)""",
            (
                uid,
                platform,
                title,
                link,
                str(a.get("payment_type", "unknown")).lower(),
                to_float(a.get("budget_usd")),
                str(a.get("core_requirement_ko") or "").strip(),
                str(a.get("risk_ko") or "").strip(),
                post_text,
                proposal,
                int(max(0, min(100, pct))) if pct is not None else None,
                int(max(5, min(180, mins))) if mins is not None else None,
                str(a.get("tech_stack") or "").strip()[:200],
                now_str(),
            ),
        )
        return cur.lastrowid


def get_job(job_id: int) -> dict | None:
    with db() as c:
        row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return dict(row) if row else None


def delete_job(job_id: int) -> None:
    with db() as c:
        c.execute("DELETE FROM jobs WHERE id=?", (job_id,))


def set_message_id(job_id: int, message_id: int) -> None:
    with db() as c:
        c.execute("UPDATE jobs SET message_id=? WHERE id=?", (message_id, job_id))


def claim_job(job_id: int, from_status: str, to_status: str) -> bool:
    """상태를 원자적으로 전환. 동시에 두 번 눌러도 한 번만 성공함 → 결과물 중복/뒤바뀜 방지"""
    with db() as c:
        cur = c.execute(
            "UPDATE jobs SET status=?, decided_at=? WHERE id=? AND status=?",
            (to_status, now_str(), job_id, from_status),
        )
        return cur.rowcount == 1


def set_job_status(job_id: int, status: str) -> None:
    with db() as c:
        c.execute("UPDATE jobs SET status=?, decided_at=? WHERE id=?", (status, now_str(), job_id))


def save_code(job_id: int, code: str) -> None:
    with db() as c:
        c.execute(
            "UPDATE jobs SET code=?, status='approved', decided_at=? WHERE id=?",
            (code, now_str(), job_id),
        )


def get_stats() -> dict:
    today = today_str()
    with db() as c:

        def q(sql: str, args: tuple = ()) -> int:
            return c.execute(sql, args).fetchone()[0]

        collected_total = q("SELECT COUNT(*) FROM seen WHERE seeded=0")
        collected_today = q("SELECT COUNT(*) FROM seen WHERE seeded=0 AND substr(ts,1,10)=?", (today,))
        baseline = q("SELECT COUNT(*) FROM seen WHERE seeded=1")
        filtered_total = q("SELECT COUNT(*) FROM seen WHERE verdict='filtered'")
        filtered_today = q("SELECT COUNT(*) FROM seen WHERE verdict='filtered' AND substr(ts,1,10)=?", (today,))
        passed_total = q("SELECT COUNT(*) FROM jobs")
        passed_today = q("SELECT COUNT(*) FROM jobs WHERE substr(created_at,1,10)=?", (today,))
        approved_today = q(
            "SELECT COUNT(*) FROM jobs WHERE status='approved' AND substr(decided_at,1,10)=?", (today,)
        )
        rejected_today = q(
            "SELECT COUNT(*) FROM jobs WHERE status='rejected' AND substr(decided_at,1,10)=?", (today,)
        )
        closed_today = q(
            "SELECT COUNT(*) FROM jobs WHERE status='closed' AND substr(decided_at,1,10)=?", (today,)
        )
        status_all = {r["status"]: r["n"] for r in c.execute("SELECT status, COUNT(*) n FROM jobs GROUP BY status")}
        reasons = {
            r["reason"]: r["n"]
            for r in c.execute(
                "SELECT reason, COUNT(*) n FROM seen WHERE verdict='filtered' GROUP BY reason ORDER BY n DESC LIMIT 6"
            )
        }
        recent = [
            dict(r)
            for r in c.execute(
                "SELECT id, title, status, budget_usd, created_at FROM jobs ORDER BY id DESC LIMIT 5"
            )
        ]

    return {
        "현재시각(KST)": now_str(),
        "오늘날짜(KST)": today,
        "신규 수집(오늘)": collected_today,
        "신규 수집(누적)": collected_total,
        "최초 실행 시 기준점으로만 저장된 기존 공고 수": baseline,
        "필터 통과해서 알림 보낸 공고(오늘)": passed_today,
        "필터 통과해서 알림 보낸 공고(누적)": passed_total,
        "필터에서 제외된 공고(오늘)": filtered_today,
        "필터에서 제외된 공고(누적)": filtered_total,
        "제외 사유별 건수(누적)": reasons,
        "상태별 알림(누적)": {
            "대기중": status_all.get("pending", 0) + status_all.get("processing", 0),
            "승인": status_all.get("approved", 0),
            "거절": status_all.get("rejected", 0),
            "마감으로 자동 패스": status_all.get("closed", 0),
        },
        "오늘 승인": approved_today,
        "오늘 거절": rejected_today,
        "오늘 마감으로 자동 패스": closed_today,
        "최근 알림 5건": recent,
        "봇 시작 시각": STATE["started_at"],
        "마지막 수집 시각": STATE["last_poll_at"],
        "마지막 수집에서 알림 보낸 건수": STATE["last_poll_alerts"],
        "마지막 수집에서 제외한 건수": STATE["last_poll_filtered"],
        "피드 수집 오류 누적 횟수": STATE["feed_errors"],
        "수집 주기(초)": POLL_INTERVAL,
        "구독 중인 피드": {k: len(v) for k, v in FEEDS.items()},
    }


# ----------------------------------------------------------------------------
# RSS 수집
# ----------------------------------------------------------------------------
def clean_html(text: str, limit: int = 3000) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def make_uid(entry) -> str:
    # 링크를 우선 기준으로 삼아 피드가 달라도 같은 공고는 같은 ID가 되게 함
    raw = (entry.get("link") or entry.get("id") or entry.get("title", "")).strip()
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def fetch_feed_sync(url: str) -> list[dict]:
    parsed = feedparser.parse(url, agent=USER_AGENT)
    if parsed.bozo and not parsed.entries:
        raise RuntimeError(f"RSS 파싱 실패: {parsed.get('bozo_exception')}")
    items = []
    for e in parsed.entries:
        items.append(
            {
                "uid": make_uid(e),
                "title": clean_html(e.get("title", "(제목 없음)"), 200),
                "link": e.get("link", ""),
                "summary": clean_html(e.get("summary", e.get("description", ""))),
            }
        )
    return items


# ----------------------------------------------------------------------------
# Claude 호출 공통
# ----------------------------------------------------------------------------
async def call_claude(system: str, user: str, max_tokens: int, model: str | None = None) -> tuple[str, str]:
    resp = await claude.messages.create(
        model=model or MODEL,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    text = "".join(b.text for b in resp.content if b.type == "text").strip()
    return text, (resp.stop_reason or "")


def to_float(v) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


# ----------------------------------------------------------------------------
# 1단계: 엄격한 일감 필터 (Claude 판정 + 코드 강제 검사)
# ----------------------------------------------------------------------------
FILTER_SYSTEM = (
    "You are the chief architect of a one-person outsourcing business. You screen freelance job posts for ONE "
    "solo developer who delivers code written by an AI (Claude). Be cold, practical and strict. "
    "Judge only from the post text; never guess details that are not there. "
    "Output ONLY one JSON object (no markdown, no commentary) with exactly these keys:\n"
    "{\n"
    '  "payment_type": "fixed" | "hourly" | "unknown",\n'
    '  "budget_usd": number or null,\n'
    '  "is_coding_task": true | false,\n'
    '  "self_contained": true | false,\n'
    '  "estimated_hours": number or null,\n'
    '  "difficulty": "하" | "중" | "상",\n'
    '  "tech_stack": string,\n'
    '  "core_requirement_ko": string,\n'
    '  "completion_pct": integer,\n'
    '  "review_minutes": integer,\n'
    '  "risk_ko": string\n'
    "}\n"
    "Rules:\n"
    '- payment_type "fixed" ONLY if the post explicitly shows a fixed / one-time price or fixed budget. '
    '"hourly" if it shows an hourly rate/range, per-hour pay, or time-based/ongoing pay. Otherwise "unknown".\n'
    "- budget_usd: the fixed price in USD if clearly stated (convert only if the currency is obvious), else null.\n"
    "- is_coding_task: true ONLY for pure code implementation, API integration or debugging. false for odd jobs, "
    "simple work-from-home gigs, customer support, data entry, translation, design, blog/content writing, "
    "virtual assistant, sales, marketing, or anything non-development.\n"
    "- self_contained: true only if one Python script/file can complete it without access to the client's private "
    "systems, long back-and-forth, new accounts, servers, or large existing codebases.\n"
    "- estimated_hours: realistic hours for an experienced developer using AI code generation; null if unclear.\n"
    '- difficulty: "하" = tiny and clearly specified (single script, one clear goal); "중"/"상" = bigger or vague.\n'
    "- tech_stack: languages/libraries/APIs named in the post, comma-separated; empty string if none.\n"
    "- core_requirement_ko: ONE line in Korean summarizing the core requirement.\n"
    "- completion_pct: 0-100, your honest estimate of how complete a first delivery of working code would be "
    "given how clear the requirements are (vague requirements = lower).\n"
    "- review_minutes: minutes the owner must spend reviewing/testing/communicating (10-60 for clear tasks; "
    "more when requirements are unclear or testing needs the client's data). AI generation time is negligible "
    "and must NOT be counted.\n"
    "- risk_ko: ONE line in Korean with the main risk or edge case; write '없음' if there is none."
)

HOURLY_RE = re.compile(r"\bhourly\b|/\s?hr\b|per\s+hour|\bhourly\s+rate\b", re.I)
FIXED_RE = re.compile(r"fixed[\s-]?price|fixed\s+budget|\bbudget\b", re.I)


def looks_hourly(text: str) -> bool:
    """API 비용 절감용 1차 규칙 필터: 시간제 신호만 있고 고정가 신호가 없으면 바로 제외"""
    return bool(HOURLY_RE.search(text)) and not FIXED_RE.search(text)


def parse_json_obj(text: str) -> dict:
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        raise ValueError("JSON 없음")
    obj = json.loads(m.group(0))
    if not isinstance(obj, dict):
        raise ValueError("JSON 객체 아님")
    return obj


def passes_filter(a: dict) -> tuple[bool, str]:
    """Claude의 판정을 믿기만 하지 않고, 코드에서 조건을 한 번 더 강제"""
    payment = str(a.get("payment_type", "unknown")).lower()
    if payment == "hourly":
        return False, "시간제(Hourly)"
    if payment != "fixed" and not (payment == "unknown" and ALLOW_UNKNOWN_PAYMENT):
        return False, "건당 고정가 확인 불가"
    if a.get("is_coding_task") is not True:
        return False, "개발 작업 아님(CS/비개발)"
    if a.get("self_contained") is not True:
        return False, "코드 1개로 끝나지 않는 작업"
    hours = to_float(a.get("estimated_hours"))
    if hours is None or hours <= 0 or hours > MAX_EST_HOURS:
        return False, f"예상 {MAX_EST_HOURS:g}시간 초과/불명확"
    if a.get("difficulty") != "하":
        return False, "난이도 '하' 아님"
    return True, ""


async def analyze_job(platform: str, title: str, summary: str, link: str) -> dict:
    user = f"Platform: {platform}\nTitle: {title}\nLink: {link}\n\nPost text:\n{summary or '(no description)'}"
    text, _ = await call_claude(FILTER_SYSTEM, user, 900, FILTER_MODEL)
    return parse_json_obj(text)


# ----------------------------------------------------------------------------
# 2단계: 알림 메시지 (스마트폰 UI)
# ----------------------------------------------------------------------------
def fmt_num(x: float) -> str:
    return f"{x:,.2f}".rstrip("0").rstrip(".")


def efficiency_text(budget_usd: float | None, review_min: int | None) -> str:
    """예산 대비 '사장님 검토 시간' 기준 환산 시급으로 효율 평가 (AI 생성 시간은 무시)"""
    if not budget_usd:
        return "판단 불가 (예산 미표기)"
    minutes = max(5, min(180, int(review_min or 30)))
    rate = budget_usd / (minutes / 60)
    if rate >= EFF_HIGH_USD_PER_HR:
        label = "초고효율"
    elif rate >= EFF_MIN_USD_PER_HR:
        label = "보통"
    else:
        label = "비추천"
    return f"{label} (검토 약 {minutes}분 기준 시급 환산 약 ${rate:,.0f})"


def build_alert(job: dict) -> str:
    usd = job.get("budget_usd")
    tag = "(Fixed-Price)" if job["payment_type"] == "fixed" else "(결제 방식 미확인)"
    budget = f"${fmt_num(usd)} {tag}" if usd else f"미표기 {tag}"
    pct = job.get("completion_pct")
    pct_txt = f"{pct}%" if pct is not None else "판단 불가"
    text = (
        f"🚨 [개발 외주 꿀알감 포착] · 공고 #{job['id']}\n"
        f"\n"
        f"1. 🎯 프로젝트 요약\n"
        f"- 제목: {job['title']}\n"
        f"- 예산: {budget}\n"
        f"- 핵심 요구사항: {job['summary_ko'] or '(요약 없음)'}\n"
        f"\n"
        f"2. ⏱️ 시간 및 비용 분석\n"
        f"- AI 코드 생성 소요 시간: 약 1~2분 (Claude API 즉시 발행 가능)\n"
        f"- 예상 수익 대비 효율: {efficiency_text(usd, job.get('review_minutes'))}\n"
        f"\n"
        f"3. 🛠️ 구현 가능성 및 리스크 진단\n"
        f"- 구현 완성도 예측: {pct_txt}\n"
        f"- 잠재적 위험 요소 / 엣지 케이스: {job['judgement_ko'] or '없음'}\n"
        f"\n"
        f"4. 📝 원클릭 맞춤 제안서 초안\n"
        f"{job['proposal'] or '(제안서 없음)'}\n"
        f"\n"
        f"👉 [수주 승인 / 제안서 복사] | [패스하기]\n"
        f"🔗 {job['link']}"
    )
    return text[:4000]


def keyboard(job_id: int) -> InlineKeyboardMarkup:
    # 라벨이 길어서 한 줄에 두 개를 넣으면 폰에서 잘리므로 두 줄로 배치
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ 수주 승인 / 제안서 복사", callback_data=f"ok:{job_id}")],
            [InlineKeyboardButton("⏭️ 패스하기", callback_data=f"no:{job_id}")],
        ]
    )


async def safe_edit(query, text: str, reply_markup=None) -> None:
    try:
        await query.edit_message_text(text=text[:4000], reply_markup=reply_markup, disable_web_page_preview=True)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            log.warning("메시지 수정 실패: %s", e)


# ----------------------------------------------------------------------------
# 3단계: 승인 시 — 마감 여부 확인 → 영문 제안서 + 파이썬 코드 생성
# ----------------------------------------------------------------------------
CLOSED_MARKERS = [
    "no longer available",
    "job is closed",
    "job has been closed",
    "this job has been closed",
    "no longer accepting",
    "has been filled",
    "already been filled",
    "has been assigned",
    "job was removed",
    "this job is no longer",
    "page not found",
]


def check_job_open_sync(url: str) -> tuple[str, str]:
    """('open'|'closed'|'unknown', 근거). 공개 페이지를 한 번 읽어 마감 신호만 찾는 '최선 노력' 확인."""
    if not url:
        return "unknown", "링크 없음"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept-Language": "en"})
    try:
        with urllib.request.urlopen(req, timeout=12) as r:
            body = r.read(400_000).decode("utf-8", errors="ignore").lower()
    except urllib.error.HTTPError as e:
        if e.code in (404, 410):
            return "closed", f"HTTP {e.code}"
        return "unknown", f"HTTP {e.code}"
    except Exception as e:  # 네트워크/타임아웃 등
        return "unknown", str(e)[:80]
    for marker in CLOSED_MARKERS:
        if marker in body:
            return "closed", marker
    return "open", "마감 신호 없음"


def job_context(job: dict) -> str:
    usd = job.get("budget_usd")
    return (
        f"Platform: {job['platform']}\nJob title: {job['title']}\nLink: {job['link']}\n"
        f"Fixed budget (USD): {fmt_num(usd) if usd else 'not stated'}\n"
        f"Tech stack mentioned: {job.get('tech_stack') or 'not stated'}\n"
        f"\nJob post text:\n{job['post_text'] or '(no description)'}"
    )


def proposal_system(platform: str) -> str:
    return (
        f"You write the opening proposal for a fixed-price {platform} job on behalf of Twomin Studio, "
        "an indie developer studio. Write exactly 3-4 sentences in English, plain text only. "
        "Sentence 1 names the client's specific pain point or goal taken from the post. "
        "The next 1-2 sentences explain how Twomin Studio will solve it quickly and cleanly "
        "(concrete deliverable, concise approach). The last sentence gives a short delivery estimate "
        "or one clarifying question. Tone: confident, calm, results-focused, not salesy. "
        "Never use words like 'perfect' or '100%', never promise guaranteed results, never invent credentials, "
        "clients, reviews or numbers, and never claim the work is already done or tested. "
        f"Studio profile: {PROFILE}\n"
        "Output ONLY the proposal text."
    )

CODE_SYSTEM = (
    "You are a senior Python engineer. Write the complete, ready-to-deliver solution for the freelance job below "
    "as ONE self-contained Python 3 file. Requirements: "
    "start with a comment block listing the pip install command, how to run it, and any assumptions you made "
    "where the post is ambiguous; use well-known libraries only; read any secrets from environment variables "
    "(never hard-code them); include a main() entry point, input validation and basic error handling; "
    "no placeholders or TODOs that would stop the script from running; Korean or English comments are fine. "
    "Output ONLY the code in a single ```python block, with no text before or after it."
)


def extract_code(text: str) -> str:
    m = re.search(r"```(?:python|py)?[ \t]*\n(.*?)```", text, re.S)
    if m:
        return m.group(1).strip()
    code = re.sub(r"^```(?:python|py)?[ \t]*\n", "", text.strip())  # 닫는 펜스 없이 잘린 경우
    return re.sub(r"\n```\s*$", "", code).strip()


def syntax_error(code: str) -> str:
    if not code.strip():
        return "코드가 비어 있음"
    try:
        ast.parse(code)
        return ""
    except SyntaxError as e:
        return f"line {e.lineno}: {e.msg}"


async def generate_proposal_short(platform: str, title: str, description: str, a: dict) -> str:
    usd = to_float(a.get("budget_usd"))
    user = (
        f"Platform: {platform}\nJob title: {title}\n"
        f"Fixed budget (USD): {fmt_num(usd) if usd else 'not stated'}\n"
        f"Tech stack mentioned: {a.get('tech_stack') or 'not stated'}\n"
        f"Core requirement (Korean note): {a.get('core_requirement_ko') or ''}\n\n"
        f"Job post text:\n{description or '(no description)'}"
    )
    text, _ = await call_claude(proposal_system(platform), user, 400)
    return text.strip()


async def generate_code(job: dict) -> tuple[str, str]:
    """(코드, 경고문). 문법 오류/잘림이 있으면 1회 자동 수정 시도. 코드를 실행하지는 않음."""
    text, stop = await call_claude(CODE_SYSTEM, job_context(job), 6000)
    code = extract_code(text)
    err = syntax_error(code)
    if not err and stop != "max_tokens":
        return code, ""

    reason = err or "응답이 길이 제한으로 잘림"
    repair = (
        f"{job_context(job)}\n\nThe previous attempt failed ({reason}). "
        "Return the COMPLETE corrected file, and keep it compact enough to finish.\n"
        f"Previous attempt:\n```python\n{code}\n```"
    )
    text2, stop2 = await call_claude(CODE_SYSTEM, repair, 6000)
    code2 = extract_code(text2)
    err2 = syntax_error(code2)
    if not err2 and stop2 != "max_tokens":
        return code2, ""
    best = code2 if code2.strip() else code
    return best, f"⚠️ 자동 문법 검사 실패({err2 or err or '잘림'}) — 실행 전 꼭 확인하세요."


# 마크다운(MarkdownV2) 전송 유틸 — 코드 블록 안에서는 ` 와 \ 만 이스케이프
def esc_code(s: str) -> str:
    return s.replace("\\", "\\\\").replace("`", "\\`")


def esc_md(s: str) -> str:
    return re.sub(r"([_*\[\]()~`>#+\-=|{}.!\\])", r"\\\1", s)


def split_for_code(text: str, limit: int = 3500) -> list[str]:
    lines: list[str] = []
    for line in text.split("\n"):
        while len(line) > 1000:
            lines.append(line[:1000])
            line = line[1000:]
        lines.append(line)
    chunks: list[str] = []
    cur: list[str] = []
    size = 0
    for line in lines:
        n = len(esc_code(line)) + 1
        if cur and size + n > limit:
            chunks.append("\n".join(cur))
            cur, size = [], 0
        cur.append(line)
        size += n
    if cur:
        chunks.append("\n".join(cur))
    return chunks or [""]


async def send_code_blocks(bot, lang: str, title: str, text: str) -> int:
    chunks = split_for_code(text)
    for i, chunk in enumerate(chunks, 1):
        label = title if len(chunks) == 1 else f"{title} ({i}/{len(chunks)})"
        md = f"{esc_md(label)}\n```{lang}\n{esc_code(chunk)}\n```"
        try:
            await bot.send_message(chat_id=ADMIN_CHAT_ID, text=md, parse_mode=ParseMode.MARKDOWN_V2)
        except BadRequest:
            await bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"{label}\n```{lang}\n{chunk}\n```"[:4096])
    return len(chunks)


async def process_approval(query, context: ContextTypes.DEFAULT_TYPE, job: dict) -> None:
    job_id = job["id"]
    alert = build_alert(job)

    # (1) 실시간 마감 여부 확인
    await safe_edit(query, alert + "\n\n⏳ 마감 여부 확인 중...")
    state, detail = await asyncio.to_thread(check_job_open_sync, job["link"])
    if state == "closed":
        await asyncio.to_thread(set_job_status, job_id, "closed")
        await safe_edit(
            query,
            f"🚫 [공고 #{job_id}] 이미 다른 곳에 컨펌되어 마감된 공고입니다. (자동 패스)",
        )
        log.info("공고 #%s 마감 감지(%s) → 자동 패스", job_id, detail)
        return

    # (2) 살아 있는 공고 → 파이썬 소스 코드 생성 (제안서는 사장님이 본 알림의 초안을 그대로 사용)
    await safe_edit(query, alert + "\n\n⏳ 파이썬 코드를 만드는 중... (최대 1~2분)")
    try:
        code, warn = await generate_code(job)
    except Exception:
        log.exception("공고 #%s 코드 생성 실패", job_id)
        await asyncio.to_thread(set_job_status, job_id, "pending")
        await safe_edit(
            query,
            alert + "\n\n⚠️ 코드 생성에 실패했어요. 잠시 후 [수주 승인]을 다시 눌러주세요.",
            reply_markup=keyboard(job_id),
        )
        return

    await asyncio.to_thread(save_code, job_id, code)
    note = "" if state == "open" else f" (마감 여부는 확인하지 못했어요: {detail})"
    await safe_edit(query, alert + f"\n\n✅ 수주 승인 — 아래 제안서와 코드를 복사하세요.{note}")

    # (3) 스마트폰 복사용 코드 블록으로 전송 (제출은 사람이 직접)
    bot = context.bot
    header = f"📋 [공고 #{job_id}] 제출용 결과물 — 코드 블록을 길게 눌러 복사하세요"
    if state == "open":
        header += "\n(마감 신호는 감지되지 않았지만, 자동 확인은 완전하지 않아요. 제출 전 공고 상태를 한 번 확인하세요.)"
    if warn:
        header += f"\n{warn}"
    await bot.send_message(chat_id=ADMIN_CHAT_ID, text=header, disable_web_page_preview=True)
    await send_code_blocks(bot, "text", f"📝 [공고 #{job_id}] 영문 제안서", job["proposal"] or "")
    parts = await send_code_blocks(bot, "python", f"💻 [공고 #{job_id}] 파이썬 소스 코드", code)
    if parts > 1:
        await bot.send_document(
            chat_id=ADMIN_CHAT_ID,
            document=io.BytesIO(code.encode("utf-8")),
            filename=f"job_{job_id}_solution.py",
            caption=f"📎 [공고 #{job_id}] 코드가 길어 여러 메시지로 나뉘어서 파일로도 보내요.",
        )
    await bot.send_message(
        chat_id=ADMIN_CHAT_ID,
        text=f"⚠️ [공고 #{job_id}] AI가 만든 코드는 서버에서 실행해보지 않았어요. 납품 전에 직접 실행해서 확인하세요.",
    )


# ----------------------------------------------------------------------------
# 수집 작업 (주기 실행)
# ----------------------------------------------------------------------------
async def poll_feeds(context: ContextTypes.DEFAULT_TYPE) -> dict:
    result = {"alerts": 0, "filtered": 0, "skipped": False}
    if _poll_lock.locked():
        log.info("이전 수집 작업이 아직 진행 중이라 건너뜁니다.")
        result["skipped"] = True
        return result

    async with _poll_lock:
        first_run = SEED_ON_FIRST_RUN and await asyncio.to_thread(seen_count) == 0
        analyzed = 0

        for platform, urls in FEEDS.items():
            for url in urls:
                try:
                    items = await asyncio.to_thread(fetch_feed_sync, url)
                except Exception as e:
                    STATE["feed_errors"] += 1
                    log.warning("피드 수집 오류 (%s): %s", url, e)
                    continue

                for item in items:
                    uid = item["uid"]
                    if await asyncio.to_thread(is_seen, uid):
                        continue

                    if first_run:  # 최초 실행: 기존 공고는 알림 없이 기록만
                        await asyncio.to_thread(mark_seen, uid, platform, item["title"], item["link"], "seeded")
                        continue

                    # 1차 규칙 필터 (API 호출 없이 시간제 제외)
                    if looks_hourly(f"{item['title']} {item['summary']}"):
                        await asyncio.to_thread(
                            mark_seen, uid, platform, item["title"], item["link"], "filtered", "시간제(Hourly)"
                        )
                        result["filtered"] += 1
                        continue

                    if analyzed >= MAX_ANALYZE_PER_CYCLE:
                        continue  # 다음 주기에 분석 (seen 기록 안 함)
                    analyzed += 1

                    # 2차 Claude 분석 + 코드 강제 검사
                    try:
                        analysis = await analyze_job(platform, item["title"], item["summary"], item["link"])
                    except (ValueError, KeyError, TypeError):
                        await asyncio.to_thread(
                            mark_seen, uid, platform, item["title"], item["link"], "filtered", "분석 결과 해석 실패"
                        )
                        result["filtered"] += 1
                        continue
                    except Exception:
                        log.exception("공고 분석 실패(다음 주기에 재시도): %s", item["title"])
                        continue

                    ok, reason = passes_filter(analysis)
                    if not ok:
                        await asyncio.to_thread(
                            mark_seen, uid, platform, item["title"], item["link"], "filtered", reason
                        )
                        result["filtered"] += 1
                        continue

                    # 통과 → 영문 제안서 초안(3~4문장) 생성 → 고유번호 부여 후 알림
                    try:
                        proposal = await generate_proposal_short(platform, item["title"], item["summary"], analysis)
                    except Exception:
                        log.exception("제안서 초안 생성 실패(다음 주기에 재시도): %s", item["title"])
                        continue
                    job_id = await asyncio.to_thread(
                        insert_job, uid, platform, item["title"], item["link"], analysis, item["summary"], proposal
                    )
                    job = await asyncio.to_thread(get_job, job_id)
                    try:
                        msg = await context.bot.send_message(
                            chat_id=ADMIN_CHAT_ID,
                            text=build_alert(job),
                            reply_markup=keyboard(job_id),
                            disable_web_page_preview=True,
                        )
                    except Exception:
                        log.exception("알림 전송 실패(다음 주기에 재시도): %s", item["title"])
                        await asyncio.to_thread(delete_job, job_id)
                        continue
                    await asyncio.to_thread(set_message_id, job_id, msg.message_id)
                    await asyncio.to_thread(mark_seen, uid, platform, item["title"], item["link"], "passed")
                    result["alerts"] += 1

        STATE["last_poll_at"] = now_str()
        STATE["last_poll_alerts"] = result["alerts"]
        STATE["last_poll_filtered"] = result["filtered"]
        if first_run:
            log.info("최초 실행: 기존 공고를 기준점으로 저장했습니다. 이후 신규 공고만 분석합니다.")
        else:
            log.info("수집 완료: 알림 %d건 / 필터 제외 %d건 / 분석 %d건", result["alerts"], result["filtered"], analyzed)
        return result


async def poll_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    await poll_feeds(context)


# ----------------------------------------------------------------------------
# 텔레그램 핸들러
# ----------------------------------------------------------------------------
async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    if not query.message or query.message.chat.id != ADMIN_CHAT_ID:
        await query.answer("권한이 없습니다.", show_alert=True)
        return

    action, _, raw_id = (query.data or "").partition(":")
    if not raw_id.isdigit():
        await query.answer()
        return
    job_id = int(raw_id)

    job = await asyncio.to_thread(get_job, job_id)
    if not job:
        await query.answer("공고를 찾을 수 없어요.", show_alert=True)
        return
    # 무결성: 이 버튼이 달린 메시지가 해당 공고의 알림 메시지가 맞는지 확인
    if job["message_id"] and query.message.message_id != job["message_id"]:
        await query.answer("알림과 공고 번호가 일치하지 않아요.", show_alert=True)
        return

    if action == "no":
        if not await asyncio.to_thread(claim_job, job_id, "pending", "rejected"):
            await query.answer(f"이미 처리된 공고예요 ({job['status']})")
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            return
        await query.answer("패스 처리")
        await safe_edit(query, f"⏭️ [공고 #{job_id}] 패스함 — {job['title']}")
    elif action == "ok":
        if not await asyncio.to_thread(claim_job, job_id, "pending", "processing"):
            await query.answer(f"이미 처리 중이거나 처리된 공고예요 ({job['status']})")
            return
        await query.answer("마감 여부를 확인하고 있어요...")
        try:
            await process_approval(query, context, job)
        except Exception:
            log.exception("공고 #%s 승인 처리 중 오류", job_id)
            await asyncio.to_thread(set_job_status, job_id, "pending")
            await safe_edit(
                query,
                build_alert(job) + "\n\n⚠️ 처리 중 오류가 났어요. [수주 승인]을 다시 눌러주세요.",
                reply_markup=keyboard(job_id),
            )
    else:
        await query.answer()


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🤖 외주 비서 봇 가동 중이에요.\n"
        f"모델: {MODEL}\n수집 주기: {POLL_INTERVAL}초\n"
        f"필터: 건당 고정가 + 예상 {MAX_EST_HOURS:g}시간 이내 + 난이도 '하'만 통과\n\n"
        "• /check — 지금 바로 공고 확인\n"
        "• 그냥 \"오늘 몇 개 수집됐어?\", \"현황 알려줘\" 처럼 말을 걸어도 돼요."
    )


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _poll_lock.locked():
        await update.message.reply_text("⏳ 이미 수집이 진행 중이에요. 잠시 후 다시 시도해 주세요.")
        return
    await update.message.reply_text("🔎 공고 확인 중...")
    r = await poll_feeds(context)
    await update.message.reply_text(f"완료! 알림 {r['alerts']}건 / 필터 제외 {r['filtered']}건")


STATS_SYSTEM = (
    "너는 외주 공고 수집/제안 봇의 비서야. 사용자가 텔레그램에서 평소 말투로 현황을 물어보면, "
    "아래 [현재 데이터]에 있는 숫자와 사실만 사용해서 친절하고 간결한 한국어로 답해. "
    "데이터에 없는 내용은 지어내지 말고 '그건 아직 기록이 없어요'라고 말해. "
    "'신규 수집'은 봇이 새로 확인한 공고, '필터 통과'는 건당 고정가+초간단 작업 조건을 통과해 알림이 간 공고, "
    "'필터 제외'는 시간제/큰 작업/비개발 등으로 걸러진 공고라는 점을 필요할 때만 짧게 설명해. "
    "'기준점'은 최초 실행 때 알림 없이 저장만 한 기존 공고야. "
    "공고 현황과 전혀 무관한 질문이면 현황 관련 질문만 도와줄 수 있다고 안내해. "
    "답변은 6줄 이내, 이모지는 적당히."
)


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """명령어 없이 평소 말투로 묻는 질문 → DB 통계를 근거로 Claude가 자연어로 답변"""
    msg = update.message
    if not msg or not msg.text:
        return

    await context.bot.send_chat_action(chat_id=ADMIN_CHAT_ID, action=ChatAction.TYPING)
    stats = await asyncio.to_thread(get_stats)
    data = json.dumps(stats, ensure_ascii=False, indent=2)

    try:
        answer, _ = await call_claude(
            STATS_SYSTEM, f"[현재 데이터]\n{data}\n\n[사용자 질문]\n{msg.text}", 600, FILTER_MODEL
        )
        await msg.reply_text(answer[:4000] or "답변을 만들지 못했어요. 다시 물어봐 주세요.")
    except Exception:
        log.exception("자연어 답변 생성 실패")
        s = stats["상태별 알림(누적)"]
        await msg.reply_text(
            "⚠️ AI 답변 생성에 실패해서 숫자만 알려드릴게요.\n"
            f"• 오늘 신규 수집: {stats['신규 수집(오늘)']}건\n"
            f"• 오늘 알림 보낸 공고: {stats['필터 통과해서 알림 보낸 공고(오늘)']}건 / "
            f"필터 제외: {stats['필터에서 제외된 공고(오늘)']}건\n"
            f"• 대기/승인/거절/마감: {s['대기중']}/{s['승인']}/{s['거절']}/{s['마감으로 자동 패스']}"
        )


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    if isinstance(context.error, Conflict):
        log.warning(
            "Telegram Conflict: 같은 토큰으로 폴링하는 다른 인스턴스가 있습니다. "
            "(로컬 PC/다른 서비스/배포 겹침 확인)"
        )
        return
    log.error("핸들러 오류", exc_info=context.error)


# ----------------------------------------------------------------------------
# Render/Railway용 헬스체크 서버 ($PORT 바인딩) — 텔레그램은 폴링이라 포트와 충돌 없음
# ----------------------------------------------------------------------------
class HealthHandler(BaseHTTPRequestHandler):
    def _ok(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()

    def do_GET(self):
        self._ok()
        self.wfile.write(b"ok")

    def do_HEAD(self):
        self._ok()

    def log_message(self, *args):
        pass


def start_health_server() -> None:
    port = int(os.environ.get("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("헬스체크 서버 시작: 0.0.0.0:%d", port)


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
async def post_init(app: Application) -> None:
    STATE["started_at"] = now_str()
    try:
        await app.bot.send_message(chat_id=ADMIN_CHAT_ID, text="🚀 외주 비서 봇이 시작됐어요.")
    except Exception:
        log.exception("시작 알림 전송 실패 (TELEGRAM_CHAT_ID 확인, 봇에게 먼저 /start 필요)")


def main() -> None:
    init_db()
    start_health_server()

    admin_only = filters.Chat(chat_id=ADMIN_CHAT_ID)

    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .concurrent_updates(True)  # 제안서/코드 생성 중에도 다른 버튼·대화가 막히지 않게 함
        .post_init(post_init)
        .build()
    )
    app.add_handler(CommandHandler("start", cmd_start, filters=admin_only))
    app.add_handler(CommandHandler("check", cmd_check, filters=admin_only))
    app.add_handler(CallbackQueryHandler(on_button, pattern=r"^(ok|no):\d+$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & admin_only, on_text))
    app.add_error_handler(on_error)

    app.job_queue.run_repeating(poll_job, interval=POLL_INTERVAL, first=10, name="poll_feeds")

    log.info(
        "봇 시작 (polling) | 생성모델=%s | 필터모델=%s | 피드=%s",
        MODEL,
        FILTER_MODEL,
        {k: len(v) for k, v in FEEDS.items()},
    )
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,  # 재시작 시 밀린 업데이트로 인한 중복 처리 방지
    )


if __name__ == "__main__":
    main()
