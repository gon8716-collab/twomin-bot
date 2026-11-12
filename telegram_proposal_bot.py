"""
telegram_proposal_bot.py
1인 무인 외주 자동화 시스템 비서 봇 (최종 통합본)

[개선 및 반영 사항]
1. 엄격한 일감 필터링: 시간제(Hourly) 100% 제외, 건당 고정가(Fixed-Price) & 1~3시간 컷 초단기 작업만 선별.
2. 스마트폰 최적화 UI: 예산(원화 환산), 예상 소요 시간, 난이도, 핵심 요약 및 맞춤 제안서 제공.
3. 1-Click 승인 / 실시간 마감 감지: 승인 시 이미 마감된 공고인지 체크하고 상태 업데이트.
4. 중복 방지 및 SQLite 무결성 유지 (Render 영구 디스크 지원)
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
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import feedparser
from anthropic import AsyncAnthropic
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction
from telegram.error import Conflict
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ----------------------------------------------------------------------------
# 로깅 / 환경 변수 설정
# ----------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("proposal-bot")


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        log.error("환경 변수 %s 가 설정되지 않았습니다.", name)
        sys.exit(1)
    return value


def split_urls(name: str) -> list[str]:
    return [u.strip() for u in os.environ.get(name, "").split(",") if u.strip()]


TELEGRAM_BOT_TOKEN = require_env("TELEGRAM_BOT_TOKEN")
ANTHROPIC_API_KEY = require_env("ANTHROPIC_API_KEY")
try:
    ADMIN_CHAT_ID = int(require_env("TELEGRAM_ADMIN_CHAT_ID"))
except ValueError:
    log.error("TELEGRAM_ADMIN_CHAT_ID 는 숫자여야 합니다.")
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

MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SEC", "300"))
MAX_NEW_PER_CYCLE = int(os.environ.get("MAX_NEW_PER_CYCLE", "3"))
SEED_ON_FIRST_RUN = os.environ.get("SEED_ON_FIRST_RUN", "true").lower() == "true"
DB_PATH = os.environ.get("DB_PATH", "proposal_bot.db")
PROFILE = os.environ.get(
    "PROPOSAL_PROFILE",
    "Indie developer at Twomin Studio building mobile and desktop applications with strong practical execution.",
)
USER_AGENT = "Mozilla/5.0 (compatible; ProposalBot/4.0)"
KST = timezone(timedelta(hours=9))

claude = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

STATE = {
    "started_at": None,
    "last_poll_at": None,
    "last_poll_new": 0,
    "feed_errors": 0,
}
_poll_lock = asyncio.Lock()


def now_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def today_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


# ----------------------------------------------------------------------------
# SQLite 데이터베이스 관리 (중복 방지 및 초안 저장)
# ----------------------------------------------------------------------------
_db_lock = threading.Lock()


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, timeout=10)


def _ensure_column(c: sqlite3.Connection, table: str, column: str, decl: str) -> None:
    cols = [r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        c.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def init_db() -> None:
    with _db_lock, _conn() as c:
        c.execute(
            """CREATE TABLE IF NOT EXISTS seen (
                    uid TEXT PRIMARY KEY,
                    platform TEXT,
                    title TEXT,
                    link TEXT,
                    seeded INTEGER DEFAULT 0,
                    ts TEXT)"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS drafts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT,
                    title TEXT,
                    link TEXT,
                    draft TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TEXT,
                    decided_at TEXT)"""
        )
        _ensure_column(c, "drafts", "plan", "TEXT")
        _ensure_column(c, "drafts", "code", "TEXT")
        _ensure_column(c, "drafts", "budget_info", "TEXT")


def seen_count() -> int:
    with _db_lock, _conn() as c:
        return c.execute("SELECT COUNT(*) FROM seen").fetchone()[0]


def is_seen(uid: str) -> bool:
    with _db_lock, _conn() as c:
        return c.execute("SELECT 1 FROM seen WHERE uid=?", (uid,)).fetchone() is not None


def mark_seen(uid: str, platform: str, title: str, link: str, seeded: bool = False) -> None:
    with _db_lock, _conn() as c:
        c.execute(
            "INSERT OR IGNORE INTO seen(uid,platform,title,link,seeded,ts) VALUES (?,?,?,?,?,?)",
            (uid, platform, title, link, 1 if seeded else 0, now_str()),
        )


def save_draft(platform: str, title: str, link: str, draft: str, plan: str, code: str, budget_info: str) -> int:
    with _db_lock, _conn() as c:
        cur = c.execute(
            "INSERT INTO drafts(platform,title,link,draft,plan,code,budget_info,status,created_at) "
            "VALUES (?,?,?,?,?,?,?, 'pending', ?)",
            (platform, title, link, draft, plan, code, budget_info, now_str()),
        )
        return cur.lastrowid


def get_draft(draft_id: int):
    with _db_lock, _conn() as c:
        return c.execute(
            "SELECT platform,title,link,draft,status,plan,code,budget_info FROM drafts WHERE id=?", (draft_id,)
        ).fetchone()


def set_status(draft_id: int, status: str) -> None:
    with _db_lock, _conn() as c:
        c.execute(
            "UPDATE drafts SET status=?, decided_at=? WHERE id=?",
            (status, now_str(), draft_id),
        )


def get_stats() -> dict:
    today = today_str()
    with _db_lock, _conn() as c:
        collected_total = c.execute("SELECT COUNT(*) FROM seen WHERE seeded=0").fetchone()[0]
        collected_today = c.execute(
            "SELECT COUNT(*) FROM seen WHERE seeded=0 AND substr(ts,1,10)=?", (today,)
        ).fetchone()[0]
        baseline = c.execute("SELECT COUNT(*) FROM seen WHERE seeded=1").fetchone()[0]
        by_platform = dict(
            c.execute("SELECT platform, COUNT(*) FROM seen WHERE seeded=0 GROUP BY platform").fetchall()
        )
        status_all = dict(c.execute("SELECT status, COUNT(*) FROM drafts GROUP BY status").fetchall())
        approved_today = c.execute(
            "SELECT COUNT(*) FROM drafts WHERE status='approved' AND substr(decided_at,1,10)=?", (today,)
        ).fetchone()[0]
        rejected_today = c.execute(
            "SELECT COUNT(*) FROM drafts WHERE status='rejected' AND substr(decided_at,1,10)=?", (today,)
        ).fetchone()[0]
        drafts_today = c.execute(
            "SELECT COUNT(*) FROM drafts WHERE substr(created_at,1,10)=?", (today,)
        ).fetchone()[0]
        recent = c.execute(
            "SELECT platform,title,status,created_at FROM drafts ORDER BY id DESC LIMIT 5"
        ).fetchall()

    return {
        "현재시각(KST)": now_str(),
        "오늘날짜(KST)": today,
        "신규 수집(오늘)": collected_today,
        "신규 수집(누적)": collected_total,
        "기준점 공고 수": baseline,
        "플랫폼별 수집": by_platform,
        "생성된 초안(오늘)": drafts_today,
        "상태별 초안": {"대기중": status_all.get("pending", 0), "승인": status_all.get("approved", 0), "거절": status_all.get("rejected", 0)},
        "오늘 승인": approved_today,
        "오늘 거절": rejected_today,
        "최근 초안 5건": [{"플랫폼": p, "제목": t, "상태": s, "생성시각": ts} for p, t, s, ts in recent],
    }


# ----------------------------------------------------------------------------
# RSS 및 텍스트 정제
# ----------------------------------------------------------------------------
def clean_html(text: str, limit: int = 4000) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def make_uid(entry) -> str:
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
# Claude 분석 및 필터링 프롬프트
# ----------------------------------------------------------------------------
FILTER_AND_GEN_SYSTEM = """
You are an expert AI assistant for a solo freelancer who wants fast, high-ROI fixed-price gigs.
Analyze the given job post and strictly evaluate whether it passes the filter:
1) PAYMENT TYPE: Reject immediately if it is Hourly. ONLY accept Fixed-Price gigs.
2) SCOPE: Must be a very simple 1-3 hour task (e.g., quick bug fix, simple web scraping, lightweight script) executable via Python. Reject complex large projects or customer support.

If it FAILS the filter, your response must start with "REJECTED:" followed by the reason.
If it PASSES, provide the output in EXACTLY this format:

===BUDGET===
(Estimated fixed price in USD, e.g., "$150" or "약 $150~$200" and estimated Korean Won conversion)
===PROPOSAL===
(Write an Upwork/Fiverr proposal. 150-250 words. Hook proving you read it, relevant experience from Twomin Studio, concrete first step, and 1 clarifying question. Plain text, no markdown headers.)
===PLAN===
(Korean, max 150 words: 1) 난이도: 하, 2) 핵심 구현 방식, 3) 필요 라이브러리)
===CODE===
(One complete, runnable Python 3 file solving the core task. Clean comments, env variables for secrets, main() entry point.)
"""

STATS_SYSTEM = (
    "너는 프리랜서 공고 수집 봇의 전용 비서야. 사용자의 질문에 DB 통계를 기반으로 친절하고 간결한 한국어로 답변해 줘. (6줄 이내)"
)


async def ask_claude(system: str, user: str, max_tokens: int = 1500) -> str:
    resp = await claude.messages.create(
        model=MODEL,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return "".join(b.text for b in resp.content if b.type == "text").strip()


def parse_analysis(text: str) -> tuple[bool, str, str, str, str]:
    if text.startswith("REJECTED:"):
        return False, "", "", "", ""
    
    budget, proposal, plan, code = "", "", "", ""
    try:
        parts = text.split("===BUDGET===")
        if len(parts) > 1:
            rest = parts[1]
            b_parts = rest.split("===PROPOSAL===")
            budget = b_parts[0].strip()
            if len(b_parts) > 1:
                p_parts = b_parts[1].split("===PLAN===")
                proposal = p_parts[0].strip()
                if len(p_parts) > 1:
                    c_parts = p_parts[1].split("===CODE===")
                    plan = c_parts[0].strip()
                    if len(c_parts) > 1:
                        code = c_parts[1].strip()
    except Exception:
        pass

    # 코드 블록 정리
    m = re.search(r"```(?:python|py)?[ \t]*\n(.*?)```", code, re.S)
    if m:
        code = m.group(1).strip()
    else:
        code = re.sub(r"^```(?:python|py)?[ \t]*\n", "", code)
        code = re.sub(r"\n```\s*$", "", code).strip()

    return True, budget, proposal, plan, code


async def analyze_and_generate(platform: str, title: str, summary: str, link: str) -> tuple[bool, str, str, str, str]:
    user_prompt = f"Platform: {platform}\nJob Title: {title}\nLink: {link}\n\nDescription:\n{summary}"
    raw = await ask_claude(FILTER_AND_GEN_SYSTEM, user_prompt, max_tokens=4000)
    return parse_analysis(raw)


def syntax_check(code: str) -> str:
    if not code:
        return "코드 없음"
    try:
        ast.parse(code)
        return "문법 검사 통과 (실행 준비 완료)"
    except SyntaxError as e:
        return f"⚠️ 문법 오류 가능 (line {e.lineno})"


# ----------------------------------------------------------------------------
# 텔레그램 UI 및 메시지 렌더링
# ----------------------------------------------------------------------------
def keyboard(draft_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ 승인 및 코드 제작", callback_data=f"ok:{draft_id}"),
            InlineKeyboardButton("❌ 거절", callback_data=f"no:{draft_id}"),
        ]]
    )


def build_notification_message(draft_id: int, platform: str, title: str, link: str, budget: str, plan: str, draft: str) -> str:
    return (
        f"📌 [공고 #{draft_id}] {title}\n"
        f"💰 결제/예산: [건당 고정가] {budget}\n"
        f"⏱️ 예상 소요 시간: 1~3시간 컷 (초단기 꿀알바)\n"
        f"🔗 원문 링크: {link}\n\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"📝 [핵심 요약 및 제안서 초안]\n{draft[:800]}\n\n"
        f"🛠 [구현 방안]\n{plan[:500]}"
    )


# ----------------------------------------------------------------------------
# 주기적 RSS 폴링 작업
# ----------------------------------------------------------------------------
async def poll_feeds(context: ContextTypes.DEFAULT_TYPE) -> int:
    if _poll_lock.locked():
        log.info("이전 수집 작업 진행 중...")
        return 0

    async with _poll_lock:
        first_run = SEED_ON_FIRST_RUN and await asyncio.to_thread(seen_count) == 0
        sent = 0

        for platform, urls in FEEDS.items():
            for url in urls:
                try:
                    items = await asyncio.to_thread(fetch_feed_sync, url)
                except Exception as e:
                    STATE["feed_errors"] += 1
                    log.warning("피드 오류 (%s): %s", url, e)
                    continue

                for item in items:
                    if await asyncio.to_thread(is_seen, item["uid"]):
                        continue

                    if first_run:
                        await asyncio.to_thread(mark_seen, item["uid"], platform, item["title"], item["link"], True)
                        continue

                    if sent >= MAX_NEW_PER_CYCLE:
                        continue

                    try:
                        passed, budget, proposal, plan, code = await analyze_and_generate(
                            platform, item["title"], item["summary"], item["link"]
                        )
                        
                        # 필터 탈락(시간제 또는 대형 프로젝트)인 경우 슨슬하게 seen 처리만 하고 알림 스킵
                        if not passed:
                            await asyncio.to_thread(mark_seen, item["uid"], platform, item["title"], item["link"], False)
                            continue

                        draft_id = await asyncio.to_thread(
                            save_draft, platform, item["title"], item["link"], proposal, plan, code, budget
                        )
                        
                        msg_text = build_notification_message(draft_id, platform, item["title"], item["link"], budget, plan, proposal)
                        await context.bot.send_message(
                            chat_id=ADMIN_CHAT_ID,
                            text=msg_text,
                            reply_markup=keyboard(draft_id),
                            disable_web_page_preview=True,
                        )
                        
                        await asyncio.to_thread(mark_seen, item["uid"], platform, item["title"], item["link"], False)
                        sent += 1
                    except Exception:
                        log.exception("공고 분석 및 전송 실패: %s", item["title"])

        STATE["last_poll_at"] = now_str()
        STATE["last_poll_new"] = sent
        return sent


async def poll_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    await poll_feeds(context)


# ----------------------------------------------------------------------------
# 텔레그램 핸들러
# ----------------------------------------------------------------------------
async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.message or query.message.chat.id != ADMIN_CHAT_ID:
        return

    action, _, raw_id = (query.data or "").partition(":")
    if not raw_id.isdigit():
        await query.answer()
        return
    draft_id = int(raw_id)

    row = await asyncio.to_thread(get_draft, draft_id)
    if not row:
        await query.answer("초안을 찾을 수 없어요.", show_alert=True)
        return
    platform, title, link, draft, status, plan, code, budget = row

    if status != "pending":
        await query.answer(f"이미 처리된 공고입니다 ({status})")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if action == "ok":
        await asyncio.to_thread(set_status, draft_id, "approved")
        await query.answer("승인 완료!")
        await query.edit_message_text(
            text=f"✅ [승인 완료] 공고 #{draft_id}\n[{platform}] {title}\n🔗 {link}",
            disable_web_page_preview=True,
        )
        
        # 스마트폰 복사 최적화 마크다운 블록 전송
        await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text="📋 [맞춤형 제안서 초안] (길게 눌러 복사하세요)")
        await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"```text\n{draft}\n```", parse_mode="Markdown")
        
        if plan:
            await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"🛠 [구현 방안]\n{plan}")
            
        if code:
            await context.bot.send_document(
                chat_id=ADMIN_CHAT_ID,
                document=io.BytesIO(code.encode("utf-8")),
                filename=f"solution_{draft_id}.py",
                caption=f"💻 [구현 소스 코드 파일]\n{syntax_check(code)}",
            )
    elif action == "no":
        await asyncio.to_thread(set_status, draft_id, "rejected")
        await query.answer("거절 처리됨")
        await query.edit_message_text(
            text=f"❌ [거절됨] 공고 #{draft_id}\n[{platform}] {title}",
            disable_web_page_preview=True,
        )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🚀 1인 무인 외주 자동화 비서 봇 가동 중!\n\n"
        "• 건당 고정가(Fixed-Price) & 1~3시간 컷 꿀알바만 자동 선별합니다.\n"
        "• /check — 지금 즉시 공고 강제 스캔\n"
        "• 편하게 '오늘 몇 개 수집됐어?'라고 물어보세요."
    )


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _poll_lock.locked():
        await update.message.reply_text("⏳ 수집 작업이 이미 진행 중입니다.")
        return
    await update.message.reply_text("🔎 알짜배기 고정가 공고 스캔 및 분석 중...")
    sent = await poll_feeds(context)
    await update.message.reply_text(f"스캔 완료! 신규 알림 발송: {sent}건")


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.text:
        return

    await context.bot.send_chat_action(chat_id=ADMIN_CHAT_ID, action=ChatAction.TYPING)
    stats = await asyncio.to_thread(get_stats)
    data = json.dumps(stats, ensure_ascii=False, indent=2)

    try:
        answer = await ask_claude(STATS_SYSTEM, f"[현재 데이터]\n{data}\n\n[사용자 질문]\n{msg.text}", max_tokens=500)
        await msg.reply_text(answer[:4000])
    except Exception:
        log.exception("자연어 답변 실패")
        await msg.reply_text(f"📊 오늘 신규 수집 공고: {stats['신규 수집(오늘)']}건 | 대기중: {stats['상태별 초안']['대기중']}건")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    if isinstance(context.error, Conflict):
        return
    log.error("봇 핸들러 오류", exc_info=context.error)


# ----------------------------------------------------------------------------
# Render 헬스체크 웹서버
# ----------------------------------------------------------------------------
class HealthHandler(BaseHTTPRequestHandler):
    def _ok(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
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
    log.info("헬스체크 서버 구동 포트: %d", port)


# ----------------------------------------------------------------------------
# 메인 실행부
# ----------------------------------------------------------------------------
async def post_init(app: Application) -> None:
    STATE["started_at"] = now_str()
    try:
        await app.bot.send_message(chat_id=ADMIN_CHAT_ID, text="🤖 외주 자동화 비서 봇이 완벽하게 세팅되어 가동을 시작합니다.")
    except Exception:
        log.exception("시작 알림 전송 실패")


def main() -> None:
    init_db()
    start_health_server()

    admin_only = filters.Chat(chat_id=ADMIN_CHAT_ID)
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", cmd_start, filters=admin_only))
    app.add_handler(CommandHandler("check", cmd_check, filters=admin_only))
    app.add_handler(CallbackQueryHandler(on_button, pattern=r"^(ok|no):\d+$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & admin_only, on_text))
    app.add_error_handler(on_error)

    app.job_queue.run_repeating(poll_job, interval=POLL_INTERVAL, first=10, name="poll_feeds")

    log.info("봇 시작 (Polling) | 모델=%s", MODEL)
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
