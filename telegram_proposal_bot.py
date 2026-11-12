"""
Upwork / Fiverr RSS → Claude 제안서 초안 → 텔레그램 승인 봇

- RSS 주기 수집 (feedparser)
- 플랫폼별 맞춤 제안서 초안 (Anthropic Claude API)
- 텔레그램 인라인 버튼 [승인 및 전송] / [거절]
- SQLite 기반 중복 공고 방지
- Render 웹 서비스용: 폴링 + 헬스체크용 미니 HTTP 서버($PORT 바인딩)

필수 환경 변수
  TELEGRAM_BOT_TOKEN, ANTHROPIC_API_KEY, TELEGRAM_ADMIN_CHAT_ID
  (최소 하나) UPWORK_RSS_URLS, FIVERR_RSS_URLS  ← 쉼표로 여러 개 구분

선택 환경 변수
  ANTHROPIC_MODEL      기본 claude-sonnet-5-5
  POLL_INTERVAL_SEC    기본 300
  MAX_NEW_PER_CYCLE    기본 5   (한 주기에 처리할 최대 신규 공고 수, 비용 제어)
  SEED_ON_FIRST_RUN    기본 true (DB가 비어 있으면 기존 공고는 알림 없이 '본 것'으로 처리)
  DB_PATH              기본 seen.db
  PROPOSAL_PROFILE     내 소개/강점 (제안서에 반영됨)
  PORT                 Render가 자동 주입
"""

import asyncio
import hashlib
import html
import logging
import os
import re
import sqlite3
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import feedparser
from anthropic import AsyncAnthropic
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

# ----------------------------------------------------------------------------
# 설정
# ----------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)  # 토큰이 URL에 찍히는 것 방지
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

FEEDS: dict[str, list[str]] = {
    "Upwork": split_urls("UPWORK_RSS_URLS"),
    "Fiverr": split_urls("FIVERR_RSS_URLS"),
}
if not any(FEEDS.values()):
    log.error("UPWORK_RSS_URLS 또는 FIVERR_RSS_URLS 중 하나는 설정해야 합니다.")
    sys.exit(1)

MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5-5")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SEC", "300"))
MAX_NEW_PER_CYCLE = int(os.environ.get("MAX_NEW_PER_CYCLE", "5"))
SEED_ON_FIRST_RUN = os.environ.get("SEED_ON_FIRST_RUN", "true").lower() == "true"
DB_PATH = os.environ.get("DB_PATH", "seen.db")
PROFILE = os.environ.get(
    "PROPOSAL_PROFILE",
    "Indie developer at Twomin Studio building mobile and desktop applications.",
)
USER_AGENT = "Mozilla/5.0 (compatible; ProposalBot/1.0)"

claude = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

# ----------------------------------------------------------------------------
# SQLite (중복 방지 + 초안 저장)
# ----------------------------------------------------------------------------
_db_lock = threading.Lock()


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, timeout=10)


def init_db() -> None:
    with _db_lock, _conn() as c:
        c.execute("CREATE TABLE IF NOT EXISTS seen (uid TEXT PRIMARY KEY, ts DATETIME DEFAULT CURRENT_TIMESTAMP)")
        c.execute(
            """CREATE TABLE IF NOT EXISTS drafts (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   platform TEXT, title TEXT, link TEXT, draft TEXT,
                   status TEXT DEFAULT 'pending',
                   ts DATETIME DEFAULT CURRENT_TIMESTAMP)"""
        )


def seen_count() -> int:
    with _db_lock, _conn() as c:
        return c.execute("SELECT COUNT(*) FROM seen").fetchone()[0]


def is_seen(uid: str) -> bool:
    with _db_lock, _conn() as c:
        return c.execute("SELECT 1 FROM seen WHERE uid=?", (uid,)).fetchone() is not None


def mark_seen(uid: str) -> None:
    with _db_lock, _conn() as c:
        c.execute("INSERT OR IGNORE INTO seen(uid) VALUES (?)", (uid,))


def save_draft(platform: str, title: str, link: str, draft: str) -> int:
    with _db_lock, _conn() as c:
        cur = c.execute(
            "INSERT INTO drafts(platform,title,link,draft) VALUES (?,?,?,?)",
            (platform, title, link, draft),
        )
        return cur.lastrowid


def get_draft(draft_id: int):
    with _db_lock, _conn() as c:
        return c.execute(
            "SELECT platform,title,link,draft,status FROM drafts WHERE id=?", (draft_id,)
        ).fetchone()


def set_status(draft_id: int, status: str) -> None:
    with _db_lock, _conn() as c:
        c.execute("UPDATE drafts SET status=? WHERE id=?", (status, draft_id))


# ----------------------------------------------------------------------------
# RSS 수집
# ----------------------------------------------------------------------------
def clean_html(text: str, limit: int = 3000) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def make_uid(entry) -> str:
    raw = entry.get("id") or entry.get("link") or entry.get("title", "")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def fetch_feed_sync(url: str) -> list[dict]:
    parsed = feedparser.parse(url, agent=USER_AGENT)
    if parsed.bozo and not parsed.entries:
        log.warning("RSS 파싱 실패: %s (%s)", url, parsed.get("bozo_exception"))
        return []
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
# Claude 제안서 생성
# ----------------------------------------------------------------------------
PLATFORM_GUIDE = {
    "Upwork": (
        "Write an Upwork cover letter. 150-250 words. Open with a specific hook that proves you read the post "
        "(no 'Hello, I am writing to apply'). Briefly show relevant experience, outline a concrete first-step "
        "approach, and end with one sharp clarifying question. Plain text, no markdown headings."
    ),
    "Fiverr": (
        "Write a Fiverr buyer-request offer message. 80-150 words, friendly and direct. State what you will "
        "deliver, a short how-it-works, and a call to action. Leave [PRICE] and [DELIVERY DAYS] as placeholders. "
        "Plain text, no markdown headings."
    ),
}


async def generate_proposal(platform: str, title: str, summary: str, link: str) -> str:
    system = (
        "You are a freelance proposal writer. Write in the same language as the job post. "
        "Be specific, honest, and concise. Never invent credentials, past clients, or numbers. "
        "Output ONLY the proposal text.\n\n"
        f"Freelancer profile: {PROFILE}\n\n{PLATFORM_GUIDE[platform]}"
    )
    user = f"Platform: {platform}\nJob title: {title}\nLink: {link}\n\nJob description:\n{summary}"
    resp = await claude.messages.create(
        model=MODEL,
        max_tokens=800,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return "".join(b.text for b in resp.content if b.type == "text").strip()


# ----------------------------------------------------------------------------
# 텔레그램
# ----------------------------------------------------------------------------
def keyboard(draft_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ 승인 및 전송", callback_data=f"ok:{draft_id}"),
            InlineKeyboardButton("❌ 거절", callback_data=f"no:{draft_id}"),
        ]]
    )


def build_message(platform: str, title: str, link: str, draft: str) -> str:
    msg = f"🆕 [{platform}] {title}\n🔗 {link}\n\n📝 제안서 초안\n{'─' * 16}\n{draft}"
    return msg[:4000]  # 텔레그램 4096자 제한 여유


async def poll_feeds(context: ContextTypes.DEFAULT_TYPE) -> None:
    first_run = SEED_ON_FIRST_RUN and await asyncio.to_thread(seen_count) == 0
    sent = 0

    for platform, urls in FEEDS.items():
        for url in urls:
            try:
                items = await asyncio.to_thread(fetch_feed_sync, url)
            except Exception:
                log.exception("피드 수집 오류: %s", url)
                continue

            for item in items:
                if await asyncio.to_thread(is_seen, item["uid"]):
                    continue

                if first_run:  # 최초 실행: 기존 공고는 알림 없이 기록만
                    await asyncio.to_thread(mark_seen, item["uid"])
                    continue

                if sent >= MAX_NEW_PER_CYCLE:
                    continue  # 다음 주기에 처리 (seen 처리 안 함)

                try:
                    draft = await generate_proposal(platform, item["title"], item["summary"], item["link"])
                    draft_id = await asyncio.to_thread(save_draft, platform, item["title"], item["link"], draft)
                    await context.bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=build_message(platform, item["title"], item["link"], draft),
                        reply_markup=keyboard(draft_id),
                        disable_web_page_preview=True,
                    )
                    await asyncio.to_thread(mark_seen, item["uid"])  # 전송 성공 후에만 기록
                    sent += 1
                except Exception:
                    log.exception("제안서 생성/전송 실패: %s", item["title"])

    if first_run:
        log.info("최초 실행: 기존 공고를 기준점으로 저장했습니다. 이후 신규 공고만 알립니다.")
    elif sent:
        log.info("신규 공고 %d건 알림 전송", sent)


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.message or query.message.chat.id != ADMIN_CHAT_ID:
        if query:
            await query.answer("권한이 없습니다.", show_alert=True)
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
    platform, title, link, draft, status = row

    if status != "pending":
        await query.answer(f"이미 처리됨 ({status})")
        await query.edit_message_reply_markup(reply_markup=None)
        return

    if action == "ok":
        await asyncio.to_thread(set_status, draft_id, "approved")
        await query.answer("승인 완료")
        await query.edit_message_reply_markup(reply_markup=None)
        # 업워크/파이버는 공식 제안서 제출 API가 없으므로, 복사해서 바로 붙여넣을 수 있게 최종본 전송
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"✅ 승인됨 — [{platform}] {title}\n🔗 {link}\n\n아래 최종본을 복사해서 제출하세요 👇",
            disable_web_page_preview=True,
        )
        await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=draft[:4000])
    elif action == "no":
        await asyncio.to_thread(set_status, draft_id, "rejected")
        await query.answer("거절 처리")
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"❌ 거절됨 — {title}")
    else:
        await query.answer()


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.id != ADMIN_CHAT_ID:
        return
    await update.message.reply_text(
        f"🤖 제안서 봇 가동 중\n모델: {MODEL}\n수집 주기: {POLL_INTERVAL}초\n/check — 지금 바로 공고 확인"
    )


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.id != ADMIN_CHAT_ID:
        return
    await update.message.reply_text("🔎 공고 확인 중...")
    await poll_feeds(context)
    await update.message.reply_text("완료")


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.error("핸들러 오류", exc_info=context.error)


# ----------------------------------------------------------------------------
# Render용 헬스체크 서버 ($PORT 바인딩) — 텔레그램은 폴링이라 포트와 충돌 없음
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
    try:
        await app.bot.send_message(chat_id=ADMIN_CHAT_ID, text="🚀 제안서 봇이 시작됐어요.")
    except Exception:
        log.exception("시작 알림 전송 실패 (TELEGRAM_ADMIN_CHAT_ID 확인, 봇에게 먼저 /start 필요)")


def main() -> None:
    init_db()
    start_health_server()

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("check", cmd_check))
    app.add_handler(CallbackQueryHandler(on_button, pattern=r"^(ok|no):\d+$"))
    app.add_error_handler(on_error)

    app.job_queue.run_repeating(poll_feeds, interval=POLL_INTERVAL, first=10, name="poll_feeds")

    log.info("봇 시작 (polling)")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,  # 재시작 시 밀린 업데이트로 인한 충돌/중복 방지
    )


if __name__ == "__main__":
    main()
