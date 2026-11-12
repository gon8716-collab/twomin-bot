"""
telegram_proposal_bot.py
RSS 공고 수집 → Claude 제안서 초안 → 텔레그램 승인/거절 버튼 → 자연어 현황 대화

[기능]
1. AI 제안서 자동 생성 (Anthropic, 기본 모델 claude-sonnet-5-5)
2. 중복 수집/알림 방지 (SQLite 파일에 공고 링크 해시 기록, 재시작해도 유지)
3. 인라인 버튼 [✅ 승인 (Send)] / [❌ 거절 (Reject)]
4. 자연어 통계 대화: "오늘 수집된 공고 몇 개야?", "현황 알려줘" 처럼 평소 말투로 물어보면
   DB 통계를 바탕으로 Claude가 친절하게 답변

[필수 환경 변수]
  TELEGRAM_BOT_TOKEN, ANTHROPIC_API_KEY, TELEGRAM_ADMIN_CHAT_ID
  그리고 아래 RSS 변수 중 최소 하나 (쉼표로 여러 개 가능)
    UPWORK_RSS_URLS   (※ Upwork는 2024-08 이후 공식 RSS를 종료함)
    FIVERR_RSS_URLS
    EXTRA_RSS_URLS    "라벨|URL,라벨|URL" 형식. 예) "WWR|https://weworkremotely.com/remote-jobs.rss"

[선택 환경 변수]
  ANTHROPIC_MODEL     기본 claude-sonnet-5-5
  POLL_INTERVAL_SEC   기본 300
  MAX_NEW_PER_CYCLE   기본 5
  SEED_ON_FIRST_RUN   기본 true  (DB가 비어 있으면 기존 공고는 알림 없이 기준점으로만 저장)
  DB_PATH             기본 proposal_bot.db  (Render 영구 디스크를 쓰면 /data/proposal_bot.db)
  PROPOSAL_PROFILE    내 소개/강점
  PORT                Render가 자동 주입

[requirements.txt]
  python-telegram-bot[job-queue]>=21.0
  feedparser
  anthropic
"""

import asyncio
import hashlib
import html
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
# 로깅 / 환경 변수
# ----------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)  # 토큰이 URL로 로그에 찍히는 것 방지
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
    log.error(
        "RSS 주소가 없습니다. UPWORK_RSS_URLS / FIVERR_RSS_URLS / EXTRA_RSS_URLS 중 하나를 설정하세요."
    )
    sys.exit(1)

MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5-5")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SEC", "300"))
MAX_NEW_PER_CYCLE = int(os.environ.get("MAX_NEW_PER_CYCLE", "5"))
SEED_ON_FIRST_RUN = os.environ.get("SEED_ON_FIRST_RUN", "true").lower() == "true"
DB_PATH = os.environ.get("DB_PATH", "proposal_bot.db")
PROFILE = os.environ.get(
    "PROPOSAL_PROFILE",
    "Indie developer at Twomin Studio building mobile and desktop applications.",
)
USER_AGENT = "Mozilla/5.0 (compatible; ProposalBot/2.0)"
KST = timezone(timedelta(hours=9))

claude = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

# 런타임 상태 (메모리) — 자연어 현황 답변에 같이 사용
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
# SQLite (중복 방지 + 초안/상태 저장 + 통계)
# ----------------------------------------------------------------------------
_db_lock = threading.Lock()


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, timeout=10)


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


def save_draft(platform: str, title: str, link: str, draft: str) -> int:
    with _db_lock, _conn() as c:
        cur = c.execute(
            "INSERT INTO drafts(platform,title,link,draft,status,created_at) VALUES (?,?,?,?, 'pending', ?)",
            (platform, title, link, draft, now_str()),
        )
        return cur.lastrowid


def get_draft(draft_id: int):
    with _db_lock, _conn() as c:
        return c.execute(
            "SELECT platform,title,link,draft,status FROM drafts WHERE id=?", (draft_id,)
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
        "최초 실행 시 기준점으로만 저장된 기존 공고 수": baseline,
        "플랫폼별 신규 수집(누적)": by_platform,
        "제안서 초안 생성(오늘)": drafts_today,
        "상태별 초안(누적)": {
            "대기중": status_all.get("pending", 0),
            "승인": status_all.get("approved", 0),
            "거절": status_all.get("rejected", 0),
        },
        "오늘 승인": approved_today,
        "오늘 거절": rejected_today,
        "최근 초안 5건": [
            {"플랫폼": p, "제목": t, "상태": s, "생성시각": ts} for p, t, s, ts in recent
        ],
        "봇 시작 시각": STATE["started_at"],
        "마지막 수집 시각": STATE["last_poll_at"],
        "마지막 수집에서 새로 알린 건수": STATE["last_poll_new"],
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
# Claude
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
GENERIC_GUIDE = (
    "Write a concise, compelling application message for this job post. 120-200 words. Open with a specific hook "
    "that proves you read the post, show relevant experience, propose a concrete first step, and end with one "
    "clarifying question. Plain text, no markdown headings."
)


async def ask_claude(system: str, user: str, max_tokens: int = 800) -> str:
    resp = await claude.messages.create(
        model=MODEL,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return "".join(b.text for b in resp.content if b.type == "text").strip()


async def generate_proposal(platform: str, title: str, summary: str, link: str) -> str:
    system = (
        "You are a freelance proposal writer. Write in the same language as the job post. "
        "Be specific, honest, and concise. Never invent credentials, past clients, or numbers. "
        "Output ONLY the proposal text.\n\n"
        f"Freelancer profile: {PROFILE}\n\n{PLATFORM_GUIDE.get(platform, GENERIC_GUIDE)}"
    )
    user = f"Platform: {platform}\nJob title: {title}\nLink: {link}\n\nJob description:\n{summary}"
    return await ask_claude(system, user, max_tokens=800)


STATS_SYSTEM = (
    "너는 프리랜서 공고 수집/제안서 봇의 비서야. 사용자가 텔레그램에서 평소 말투로 현황을 물어보면, "
    "아래 [현재 데이터]에 있는 숫자와 사실만 사용해서 친절하고 간결한 한국어(반말 섞인 친근한 존댓말 정도)로 답해. "
    "데이터에 없는 내용은 지어내지 말고 '그건 아직 기록이 없어요'라고 말해. "
    "'신규 수집'은 봇이 알림 대상으로 처리한 공고이고, '기준점'은 최초 실행 때 알림 없이 저장만 한 기존 공고라는 점을 "
    "필요할 때만 짧게 설명해. 공고 수집/제안서/현황과 전혀 무관한 질문이면 현황 관련 질문만 도와줄 수 있다고 안내해. "
    "답변은 6줄 이내, 이모지는 적당히."
)


# ----------------------------------------------------------------------------
# 텔레그램 UI
# ----------------------------------------------------------------------------
def keyboard(draft_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ 승인 (Send)", callback_data=f"ok:{draft_id}"),
            InlineKeyboardButton("❌ 거절 (Reject)", callback_data=f"no:{draft_id}"),
        ]]
    )


def build_message(platform: str, title: str, link: str, draft: str) -> str:
    msg = f"🆕 [{platform}] {title}\n🔗 {link}\n\n📝 제안서 초안\n{'─' * 16}\n{draft}"
    return msg[:4000]  # 텔레그램 4096자 제한 여유


# ----------------------------------------------------------------------------
# 수집 작업 (주기 실행)
# ----------------------------------------------------------------------------
async def poll_feeds(context: ContextTypes.DEFAULT_TYPE) -> int:
    if _poll_lock.locked():
        log.info("이전 수집 작업이 아직 진행 중이라 건너뜁니다.")
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
                    log.warning("피드 수집 오류 (%s): %s", url, e)
                    continue

                for item in items:
                    if await asyncio.to_thread(is_seen, item["uid"]):
                        continue

                    if first_run:  # 최초 실행: 기존 공고는 알림 없이 기록만
                        await asyncio.to_thread(
                            mark_seen, item["uid"], platform, item["title"], item["link"], True
                        )
                        continue

                    if sent >= MAX_NEW_PER_CYCLE:
                        continue  # 다음 주기에 처리 (seen 기록 안 함)

                    try:
                        draft = await generate_proposal(platform, item["title"], item["summary"], item["link"])
                        draft_id = await asyncio.to_thread(
                            save_draft, platform, item["title"], item["link"], draft
                        )
                        await context.bot.send_message(
                            chat_id=ADMIN_CHAT_ID,
                            text=build_message(platform, item["title"], item["link"], draft),
                            reply_markup=keyboard(draft_id),
                            disable_web_page_preview=True,
                        )
                        # 알림 전송에 성공한 뒤에만 '처리 완료'로 기록 → 실패 건은 다음 주기에 재시도
                        await asyncio.to_thread(
                            mark_seen, item["uid"], platform, item["title"], item["link"], False
                        )
                        sent += 1
                    except Exception:
                        log.exception("제안서 생성/전송 실패: %s", item["title"])

        STATE["last_poll_at"] = now_str()
        STATE["last_poll_new"] = sent
        if first_run:
            log.info("최초 실행: 기존 공고를 기준점으로 저장했습니다. 이후 신규 공고만 알립니다.")
        elif sent:
            log.info("신규 공고 %d건 알림 전송", sent)
        return sent


async def poll_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    await poll_feeds(context)


# ----------------------------------------------------------------------------
# 핸들러
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
    draft_id = int(raw_id)

    row = await asyncio.to_thread(get_draft, draft_id)
    if not row:
        await query.answer("초안을 찾을 수 없어요.", show_alert=True)
        return
    platform, title, link, draft, status = row

    if status != "pending":
        await query.answer(f"이미 처리됨 ({status})")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if action == "ok":
        await asyncio.to_thread(set_status, draft_id, "approved")
        await query.answer("승인 완료")
        await query.edit_message_text(
            text=f"✅ 승인 완료 — [{platform}] {title}\n🔗 {link}",
            disable_web_page_preview=True,
        )
        # 업워크/파이버에는 공식 제안서 제출 API가 없으므로, 바로 복사해 붙여넣을 최종본을 전송
        await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text="📋 아래 최종본을 복사해서 제출하세요 👇")
        await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=draft[:4000])
    elif action == "no":
        await asyncio.to_thread(set_status, draft_id, "rejected")
        await query.answer("거절 처리")
        await query.edit_message_text(
            text=f"❌ 거절됨 — [{platform}] {title}",
            disable_web_page_preview=True,
        )
    else:
        await query.answer()


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🤖 제안서 봇 가동 중이에요.\n"
        f"모델: {MODEL}\n수집 주기: {POLL_INTERVAL}초\n\n"
        "• /check — 지금 바로 공고 확인\n"
        "• 그냥 \"오늘 몇 개 수집됐어?\", \"현황 알려줘\" 처럼 말을 걸어도 돼요."
    )


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _poll_lock.locked():
        await update.message.reply_text("⏳ 이미 수집이 진행 중이에요. 잠시 후 다시 시도해 주세요.")
        return
    await update.message.reply_text("🔎 공고 확인 중...")
    sent = await poll_feeds(context)
    await update.message.reply_text(f"완료! 새로 알린 공고: {sent}건")


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """명령어 없이 평소 말투로 묻는 질문 → DB 통계를 근거로 Claude가 자연어로 답변"""
    msg = update.message
    if not msg or not msg.text:
        return

    await context.bot.send_chat_action(chat_id=ADMIN_CHAT_ID, action=ChatAction.TYPING)
    stats = await asyncio.to_thread(get_stats)
    data = json.dumps(stats, ensure_ascii=False, indent=2)

    try:
        answer = await ask_claude(
            STATS_SYSTEM,
            f"[현재 데이터]\n{data}\n\n[사용자 질문]\n{msg.text}",
            max_tokens=600,
        )
        await msg.reply_text(answer[:4000] or "답변을 만들지 못했어요. 다시 물어봐 주세요.")
    except Exception:
        log.exception("자연어 답변 생성 실패")
        fallback = (
            "⚠️ AI 답변 생성에 실패해서 숫자만 알려드릴게요.\n"
            f"• 오늘 신규 수집: {stats['신규 수집(오늘)']}건\n"
            f"• 누적 신규 수집: {stats['신규 수집(누적)']}건\n"
            f"• 대기중/승인/거절: {stats['상태별 초안(누적)']['대기중']}/"
            f"{stats['상태별 초안(누적)']['승인']}/{stats['상태별 초안(누적)']['거절']}"
        )
        await msg.reply_text(fallback)


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    if isinstance(context.error, Conflict):
        log.warning(
            "Telegram Conflict: 같은 토큰으로 폴링하는 다른 인스턴스가 있습니다. "
            "(로컬 PC/다른 Render 서비스/배포 겹침 확인)"
        )
        return
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
    STATE["started_at"] = now_str()
    try:
        await app.bot.send_message(chat_id=ADMIN_CHAT_ID, text="🚀 제안서 봇이 시작됐어요.")
    except Exception:
        log.exception("시작 알림 전송 실패 (TELEGRAM_ADMIN_CHAT_ID 확인, 봇에게 먼저 /start 필요)")


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

    log.info("봇 시작 (polling) | 모델=%s | 피드=%s", MODEL, {k: len(v) for k, v in FEEDS.items()})
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,  # 재시작 시 밀린 업데이트로 인한 중복 처리 방지
    )


if __name__ == "__main__":
    main()
