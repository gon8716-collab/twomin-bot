import os
import logging
import feedparser
import httpx
import anthropic
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# 로깅 설정
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# 환경 변수 불러오기
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ADMIN_CHAT_ID = os.getenv("TELEGRAM_ADMIN_CHAT_ID")

# 클라이언트 초기화
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# 업워크 RSS 피드 URL (예: Python 관련 최신 공고 검색 RSS)
UPWORK_RSS_URL = "https://www.upwork.com/ab/feed/jobs/rss?q=python&sort=rec"

# 이미 처리한 공고 ID 저장용 세트 (중복 알림 방지)
processed_jobs = set()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """봇 시작 명령어"""
    await update.message.reply_text("🤖 업워크 자동 제안서 봇이 정상적으로 작동 중입니다!")

def fetch_latest_upwork_job():
    """업워크 RSS에서 최신 공고 1개를 가져옴"""
    try:
        feed = feedparser.parse(UPWORK_RSS_URL)
        if not feed.entries:
            return None
        
        latest_entry = feed.entries[0]
        job_id = latest_entry.id
        job_title = latest_entry.title
        job_summary = latest_entry.summary
        job_link = latest_entry.link
        
        return {
            "id": job_id,
            "title": job_title,
            "summary": job_summary,
            "link": job_link
        }
    except Exception as e:
        logger.error(f"RSS 피드 수집 에러: {e}")
        return None

def generate_proposal_with_claude(job_title, job_summary):
    """클로드 API를 이용해 맞춤형 제안서 초안 작성"""
    try:
        prompt = f"""
다음은 업워크에 올라온 프리랜서 구인 공고입니다. 이 공고를 분석하고, 클라이언트의 마음을 사로잡을 수 있는 전문적이고 설득력 있는 영문 제안서(Proposal) 초안을 작성해주세요.

[공고 제목]
{job_title}

[공고 내용]
{job_summary}

제안서 초안:
"""
        response = anthropic_client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f"Claude API 생성 에러: {e}")
        return "제안서 생성 중 오류가 발생했습니다."

async def check_jobs(context: ContextTypes.DEFAULT_TYPE):
    """주기적으로 업워크 공고를 확인하고 텔레그램으로 전송"""
    job = fetch_latest_upwork_job()
    if not job:
        return
    
    job_id = job["id"]
    if job_id in processed_jobs:
        return  # 이미 처리한 공고면 패스
    
    # 새로운 공고 처리 등록
    processed_jobs.add(job_id)
    
    # 클로드로 제안서 생성
    proposal = generate_proposal_with_claude(job["title"], job["summary"])
    
    # 텔레그램 메시지 구성
    message_text = (
        f"🚨 **새로운 업워크 공고 발견!**\n\n"
        f"📌 **제목:** {job['title']}\n"
        f"🔗 **링크:** {job['link']}\n\n"
        f"📝 **[클로드 작성 제안서 초안]**\n{proposal}"
    )
    
    # 승인 / 거절 버튼 생성
    keyboard = [
        [
            InlineKeyboardButton("✅ 승인 및 전송", callback_data="approve_job"),
            InlineKeyboardButton("❌ 거절", callback_data="reject_job")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # 관리자에게 전송
    if ADMIN_CHAT_ID:
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=message_text,
            parse_mode="Markdown",
            reply_markup=reply_markup
        )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """텔레그램 버튼 클릭 이벤트 처리"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "approve_job":
        await query.edit_message_text(text=query.message.text + "\n\n✅ **[상태: 승인됨 (전송 완료)]**")
    elif query.data == "reject_job":
        await query.edit_message_text(text=query.message.text + "\n\n❌ **[상태: 거절됨]**")

def main():
    """봇 실행 메인 함수"""
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN이 설정되지 않았습니다.")
        return

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # 핸들러 등록
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))

    # 백그라운드 작업 스케줄러 (10분마다 업워크 RSS 확인)
    job_queue = application.job_queue
    job_queue.run_repeating(check_jobs, interval=600, first=10)

    # 봇 실행 (폴링 방식)
    application.run_polling()

if __name__ == "__main__":
    main()
