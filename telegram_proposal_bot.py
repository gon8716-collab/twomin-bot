import os
import logging
import asyncio
import feedparser
import anthropic
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    CallbackQueryHandler,
)

# 로깅 설정
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# 환경 변수 불러오기
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# Claude 클라이언트 초기화
claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None

# 이미 처리한 공고 ID를 기억하여 중복 알림 방지
processed_job_ids = set()

# -------------------------------------------------------------------------
# 1. Claude API를 통한 제안서(Proposal) 초안 자동 생성 함수
# -------------------------------------------------------------------------
def generate_proposal_with_claude(job_title, job_description):
    if not claude_client:
        return "⚠️ Anthropic API Key가 설정되지 않아 제안서를 생성할 수 없습니다."
    
    prompt = f"""
당신은 뛰어난 프리랜서 제안서 작성 전문가입니다. 아래 업워크 공고를 분석하고, 클라이언트의 마음을 사로잡을 수 있는 전문적이고 간결한 영문 제안서 초안을 작성해주세요.

[공고 제목]
{job_title}

[공고 내용]
{job_description}

[작성 지침]
1. 친근하면서도 신뢰감을 주는 전문적인 어조를 사용할 것.
2. 클라이언트의 요구사항을 정확히 이해했음을 첫 줄에서 어필할 것.
3. 관련 경험과 실행 방안을 간결하게 제시할 것.
"""
    try:
        response = claude_client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f"Claude API 호출 에러: {e}")
        return f"제안서 생성 중 오류가 발생했습니다: {str(e)}"

# -------------------------------------------------------------------------
# 2. 업워크 공고 크롤링 (RSS 피드 활용 방식)
# -------------------------------------------------------------------------
async def fetch_latest_upwork_job():
    rss_url = "https://www.upwork.com/ab/feed/jobs/rss?q=python&sort=recency"
    
    try:
        feed = await asyncio.to_thread(feedparser.parse, rss_url)
        if feed.entries:
            latest_entry = feed.entries[0]
            job_id = latest_entry.id
            job_title = latest_entry.title
            job_desc = latest_entry.summary
            job_link = latest_entry.link
            return job_id, job_title, job_desc, job_link
    except Exception as e:
        logger.error(f"업워크 크롤링 에러: {e}")
    
    return None, None, None, None

# -------------------------------------------------------------------------
# 3. 백그라운드 공고 모니터링 루프
# -------------------------------------------------------------------------
async def job_monitor_loop(application):
    await asyncio.sleep(10)
    
    while True:
        try:
            job_id, title, desc, link = await fetch_latest_upwork_job()
            
            if job_id and job_id not in processed_job_ids:
                processed_job_ids.add(job_id)
                
                if len(processed_job_ids) > 100:
                    processed_job_ids.pop()
                
                logger.info(f"신규 공고 발견: {title}")
                
                # Claude로 제안서 초안 생성
                proposal_draft = generate_proposal_with_claude(title, desc)
                
                message_text = (
                    f"🚨 **[신규 업워크 공고 감지]**\n\n"
                    f"📌 **제목:** {title}\n"
                    f"🔗 **링크:** {link}\n\n"
                    f"📝 **[Claude 제안서 초안]**\n{proposal_draft}"
                )
                
                keyboard = [
                    [
                        InlineKeyboardButton("✅ 승인 및 전송", callback_data=f"approve_{job_id}"),
                        InlineKeyboardButton("❌ 거절", callback_data=f"reject_{job_id}")
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                ADMIN_CHAT_ID = os.environ.get("TELEGRAM_ADMIN_CHAT_ID")
                
                if ADMIN_CHAT_ID:
                    await application.bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=message_text,
                        reply_markup=reply_markup,
                        parse_mode="Markdown"
                    )
        
        except Exception as e:
            logger.error(f"모니터링 루프 에러: {e}")
        
        await asyncio.sleep(300)

# -------------------------------------------------------------------------
# 4. 텔레그램 명령어 및 버튼 핸들러
# -------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_name = update.effective_user.first_name
    chat_id = update.effective_chat.id
    
    logger.info(f"사용자 Chat ID: {chat_id}")
    
    await update.message.reply_text(
        f"안녕하세요, {user_name}님! 2분봇이 정상적으로 작동을 시작했습니다. 🚀\n"
        f"현재 사장님의 텔레그램 Chat ID는 `{chat_id}` 입니다.\n"
        "이 값을 Render 환경 변수(`TELEGRAM_ADMIN_CHAT_ID`)에 등록해 주세요!"
    )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    if data.startswith("approve_"):
        await query.edit_message_text(text=f"{query.message.text}\n\n✅ **[상태: 승인 완료 및 전송됨]**")
    elif data.startswith("reject_"):
        await query.edit_message_text(text=f"{query.message.text}\n\n❌ **[상태: 거절됨 (무시함)]**")

def main():
    if not TELEGRAM_BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN 환경 변수가 설정되지 않았습니다.")
        return

    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_callback))

    async def post_init(app):
        asyncio.create_task(job_monitor_loop(app))
        
    application.post_init = post_init

    print("봇을 실행합니다...")
    application.run_polling()

if __name__ == '__main__':
    main()
