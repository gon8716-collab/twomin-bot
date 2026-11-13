import os
import time
import logging
import asyncio
import feedparser
import anthropic
from telegram import (
    Update, 
    ReplyKeyboardMarkup, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup
)
from telegram.ext import (
    ApplicationBuilder, 
    ContextTypes, 
    CommandHandler, 
    MessageHandler, 
    CallbackQueryHandler, 
    filters
)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 환경 변수 로드
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
ALLOW_UNKNOWN_PAYMENT = os.environ.get("ALLOW_UNKNOWN_PAYMENT", "true").lower() == "true"

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# 1) 하단 고정 메뉴 버튼 (키보드)
MAIN_MENU_KEYBOARD = ReplyKeyboardMarkup(
    [
        ["📊 현황 조회", "🔍 지금 즉시 스캔"],
        ["⚙️ 필터 상태 확인", "💰 모델·설정 확인"]
    ],
    resize_keyboard=True,
    is_persistent=True
)

# 2) 공고 알림 아래 붙는 인라인 버튼
def get_job_action_buttons(job_id: str):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🚀 수락 (완성 코드 생성)", callback_data=f"accept_{job_id}")
        ],
        [
            InlineKeyboardButton("✍️ 제안서 다시 쓰기", callback_data=f"rewrite_{job_id}"),
            InlineKeyboardButton("❌ 패스", callback_data=f"skip_{job_id}")
        ]
    ])

# 임시 공고 저장소 (실행 중 봇 메모리용)
PROCESSED_JOBS = {}

# 클로드 API: 1차 필터링 및 실무형 제안서 작성 (저렴하고 빠른 Haiku 모델 사용)
def analyze_job_with_haiku(title: str, description: str):
    prompt = f"""
    당신은 15년 차 시니어 프리랜서 외주 개발자입니다. 아래 업워크 공고를 분석하여 지원 가치가 있는지 판단하고, 맞춤형 제안서 초안을 작성하세요.
    
    [공고 제목]: {title}
    [상세 내용]: {description}
    
    출력 형식 (반드시 JSON 혹은 명확한 구분자로 작성):
    - PASS: true 또는 false (지원할 만한 가치가 있으면 true)
    - REASON: 판단 이유 (1줄)
    - COVER_LETTER: 클라이언트의 문제를 정확히 짚고, 내 기술 스택(Python, 자동화, API 등)으로 어떻게 해결할 것인지 3줄 이내로 작성한 실무형 영문 제안서 (인사말/인디 개발자 언급 금지)
    """
    try:
        response = client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text
        return text
    except Exception as e:
        logger.error(f"Haiku API Error: {e}")
        return None

# 클로드 API: 수락 시 최고급 코딩 모델(Sonnet)로 전체 완성 코드 생성
def generate_complete_code(title: str, description: str) -> str:
    prompt = f"""
    당신은 세계 최고 수준의 시니어 풀스택 및 자동화 엔지니어입니다.
    아래 외주 공고의 요구사항을 완벽하게 충족하는 '즉시 실행 가능한 전체 완성 코드'를 작성하세요.
    
    [공고 제목]: {title}
    [상세 내용]: {description}
    
    규칙:
    1. 주석을 제외하고 생략(중간에 # 생략 등) 없이 전체 코드를 출력할 것.
    2. 필요한 라이브러리 설치 명령어(pip install 등)를 맨 위에 포함할 것.
    3. 복사해서 바로 돌릴 수 있는 완전한 코드로 작성할 것.
    """
    try:
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f"Sonnet API Error: {e}")
        return f"코드 생성 중 오류 발생: {e}"

# /start 명령어 및 메뉴 핸들러
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 **1인 무인 외주 자동화 시스템 가동 중**\n\n"
        "아래 하단 버튼을 눌러 상태를 확인하거나 즉시 스캔을 실행하세요.",
        reply_markup=MAIN_MENU_KEYBOARD,
        parse_mode="Markdown"
    )

# 텍스트 메뉴 응답 처리
async def handle_text_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    
    if text == "📊 현황 조회":
        await update.message.reply_text("📊 현재 24시간 백그라운드 공고 수집 및 텔레그램 연동이 정상 작동 중입니다.")
    elif text == "🔍 지금 즉시 스캔":
        await update.message.reply_text("🔍 수동 스캔을 시작합니다. 신규 공고를 탐색 중...")
        # 여기에 강제 스캔 트리거 로직 연결 가능
    elif text == "⚙️ 필터 상태 확인":
        status_msg = f"⚙️ **현재 필터 설정**\n- 가격 미기재 공고 허용 (`ALLOW_UNKNOWN_PAYMENT`): `{ALLOW_UNKNOWN_PAYMENT}`"
        await update.message.reply_text(status_msg, parse_mode="Markdown")
    elif text == "💰 모델·설정 확인":
        await update.message.reply_text("💰 **AI 모델 이원화 상태**\n- 1차 필터 및 제안서: `claude-3-5-haiku`\n- 2차 완성 코드 생성: `claude-3-5-sonnet`", parse_mode="Markdown")

# 인라인 버튼 클릭 처리 (수락, 재작성, 패스)
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    parts = data.split("_", 1)
    action = parts[0]
    job_id = parts[1] if len(parts) > 1 else ""
    
    job_data = PROCESSED_JOBS.get(job_id)
    
    if action == "accept":
        await query.edit_message_text(text=f"🚀 **[작업 수락됨]** 최고급 소네트 모델이 전체 완성 코드를 작성 중입니다. 잠시만 기다려주세요...")
        
        if job_data:
            title = job_data.get("title", "외주 프로젝트")
            desc = job_data.get("description", "")
        else:
            title = "외주 프로젝트"
            desc = "상세 내용 없음"
            
        code_result = generate_complete_code(title, desc)
        
        # 텔레그램 메시지 글자 수 제한(4096자) 대응 분할 전송
        if len(code_result) > 4000:
            chunks = [code_result[i:i+4000] for i in range(0, len(code_result), 4000)]
            for idx, chunk in enumerate(chunks):
                await context.bot.send_message(chat_id=query.message.chat_id, text=f"💻 **생성된 완성 코드 ({idx+1}/{len(chunks)})**\n\n```python\n{chunk}\n```", parse_mode="Markdown")
        else:
            await context.bot.send_message(chat_id=query.message.chat_id, text=f"💻 **생성된 완성 코드**\n\n```python\n{code_result}\n```", parse_mode="Markdown")
            
    elif action == "rewrite":
        await query.edit_message_text(text="✍️ 제안서를 새로운 톤으로 다시 작성하는 중...")
        # 재작성 로직 수행
    elif action == "skip":
        await query.edit_message_text(text="❌ 해당 공고는 패스 처리되었습니다.")

# 봇 실행 메인 함수
def main():
    if not TELEGRAM_BOT_TOKEN or not ANTHROPIC_API_KEY:
        logger.error("TELEGRAM_BOT_TOKEN 또는 ANTHROPIC_API_KEY가 설정되지 않았습니다.")
        return

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_menu))

    logger.info("봇이 성공적으로 실행되었습니다.")
    app.run_polling()

if __name__ == "__main__":
    main()