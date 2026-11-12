import os
import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler

# 로깅 설정
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# 1. 깃허브 보안 경고를 피하기 위해 환경 변수에서 토큰을 불러옵니다.
# (실제 토큰 값은 나중에 Render 설정 화면의 Environment Variables에 입력합니다.)
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# 봇 시작 명령어 (/start) 핸들러
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_name = update.effective_user.first_name
    await update.message.reply_text(
        f"안녕하세요, {user_name}님! 2분봇이 정상적으로 작동을 시작했습니다. 🚀\n"
        "공고 분석 및 반자동 승인 시스템 대기 중입니다."
    )

def main():
    if not TELEGRAM_BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN 환경 변수가 설정되지 않았습니다.")
        return

    # 텔레그램 봇 애플리케이션 빌드
    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # 명령어 핸들러 등록
    application.add_handler(CommandHandler("start", start))

    print("봇을 실행합니다...")
    # 봇 실행 (Polling 방식)
    application.run_polling()

if __name__ == '__main__':
    main()