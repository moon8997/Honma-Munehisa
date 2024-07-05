from telegram import ForceReply, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import pandas as pd
import warnings
from datetime import datetime, timedelta, time
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import asyncio

warnings.simplefilter(action='ignore', category=FutureWarning)
# SSL 경고를 비활성화하기 위한 코드
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

async def number_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    command = update.message.text  # Get the text of the message
    # Determine the command number and send the corresponding message
    if command == "/1":
        await update.message.reply_text("거래량 TOP50 검색중..")
        message = fin(command)
        await update.message.reply_text(message)
    elif command == "/2":
        await update.message.reply_text("우량주 TOP50 검색중..")
        message = fin(command)
        await update.message.reply_text(message)
    elif command == "/stop":
        await update.message.reply_text("🔴모든 모니터링 작업을 종료합니다🔴")
        stop_all_jobs()

scheduler = AsyncIOScheduler()  # 스케줄러 인스턴스를 글로벌로 생성

# 각 채팅 및 심볼에 대한 작업을 저장할 딕셔너리
jobs_dict = {}

def format_volume(volume):
    if volume >= 1_000_000:
        return f"{volume / 1_000_000:.2f}M"
    elif volume >= 1_000:
        return f"{volume / 1_000:.2f}K"
    else:
        return str(volume)

# 전역 변수로 최대 거래량을 추적하는 딕셔너리를 선언
max_volume_dict = {}

# start_volume_tracking 함수에서 최대 거래량 저장 로직 추가
async def start_volume_tracking(context, chat_id, symbol):
    # max_volume_dict에서 심볼 존재 여부 확인
    if symbol in max_volume_dict:
        # 이미 존재하는 경우, 오류 메시지 전송
        await context.bot.send_message(
            chat_id=chat_id, 
            text=f"🔴 <b>{symbol}</b>는 이미 모니터링 중입니다. 🔴",
            parse_mode='HTML'
        )
        return

    one_hour_data = yf.download(tickers=symbol, period="1h", interval="5m")
    if not one_hour_data.empty:
        max_volume_last_hour = one_hour_data['Volume'].max()
        formatted_volume = format_volume(max_volume_last_hour)  # 최대 거래량 포맷팅
        await context.bot.send_message(
            chat_id=chat_id, 
            text=f"<b><i>{symbol}</i></b> 의 최대거래량 : <b><i>{formatted_volume}</i></b> \n🟢거래량 모니터링 시작!🟢",
            parse_mode='HTML'
        )

        # 스케줄러 작업 추가 전에 최대 거래량 저장
        max_volume_dict[symbol] = max_volume_last_hour
    else:
        await context.bot.send_message(chat_id=chat_id, text=f"{symbol} 이라는 심볼은 없어요.")
        return

    scheduler = AsyncIOScheduler()
    scheduler.add_job(fetch_and_send_volume, 'interval', minutes=1, args=[context, chat_id, symbol, max_volume_dict])
    scheduler.start()

    if chat_id not in jobs_dict:
        jobs_dict[chat_id] = {}
    jobs_dict[chat_id][symbol] = {'scheduler': scheduler, 'max_volume': max_volume_last_hour}

# fetch_and_send_volume 함수에서 최대 거래량 비교 로직 추가
async def fetch_and_send_volume(context, chat_id, symbol, max_volume_dict):
    data = yf.download(tickers=symbol, period="1d", interval="5m")
    if not data.empty:
        last_volume = data['Volume'].iloc[-2]
        max_volume = max_volume_dict.get(symbol, 0)
        formatted_volume = format_volume(last_volume)  # 거래량 포맷팅
        now = datetime.now().time()
        # 시간대 검사 (23:30 ~ 05:00 이외의 시간에는 작업 중지)
        if not (time(23, 30) <= now or now <= time(5, 0)):
            await context.bot.send_message(
                chat_id=chat_id, 
                text=f"정규장시간이 아닙니다! \n🔴모니터링 작업을 종료합니다🔴",
                parse_mode='HTML'
            )
            stop_all_jobs()
            return
        if last_volume <= 0.3 * max_volume: # 여기
            await context.bot.send_message(
                chat_id=chat_id, 
                text=f"<b><i>{symbol}</i></b>의 현재거래량이 <b><i>{formatted_volume}</i></b>로,\n최대거래량의 30% 이하입니다! \n🔴작업을 종료합니다🔴",
                parse_mode='HTML'
            )
            stop_job(chat_id, symbol)
            return
        if last_volume > max_volume:
            await context.bot.send_message(
                chat_id=chat_id, 
                text=f"<b><i>{symbol}</i></b>의\n🔴새로운 최대거래량 <b><i>{formatted_volume}</i></b>이 발견됐습니다!🔴",
                parse_mode='HTML'
            )
            max_volume_dict[symbol] = last_volume
        # -------------------------------
        # await context.bot.send_message(
        #     chat_id=chat_id, 
        #     text=f"<b><i>{symbol}</i></b>의 현재거래량 : <b><i>{formatted_volume}</i></b> \n최대거래량 {max_volume}",
        #     parse_mode='HTML'
        # )
        # max_volume_dict[symbol] = max_volume * 1.03
        # -------------------------------
    else:
        await context.bot.send_message(chat_id=chat_id, text=f"No data found for {symbol}.")

# 작업을 중지하는 stop_job 함수
def stop_job(chat_id, symbol):
    if chat_id in jobs_dict and symbol in jobs_dict[chat_id]:
        jobs_dict[chat_id][symbol]['scheduler'].shutdown()
        del jobs_dict[chat_id][symbol]
    # max_volume_dict에서 해당 심볼의 최대 거래량 정보를 삭제
    if symbol in max_volume_dict:
        del max_volume_dict[symbol]

def stop_all_jobs():
    # jobs_dict 내 모든 작업에 대해 반복
    for chat_id in list(jobs_dict.keys()):
        for symbol in list(jobs_dict[chat_id].keys()):
            # 각 스케줄러 작업 중지
            if 'scheduler' in jobs_dict[chat_id][symbol]:
                jobs_dict[chat_id][symbol]['scheduler'].shutdown(wait=False)
            # 해당 심볼 삭제
            del jobs_dict[chat_id][symbol]
        if not jobs_dict[chat_id]:
            # chat_id 항목이 비어있으면 삭제
            del jobs_dict[chat_id]

    # max_volume_dict 초기화
    max_volume_dict.clear()

async def monitor_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args
    chat_id = update.effective_chat.id

    # 현재 시간을 현지 시간대로 얻기
    now = datetime.now().time()

    # 지정된 시간대(23:30~05:00)인지 확인
    if not (time(23, 30) <= now or now <= time(5, 0)):
        # 시간대가 아니면 사용자에게 메시지 전송하고 함수 종료
        await update.message.reply_text("해당 기능은 정규장 시간에만 가능합니다.")
        return
    
    if len(args) == 1:
        symbol = args[0].upper()
        await start_volume_tracking(context, chat_id, symbol)
    else:
        await update.message.reply_text("올바른 심볼을 입력해주세요. 예: /monitor AAPL")

async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Echo the user message."""
    await update.message.reply_text(update.message.text)

def main() -> None:
    """Start the bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token("").build()
    # application = Application.builder().token("7071098688:AAG7IRyltKLORSr85UYixRpBKDuxKPf37NY").build()

    # On different commands - answer in Telegram
    application.add_handler(CommandHandler("1", number_command))
    application.add_handler(CommandHandler("2", number_command))
    application.add_handler(CommandHandler("monitor", monitor_command))  # 여기에 추가
    application.add_handler(CommandHandler("stop", number_command))  # 여기에 추가

    # On non command i.e message - echo the message on Telegram
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)


def calculate_sma(data, window):
    """단순 이동 평균(SMA)을 계산하는 함수"""
    return data.rolling(window=window).mean()

def is_downtrend(data, short_window, long_window, index):
    short_sma = calculate_sma(data['Close'], short_window)
    long_sma = calculate_sma(data['Close'], long_window)
    
    # 현재 단기 이동 평균이 장기 이동 평균보다 낮은지 확인
    return short_sma.iloc[index] < long_sma.iloc[index]

def is_hammer(candle, data, index):
    body_ratio = 0.3  # 몸통이 전체 캔들 크기의 30% 이하여야 함
    lower_wick_ratio = 2  # 아래 그림자가 몸통의 2배 이상이어야 함
    upper_shadow_ratio = 0.1  # 윗 그림자가 전체 캔들 크기의 10% 미만이어야 함

    open_price = candle['Open']
    close_price = candle['Close']
    high_price = candle['High']
    low_price = candle['Low']

    body_size = abs(close_price - open_price)
    total_size = high_price - low_price
    lower_wick_size = min(open_price, close_price) - low_price
    upper_shadow_size = high_price - max(open_price, close_price)

    is_small_body = body_size / total_size <= body_ratio
    is_long_lower_wick = lower_wick_size >= lower_wick_ratio * body_size
    is_small_upper_shadow = upper_shadow_size / total_size <= upper_shadow_ratio

    is_same = open_price != close_price
    if index >= 5:  # 20일 이동 평균을 위한 충분한 데이터
        is_downtrend_condition = is_downtrend(data, 20, 50, index)
    else:
        is_downtrend_condition = False

    return is_small_body and is_long_lower_wick and is_small_upper_shadow and is_same and is_downtrend_condition

def is_engulfing(data, index):
    if index == 0:  # 첫 번째 데이터는 비교할 직전 캔들이 없으므로 장악형 캔들이 될 수 없습니다.
        return False

    current_candle = data.iloc[index]
    previous_candle = data.iloc[index - 1]

    current_open = current_candle['Open']
    current_close = current_candle['Close']
    previous_open = previous_candle['Open']
    previous_close = previous_candle['Close']

    # 현재 캔들이 양봉이고, 직전 캔들이 음봉인지 확인
    is_current_bullish = current_close > current_open
    is_previous_bearish = previous_close < previous_open

    # 현재 캔들이 직전 캔들의 몸통을 완전히 덮는지 확인
    is_engulfing = is_current_bullish and is_previous_bearish and \
                   (current_close > previous_open) and (current_open < previous_close)

    if index >= 5:  # 20일 이동 평균을 위한 충분한 데이터
        is_downtrend_condition = is_downtrend(data, 20, 50, index)
    else:
        is_downtrend_condition = False

    return is_engulfing and is_downtrend_condition

def is_piercing_line(data, index): #관통형
    if index == 0:  # 첫 번째 데이터는 비교할 직전 캔들이 없으므로 관통형 캔들이 될 수 없습니다.
        return False

    current_candle = data.iloc[index]
    previous_candle = data.iloc[index - 1]

    current_open = current_candle['Open']
    current_close = current_candle['Close']
    previous_open = previous_candle['Open']
    previous_close = previous_candle['Close']

    # 현재 캔들이 양봉이고, 직전 캔들이 음봉인지 확인
    is_current_bullish = current_close > current_open
    is_previous_bearish = previous_close < previous_open

    # 직전 음봉의 몸통 중간 지점 이상으로 올라왔는지 확인
    midpoint = (previous_open + previous_close) / 2
    is_above_midpoint = current_close > midpoint

    # 현재 캔들의 시가가 직전 캔들의 종가보다 낮은지 확인
    is_open_below_previous_close = current_open < previous_close

    if index >= 5:  # 20일 이동 평균을 위한 충분한 데이터
        is_downtrend_condition = is_downtrend(data, 20, 50, index)
    else:
        is_downtrend_condition = False
        
    return is_current_bullish and is_previous_bearish and is_above_midpoint and is_open_below_previous_close and is_downtrend_condition


def fin(url_type) :
    if(url_type == "/1") :
        url = 'https://www.webull.com/quote/us/actives'
    else :
        url = 'https://www.webull.com/quote/us/options'
    response = requests.get(url)
    html = response.text

    soup = BeautifulSoup(html, 'html.parser')
    rowss = soup.find_all(class_="table-row table-row-hover")[:50]

    symbols = []

    for index, rows in enumerate(rowss):
            name_child = rows.contents[1]
            name_elements = name_child.select('.detail .txt')[0]
            name = name_elements.text
    
            symbols.append(name)

    # print(symbols)
    hammer_dates = {}
    engulfing_dates = {}
    piercing_line_dates = {}

    one_week_ago = datetime.now() - timedelta(days=7)  # 현재로부터 1주일 전의 날짜

    for ticker in symbols[:50]:  
        data = yf.download(ticker, period="1y", interval="1d")
        data.reset_index(inplace=True)  
        for i in range(1, len(data)):  # 첫 번째 캔들은 비교할 직전 캔들이 없으므로 1부터 시작
            candle_date = data.iloc[i]['Date']
            if candle_date.date() >= one_week_ago.date():  # 최근 1주일 이내인지 확인
                if is_hammer(data.iloc[i], data, i):
                    if ticker not in hammer_dates:
                        hammer_dates[ticker] = []
                    hammer_dates[ticker].append(candle_date.date())
                if is_engulfing(data, i):  # 장악형 캔들 확인
                    if ticker not in engulfing_dates:
                        engulfing_dates[ticker] = []
                    engulfing_dates[ticker].append(candle_date.date())

                # engulfing_dates에 동일한 티커와 날짜가 없는 경우에만 추가
                if is_piercing_line(data, i):
                # engulfing_dates에 해당 날짜가 이미 존재하는지 확인
                    if not (ticker in engulfing_dates and candle_date.date() in [date for date in engulfing_dates[ticker]]):
                        if ticker not in piercing_line_dates:
                            piercing_line_dates[ticker] = []
                        piercing_line_dates[ticker].append(candle_date.date())

    def format_candle_patterns(hammer_dates, engulfing_dates, piercing_line_dates):
        output = []

        # 망치형
        output.append("\n--- 망치형 ---")
        for ticker, dates in hammer_dates.items():
            formatted_dates = [date.strftime("%Y. %m. %d") for date in dates]  # 각 날짜를 원하는 형식의 문자열로 변환
            output.append(f"{ticker}: {' '.join(formatted_dates)}")

        # 장악형
        output.append("\n--- 장악형 ---")
        for ticker, dates in engulfing_dates.items():
            formatted_dates = [date.strftime("%Y. %m. %d") for date in dates]
            output.append(f"{ticker}: {' '.join(formatted_dates)}")

        # 관통형
        output.append("\n--- 관통형 ---")
        for ticker, dates in piercing_line_dates.items():
            formatted_dates = [date.strftime("%Y. %m. %d") for date in dates]
            output.append(f"{ticker}: {' '.join(formatted_dates)}")

        # 결과 문자열을 반환
        return '\n'.join(output)
    
    result_string = format_candle_patterns(hammer_dates, engulfing_dates, piercing_line_dates)

    return result_string

if __name__ == "__main__":
    main()




