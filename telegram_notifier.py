import requests

def send_telegram_message(bot_token, chat_id, text):
    """Gửi tin nhắn thông báo tới một chat Telegram."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'Markdown'
    }
    
    try:
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            print("Đã gửi thông báo tới Telegram.")
        else:
            print(f"Gửi thông báo Telegram thất bại: {response.text}")
    except Exception as e:
        print(f"Lỗi khi gửi thông báo Telegram: {e}")