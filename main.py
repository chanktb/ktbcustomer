import os
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

from shopify_client import get_new_customers_from_shopify
from google_sheets_client import get_gsheet_client, update_gsheet, get_all_records_as_dataframe
from sendy_client import sync_customers_to_sendy
from telegram_notifier import send_telegram_message
from data_cleaner import clean_customer_data
from git_utils import commit_and_push

def get_start_date(state_file):
    try:
        with open(state_file, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"Không tìm thấy file trạng thái '{state_file}'. Lấy mốc 7 ngày trước.")
        return (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

def save_end_date(state_file, end_date):
    with open(state_file, 'w') as f:
        f.write(end_date)

def run():
    GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON_CONTENT")
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    SENDY_URL = os.getenv("SENDY_URL")
    SENDY_API_KEY = os.getenv("SENDY_API_KEY")
    COMBINED_GOOGLE_SHEET_NAME = os.getenv("COMBINED_GOOGLE_SHEET_NAME") 
    COMBINED_WORKSHEET_NAME = "All_Customers"

    with open("google_creds.json", "w") as f:
        f.write(GOOGLE_CREDS_JSON)
        
    gsheet_client = get_gsheet_client("google_creds.json")
    if not gsheet_client:
        raise Exception("Xác thực Google Sheets thất bại.")

    with open('config.json', 'r') as f:
        configs = json.load(f)

    full_report = ["*BÁO CÁO ĐỒNG BỘ KHÁCH HÀNG*"]
    any_errors = False
    all_new_customers_dfs = []

    for config in configs:
        if not config.get("enabled", False):
            continue
        
        site_name = config["site_name"]
        print(f"\n{'='*20}\nBắt đầu xử lý cho site: {site_name}\n{'='*20}")
        site_report = [f"*{site_name}*"]
        
        try:
            SHOPIFY_SHOP_URL = os.getenv(config["shopify_shop_url_secret"])
            SHOPIFY_API_KEY = os.getenv(config["shopify_api_key_secret"])
            SHOPIFY_PASSWORD = os.getenv(config["shopify_password_secret"])
            SENDY_LIST_ID = os.getenv(config["sendy_list_id_secret"])
            
            start_date = get_start_date(config["state_file"])
            customers_df, end_date = get_new_customers_from_shopify(
                SHOPIFY_SHOP_URL, SHOPIFY_API_KEY, SHOPIFY_PASSWORD, start_date
            )
            
            if customers_df is None:
                raise Exception("Lấy dữ liệu Shopify thất bại.")
            
            cleaned_df = clean_customer_data(customers_df)
            site_report.append(f"🛍️ Shopify: Lấy và làm sạch {len(cleaned_df)} khách hàng mới.")
            
            success, fails = sync_customers_to_sendy(SENDY_URL, SENDY_API_KEY, SENDY_LIST_ID, cleaned_df)
            site_report.append(f"📩 Sendy: Đồng bộ vào list riêng (Thành công: {success}, Lỗi: {fails}).")

            if not cleaned_df.empty:
                all_new_customers_dfs.append(cleaned_df)
            
            save_end_date(config["state_file"], end_date)
            commit_and_push(
                file_path=config["state_file"],
                commit_message=f"Update state file for {site_name} to {end_date}"
            )
        
        except Exception as e:
            any_errors = True
            print(f"LỖI khi xử lý site {site_name}: {e}")
            site_report.append(f"🚨 *Lỗi:* `{e}`")
        
        full_report.append("\n".join(site_report))

    if not any_errors:
        try:
            print("\nTổng hợp dữ liệu để cập nhật Google Sheet...")
            
            # BƯỚC MỚI 1: Đọc dữ liệu cũ từ Google Sheet
            print("Đọc dữ liệu khách hàng cũ từ Google Sheet...")
            existing_customers_df = get_all_records_as_dataframe(gsheet_client, COMBINED_GOOGLE_SHEET_NAME, COMBINED_WORKSHEET_NAME)
            
            # BƯỚC MỚI 2: Gộp dữ liệu mới
            if all_new_customers_dfs:
                new_customers_df = pd.concat(all_new_customers_dfs, ignore_index=True)
                
                # BƯỚC MỚI 3: Gộp cũ và mới, sau đó lọc trùng
                final_df = pd.concat([existing_customers_df, new_customers_df], ignore_index=True)
                final_df.drop_duplicates(subset=['email'], keep='last', inplace=True)
            else:
                # Nếu không có khách mới, danh sách cuối cùng chính là danh sách cũ
                final_df = existing_customers_df

            print(f"Tổng số khách hàng duy nhất sau khi cập nhật là: {len(final_df)}")
            
            # BƯỚC MỚI 4: Cập nhật lại toàn bộ danh sách tổng hợp
            update_success = update_gsheet(gsheet_client, COMBINED_GOOGLE_SHEET_NAME, COMBINED_WORKSHEET_NAME, final_df)
            if not update_success:
                raise Exception("Cập nhật Google Sheets chung thất bại.")
            
            full_report.append(f"\n📊 *Google Sheets Tổng hợp:* Cập nhật thành công. Tổng số khách hàng trong danh sách là {len(final_df)}.")
        
        except Exception as e:
            any_errors = True
            print(f"LỖI khi xử lý Google Sheet chung: {e}")
            full_report.append(f"\n🚨 *Lỗi Google Sheet chung:* `{e}`")
    else:
        print("\nBỏ qua việc cập nhật Google Sheet chung do có lỗi xảy ra ở các bước trước.")

    final_report = "\n\n".join(full_report)
    if any_errors:
        final_report += "\n\n⚠️ *Quy trình có lỗi, vui lòng kiểm tra lại log.*"
        
    send_telegram_message(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, final_report)

    if os.path.exists("google_creds.json"):
        os.remove("google_creds.json")

    if any_errors:
        raise Exception("Một hoặc nhiều luồng công việc đã gặp lỗi.")

if __name__ == "__main__":
    run()