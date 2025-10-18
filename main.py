import os
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import pandas as pd

# Tải biến môi trường từ file .env nếu có (khi chạy local)
load_dotenv()

from shopify_client import get_new_customers_from_shopify
from google_sheets_client import get_gsheet_client, update_gsheet
from sendy_client import sync_customers_to_sendy
from telegram_notifier import send_telegram_message
from data_cleaner import clean_customer_data
from git_utils import commit_and_push

def get_start_date(state_file):
    """Đọc ngày chạy cuối cùng từ file trạng thái."""
    try:
        with open(state_file, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        # Nếu file không tồn tại, lấy mốc 7 ngày trước
        print(f"Không tìm thấy file trạng thái '{state_file}'. Lấy mốc 7 ngày trước.")
        return (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

def save_end_date(state_file, end_date):
    """Lưu thời gian chạy thành công vào file trạng thái."""
    with open(state_file, 'w') as f:
        f.write(end_date)

def run():
    # --- Lấy credentials chung ---
    GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON_CONTENT")
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    SENDY_URL = os.getenv("SENDY_URL")
    SENDY_API_KEY = os.getenv("SENDY_API_KEY")
    COMBINED_GOOGLE_SHEET_NAME = os.getenv("COMBINED_GOOGLE_SHEET_NAME") 
    COMBINED_WORKSHEET_NAME = "New_Customers_Weekly" # Tên tab chứa khách mới hàng tuần

    # Tạo file credentials tạm thời cho Google
    with open("google_creds.json", "w") as f:
        f.write(GOOGLE_CREDS_JSON)
        
    gsheet_client = get_gsheet_client("google_creds.json")
    if not gsheet_client:
        raise Exception("Xác thực Google Sheets thất bại.")

    # --- Đọc file config và chạy vòng lặp cho từng site ---
    with open('config.json', 'r') as f:
        configs = json.load(f)

    full_report = ["*BÁO CÁO ĐỒNG BỘ KHÁCH HÀNG*"]
    any_errors = False
    
    # Danh sách để chứa DataFrame của khách hàng MỚI từ tất cả các site
    all_new_customers_dfs = []

    for config in configs:
        if not config.get("enabled", False):
            continue
        
        site_name = config["site_name"]
        print(f"\n{'='*20}\nBắt đầu xử lý cho site: {site_name}\n{'='*20}")
        
        site_report = [f"*{site_name}*"]
        
        try:
            # Lấy credentials cụ thể cho từng site từ env
            SHOPIFY_SHOP_URL = os.getenv(config["shopify_shop_url_secret"])
            SHOPIFY_API_KEY = os.getenv(config["shopify_api_key_secret"])
            SHOPIFY_PASSWORD = os.getenv(config["shopify_password_secret"])
            # Đọc chuỗi các list ID (phân tách bằng dấu phẩy)
            SENDY_LIST_IDS_STRING = os.getenv(config["sendy_list_ids_secret"]) 
            
            # --- BƯỚC 1: LẤY DỮ LIỆU TỪ SHOPIFY ---
            start_date = get_start_date(config["state_file"])
            customers_df, end_date = get_new_customers_from_shopify(
                SHOPIFY_SHOP_URL, SHOPIFY_API_KEY, SHOPIFY_PASSWORD, start_date
            )
            
            if customers_df is None:
                raise Exception("Lấy dữ liệu Shopify thất bại.")
            
            print(f"Lấy về tổng cộng {len(customers_df)} khách hàng (bao gồm cả 'not subscribed').")

            # --- BƯỚC 2: LỌC KHÁCH HÀNG (ĐÃ MUA HOẶC SUBSCRIBED) ---
            if customers_df.empty:
                print("Không có khách hàng mới, bỏ qua bước lọc.")
                cleaned_df = customers_df # Gán là dataframe rỗng
            else:
                # Nếu có khách hàng thì mới thực hiện lọc
                customers_df['orders_count'] = pd.to_numeric(customers_df['orders_count'])
                
                filtered_df = customers_df[
                    (customers_df['orders_count'] > 0) | 
                    (customers_df['accepts_marketing'] == True)
                ].copy()
                
                print(f"Sau khi lọc, còn lại {len(filtered_df)} khách hàng hợp lệ (đã mua hoặc subscribed).")
                
                cleaned_df = clean_customer_data(filtered_df) 

            site_report.append(f"🛍️ Shopify: Lấy và làm sạch {len(cleaned_df)} khách hàng mới.")
            
            # --- BƯỚC 3: ĐỒNG BỘ SANG SENDY (TÁCH BIỆT) ---
            success, fails = sync_customers_to_sendy(SENDY_URL, SENDY_API_KEY, SENDY_LIST_IDS_STRING, cleaned_df)
            site_report.append(f"📩 Sendy: Đồng bộ vào list riêng (Thành công: {success}, Lỗi: {fails}).")

            # --- BƯỚC 4: THÊM DỮ LIỆU VÀO DANH SÁCH TỔNG HỢP (CHO GOOGLE SHEET) ---
            if not cleaned_df.empty:
                all_new_customers_dfs.append(cleaned_df)
            
            # --- BƯỚC 5: LƯU TRẠNG THÁI & COMMIT ---
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

    # --- BƯỚC 6: XỬ LÝ GOOGLE SHEET CHUNG (SAU KHI LẶP XONG) ---
    if not any_errors:
        if all_new_customers_dfs:
            try:
                print("\nTổng hợp dữ liệu khách hàng MỚI từ tất cả các site...")
                new_customers_df = pd.concat(all_new_customers_dfs, ignore_index=True)
                new_customers_df.drop_duplicates(subset=['email'], inplace=True)
                
                print(f"Tổng số khách hàng mới và duy nhất để thêm vào Google Ads là: {len(new_customers_df)}")
                
                # Thêm các cột trống mà Google Ads yêu cầu
                new_customers_df['country_code'] = ''
                new_customers_df['postal_code'] = ''

                # Ghi đè chỉ khách hàng MỚI vào Google Sheet
                update_success = update_gsheet(gsheet_client, COMBINED_GOOGLE_SHEET_NAME, COMBINED_WORKSHEET_NAME, new_customers_df)
                if not update_success:
                    raise Exception("Cập nhật Google Sheets thất bại.")
                
                full_report.append(f"\n📊 *Google Sheets:* Ghi thành công {len(new_customers_df)} khách hàng mới để Google Ads cộng dồn.")
            
            except Exception as e:
                any_errors = True
                print(f"LỖI khi xử lý Google Sheet: {e}")
                full_report.append(f"\n🚨 *Lỗi Google Sheet:* `{e}`")
        else:
             print("\nKhông có khách hàng mới nào từ tất cả các site, bỏ qua cập nhật Google Sheet.")
    else:
        print("\nBỏ qua việc cập nhật Google Sheet do có lỗi xảy ra ở các bước trước.")


    # --- GỬI BÁO CÁO TỔNG KẾT ---
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