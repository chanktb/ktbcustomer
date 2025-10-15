import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

def get_gsheet_client(creds_json):
    """Xác thực với Google Sheets API sử dụng file JSON của Service Account."""
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json, SCOPE)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"Lỗi xác thực Google Sheets: {e}")
        return None

def update_gsheet(client, sheet_name, worksheet_name, dataframe):
    """Xóa dữ liệu cũ và ghi dữ liệu mới từ DataFrame vào Google Sheet."""
    try:
        print(f"Bắt đầu cập nhật Google Sheet '{sheet_name}'...")
        sheet = client.open(sheet_name).worksheet(worksheet_name)
        
        sheet.clear() # Xóa toàn bộ dữ liệu cũ
        
        # Ghi header và dữ liệu của dataframe vào sheet
        sheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())
        
        print("Cập nhật Google Sheet thành công.")
        return True
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Lỗi: Không tìm thấy Google Sheet với tên '{sheet_name}'")
        return False
    except Exception as e:
        print(f"Lỗi khi cập nhật Google Sheet: {e}")
        return False

def get_all_records_as_dataframe(client, sheet_name, worksheet_name):
    """Đọc toàn bộ dữ liệu từ worksheet và chuyển thành DataFrame."""
    try:
        sheet = client.open(sheet_name).worksheet(worksheet_name)
        records = sheet.get_all_records()
        if not records:
            print(f"Worksheet '{worksheet_name}' trống.")
            return pd.DataFrame()
        return pd.DataFrame(records)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Không tìm thấy worksheet '{worksheet_name}'. Coi như là sheet trống.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Lỗi khi đọc Google Sheet: {e}")
        # Trả về DataFrame rỗng để quy trình không bị dừng
        return pd.DataFrame()