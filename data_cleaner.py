import pandas as pd

def clean_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Làm sạch dữ liệu khách hàng, đặc biệt là email.
    """
    if df.empty or 'email' not in df.columns:
        return df

    print("Bắt đầu làm sạch dữ liệu email...")
    
    # 1. Chuẩn hóa: chữ thường, xóa khoảng trắng
    df['email'] = df['email'].str.lower().str.strip()

    # 2. Sửa các lỗi typo phổ biến
    corrections = {
        '@ymail.com': '@gmail.com',
        '@gmal.com': '@gmail.com',
        '@gmial.com': '@gmail.com',
        '.con': '.com',
        # Thêm các quy tắc khác của bạn ở đây
    }
    
    for wrong, correct in corrections.items():
        df['email'] = df['email'].str.replace(wrong, correct, regex=False)

    # 3. Loại bỏ các dòng không có email sau khi làm sạch
    df.dropna(subset=['email'], inplace=True)
    
    print("Làm sạch dữ liệu hoàn tất.")
    return df