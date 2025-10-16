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

    # 2. Lọc bỏ tất cả các dòng có email chứa "tiktokw.us"
    initial_count = len(df)
    # THÊM .copy() ĐỂ TRÁNH LỖI SettingWithCopyWarning
    df = df[~df['email'].str.contains("tiktokw.us", na=False)].copy().reset_index(drop=True) 
    removed_count = initial_count - len(df)
    if removed_count > 0:
        print(f"Đã lọc bỏ {removed_count} khách hàng có email chứa 'tiktokw.us'.")

    # 3. Sửa các lỗi typo phổ biến một cách an toàn
    # Logic cũ quá rủi ro, chúng ta sẽ dùng logic an toàn hơn
    def safer_corrections(email):
        if not isinstance(email, str):
            return email
        
        # Sửa các lỗi tên miền an toàn
        email = email.replace('@ymail.com', '@gmail.com')
        email = email.replace('@gmal.com', '@gmail.com')
        email = email.replace('@gmial.com', '@gmail.com')
        
        # Sửa lỗi đuôi TLD một cách cẩn thận
        # Chỉ sửa nếu email KẾT THÚC BẰNG chuỗi lỗi
        if email.endswith('.con'):
            email = email[:-4] + '.com'
        if email.endswith('.cơm'):
            email = email[:-4] + '.com'
            
        return email

    df['email'] = df['email'].apply(safer_corrections)

    # 4. Loại bỏ các dòng không có email sau khi làm sạch
    df.dropna(subset=['email'], inplace=True)
    
    print("Làm sạch dữ liệu hoàn tất.")
    return df