import requests
import pandas as pd

def subscribe_to_sendy_single(sendy_url, api_key, list_id, customer_data):
    """
    Gửi thông tin của MỘT khách hàng tới Sendy để đăng ký.
    Đây là hàm gọi API đơn lẻ.
    """
    # SỬ DỤNG URL "/subscribe" ĐÃ ĐƯỢC XÁC NHẬN LÀ HOẠT ĐỘNG
    subscribe_url = f"{sendy_url}/subscribe"
    
    payload = {
        'api_key': api_key,
        'list': list_id,
        'email': customer_data.get('email'),
        'name': customer_data.get('name'),
        'boolean': 'true'
    }
    
    try:
        response = requests.post(subscribe_url, data=payload)
        response_text = response.text.lower()

        # Phân tích phản hồi từ Sendy cho một lần subscribe
        if response_text == '1' or "already subscribed" in response_text:
            return "success", response.text
        elif "bounced" in response_text:
            return "bounced", response.text
        else:
            return "failed", response.text
            
    except requests.exceptions.RequestException as e:
        return "failed", str(e)

def sync_customers_to_sendy(sendy_url, api_key, list_id, dataframe):
    """
    Đọc dữ liệu từ DataFrame và đồng bộ lên Sendy BẰNG CÁCH GỌI API LẶP LẠI.
    """
    if dataframe.empty:
        print("Không có dữ liệu khách hàng để đồng bộ lên Sendy.")
        return 0, 0

    print("Bắt đầu đồng bộ khách hàng lên Sendy (phương pháp đơn lẻ)...")
    success_count = 0
    fail_count = 0
    
    total = len(dataframe)
    for index, row in dataframe.iterrows():
        full_name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
        customer = {
            'email': row.get('email'),
            'name': full_name
        }
        
        if not customer['email']:
            continue
        
        # In tiến trình để theo dõi
        print(f"Đang xử lý {index + 1}/{total}: {customer['email']}", end='\r')
            
        status, message = subscribe_to_sendy_single(sendy_url, api_key, list_id, customer)

        if status == "success":
            success_count += 1
        else:
            # Ghi lại các lỗi cụ thể để xem xét
            if status == "bounced":
                # Lỗi bounced không quá nghiêm trọng, chỉ cần ghi nhận
                pass 
            else: # Các lỗi khác (invalid email, etc.)
                print(f"\nLỗi với email {customer['email']}: {message}")
            fail_count += 1
            
    print("\nĐồng bộ Sendy hoàn tất.")
    return success_count, fail_count