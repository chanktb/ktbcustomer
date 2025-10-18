import requests
import pandas as pd

def subscribe_to_sendy_single(sendy_url, api_key, list_id, customer_data):
    """
    Gửi thông tin của MỘT khách hàng tới MỘT list Sendy.
    """
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

        if response_text == '1' or "already subscribed" in response_text:
            return "success", response.text
        elif "bounced" in response_text:
            return "bounced", response.text
        else:
            return "failed", response.text
            
    except requests.exceptions.RequestException as e:
        return "failed", str(e)

def sync_customers_to_sendy(sendy_url, api_key, list_id_string, dataframe):
    """
    Đồng bộ khách hàng lên NHIỀU list Sendy (bằng phương pháp đơn lẻ).
    """
    if dataframe.empty:
        print("Không có dữ liệu khách hàng để đồng bộ lên Sendy.")
        return 0, 0
    
    # Kiểm tra xem list_id_string có phải là None hoặc rỗng không
    if not list_id_string:
        print("Lỗi: Chuỗi List ID trống hoặc không được cấu hình (None).")
        return 0, 0
        
    # Tách chuỗi ID thành một danh sách các ID
    list_ids = [list_id.strip() for list_id in list_id_string.split(',') if list_id.strip()]
    
    if not list_ids:
        print("Lỗi: Không tìm thấy List ID nào hợp lệ sau khi tách chuỗi.")
        return 0, 0

    print(f"Bắt đầu đồng bộ khách hàng lên {len(list_ids)} list Sendy (phương pháp đơn lẻ)...")
    
    success_count = 0
    fail_count = 0
    total_customers = len(dataframe)
    total_operations = total_customers * len(list_ids)
    current_operation = 0

    # Vòng lặp 1: Lặp qua từng khách hàng
    for index, row in dataframe.iterrows():
        full_name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
        customer = {
            'email': row.get('email'),
            'name': full_name
        }
        
        if not customer['email']:
            continue
            
        # Vòng lặp 2: Lặp qua từng List ID
        for list_id in list_ids:
            current_operation += 1
            progress_message = f"Đang xử lý {current_operation}/{total_operations} (KH: {index + 1}/{total_customers} -> List: {list_id}): {customer['email']}"
            
            # Thêm padding (khoảng trắng) để xóa sạch dòng cũ, tránh lỗi hiển thị 'comcom'
            print(f"{progress_message:<120}", end='\r') 
            
            status, message = subscribe_to_sendy_single(sendy_url, api_key, list_id, customer)

            if status == "success":
                success_count += 1
            else:
                fail_count += 1
                if status != "bounced":
                    # In ra lỗi nếu không phải là lỗi 'bounced'
                    print(f"\nLỗi với email {customer['email']} trên list {list_id}: {message}")
            
    print(f"\nĐồng bộ Sendy hoàn tất. Tổng số lượt đăng ký thành công: {success_count}, Thất bại: {fail_count}")
    return success_count, fail_count