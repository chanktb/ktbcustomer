import requests
import pandas as pd
from datetime import datetime
import time

def get_new_customers_from_shopify(shop_url, api_key, password, start_date):
    """
    Lấy TOÀN BỘ danh sách khách hàng từ Shopify kể từ một thời điểm nhất định,
    tự động xử lý việc lật trang (pagination).
    """
    end_date = datetime.utcnow().isoformat() + "Z"
    
    # Yêu cầu các trường cần thiết, bao gồm cả 2 trường mới để lọc
    fields = "email,first_name,last_name,phone,tags,accepts_marketing,orders_count"
    endpoint = f"/admin/api/2024-04/customers.json?created_at_min={start_date}&created_at_max={end_date}&limit=250&fields={fields}"
    next_page_url = f"https://{shop_url}{endpoint}"
    
    all_customers = []
    
    print(f"Bắt đầu lấy dữ liệu khách hàng từ {start_date}...")
    
    # Vòng lặp sẽ tiếp tục chừng nào còn trang tiếp theo
    while next_page_url:
        try:
            response = requests.get(next_page_url, auth=(api_key, password))
            response.raise_for_status()
            
            customers_data = response.json().get('customers', [])
            if customers_data:
                all_customers.extend(customers_data)
                print(f"Đã lấy {len(customers_data)} khách hàng. Tổng số: {len(all_customers)}.")
            
            next_page_url = None # Reset lại
            if 'Link' in response.headers:
                links = response.headers['Link'].split(', ')
                for link in links:
                    if 'rel="next"' in link:
                        next_page_url = link[link.find('<')+1:link.find('>')]
                        time.sleep(0.5) # Nghỉ 0.5s để tránh rate limit của Shopify
                        break

        except requests.exceptions.RequestException as e:
            print(f"Lỗi khi gọi Shopify API: {e}")
            return None, end_date

    if not all_customers:
        print("Không có khách hàng mới.")
        return pd.DataFrame(), end_date

    # Chuyển đổi toàn bộ danh sách khách hàng sang DataFrame
    customer_list = []
    for customer in all_customers:
        customer_list.append({
            'email': customer.get('email'),
            'first_name': customer.get('first_name', ''),
            'last_name': customer.get('last_name', ''),
            'phone': customer.get('phone', ''),
            'tags': customer.get('tags', ''),
            'accepts_marketing': customer.get('accepts_marketing'),
            'orders_count': customer.get('orders_count', 0)
        })
    
    print(f"Lấy thành công TOÀN BỘ {len(customer_list)} khách hàng mới.")
    return pd.DataFrame(customer_list), end_date