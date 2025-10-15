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
    
    # URL ban đầu chỉ có thông tin trang đầu tiên
    endpoint = f"/admin/api/2024-04/customers.json?created_at_min={start_date}&created_at_max={end_date}&limit=250"
    next_page_url = f"https://{shop_url}{endpoint}"
    
    all_customers = []
    
    print(f"Bắt đầu lấy dữ liệu khách hàng từ {start_date}...")
    
    # Vòng lặp sẽ tiếp tục chừng nào còn trang tiếp theo
    while next_page_url:
        try:
            response = requests.get(next_page_url, auth=(api_key, password))
            response.raise_for_status()
            
            # Thêm khách hàng từ trang hiện tại vào danh sách tổng
            customers_data = response.json().get('customers', [])
            if customers_data:
                all_customers.extend(customers_data)
                print(f"Đã lấy {len(customers_data)} khách hàng. Tổng số: {len(all_customers)}.")
            
            # Tìm link của trang tiếp theo trong header của response
            next_page_url = None # Reset lại
            if 'Link' in response.headers:
                links = response.headers['Link'].split(', ')
                for link in links:
                    if 'rel="next"' in link:
                        # Trích xuất URL từ trong <...>
                        next_page_url = link[link.find('<')+1:link.find('>')]
                        # Shopify giới hạn tốc độ gọi API, đợi một chút trước khi gọi trang tiếp
                        time.sleep(0.5) 
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
            'tags': customer.get('tags', '')
        })
    
    print(f"Lấy thành công TOÀN BỘ {len(customer_list)} khách hàng mới.")
    return pd.DataFrame(customer_list), end_date