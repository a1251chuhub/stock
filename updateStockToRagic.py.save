import mysql.connector
import requests
import base64
import math
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# 資料庫設定
DB_CONFIG = {
    'host': '34.136.7.211',
    'user': 'a1251chu',
    'password': 'Skc6168jemq0!~!',
    'database': 'ragic_database'
}

# Ragic配置
RAGIC_EMAIL = "goodmoonmood@gmail.com"
RAGIC_PASSWORD = "QmJez57!"
RAGIC_BASE_URL = "https://ap9.ragic.com"
RAGIC_INVENTORY_URL = "https://ap9.ragic.com/goodmoonmood/ragicinventory/20008"

# Email配置
GMAIL_USER = "richard@goodmoonmood.com"
GMAIL_PASSWORD = "wcbncemxyirbsspy"  # 應用程式密碼
RECIPIENT_EMAIL = "richard@goodmoonmood.com,terry@goodmoonmood.com"

API_ID = '03e0704b56-svpw-405o-nfcm-fmmbvb'
API_KEY = '1X5Kl5THj7VAfZ5m6Nv5BGnqb2n8VK8MlKCO2NePb8fYa55Xx38STID7Lt5mNdrB'
BASE_URL = 'https://hdw001.changliu.com.tw/api_v1'

def send_update_email(updated_products):
    """發送更新通知email"""
    try:
        msg = MIMEMultipart()
        msg['From'] = GMAIL_USER
        msg['To'] = RECIPIENT_EMAIL
        msg['Subject'] = f"倉庫庫存更新通知 - {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}"

        # 建立email內容
        body = f"""
        執行時間: {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}
        
        已更新的商品庫存資訊：
        
        """
        
        for product in updated_products:
            body += f"""
        商品UID: {product['id']}
        SKU: {product['sku']}
        更新庫存: {product['stock']}
        -------------------------
            """

        msg.attach(MIMEText(body, 'plain'))

        # 使用SSL發送郵件
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.send_message(msg)
            
        print("✅ 更新通知email已成功發送")
    except Exception as e:
        print(f"❌ 發送email時發生錯誤: {str(e)}")

def get_ragic_session_id():
    """獲取Ragic的session ID"""
    auth_url = f"{RAGIC_BASE_URL}/AUTH"
    payload = {
        "u": RAGIC_EMAIL,
        "p": RAGIC_PASSWORD,
        "login_type": "sessionId"
    }
    
    try:
        response = requests.post(auth_url, data=payload)
        response.raise_for_status()
        session_id = response.text.strip()
        
        if session_id == "-1":
            print("❌ Ragic認證失敗")
            return None
        return session_id
    except Exception as e:
        print(f"❌ 獲取Ragic session ID時發生錯誤: {str(e)}")
        return None

def get_access_token():
    credentials = base64.b64encode(f"{API_ID}:{API_KEY}".encode()).decode()
    headers = {'Authorization': f'Basic {credentials}'}
    response = requests.get(f'{BASE_URL}/token/authorize.php', headers=headers)
    data = response.json()
    if data.get('result', {}).get('ok'):
        return data['data']['access_token']
    else:
        print('取得 access token 失敗:', data.get('result', {}).get('message'))
        return None

def get_products_from_db():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT ID, RAGIC_1000602 as sku
        FROM stockInventory
        WHERE RAGIC_3001104 = '00055'
    """)
    products = cursor.fetchall()
    cursor.close()
    conn.close()
    return products

def batch_query_stock(token, sku_list):
    # 分批查詢，每批最多50個
    all_stock = {}
    batch_size = 50
    for i in range(0, len(sku_list), batch_size):
        batch = sku_list[i:i+batch_size]
        sku_str = ','.join(batch)
        url = f"{BASE_URL}/inventory/stock_query.php?sku={sku_str}"
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.get(url, headers=headers)
        data = response.json()
        if data.get('result', {}).get('ok'):
            for item in data['data']['rows']:
                all_stock[item['sku']] = item['stock']
        else:
            print('查詢庫存失敗:', data.get('result', {}).get('message'))
    return all_stock

def update_mysql_stock(product_id, stock):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE stockInventory 
            SET RAGIC_3001107 = %s 
            WHERE ID = %s
        """, (stock, product_id))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"更新MySQL庫存失敗 (ID: {product_id}): {str(e)}")
        return False

def update_ragic_stock(product_id, stock):
    try:
        # 獲取session ID
        session_id = get_ragic_session_id()
        if not session_id:
            return False

        update_url = f"{RAGIC_INVENTORY_URL}/{product_id}?sid={session_id}"
        update_data = {
            "3001107": stock  # Ragic欄位ID
        }
        
        response = requests.post(
            update_url,
            json=update_data,
            headers={'Content-Type': 'application/json'}
        )
        
        return response.status_code == 200
    except Exception as e:
        return False

def main():
    # 1. 取得商品清單
    products = get_products_from_db()
    if not products:
        print("❌ 沒有找到需要處理的商品")
        return

    # 2. 取得API token
    token = get_access_token()
    if not token:
        return

    # 3. 整理SKU清單
    sku_list = [p['sku'] for p in products if p['sku']]
    if not sku_list:
        print("❌ 沒有SKU可查詢")
        return

    # 4. 分批查詢API庫存
    stock_dict = batch_query_stock(token, sku_list)

    # 5. 更新並記錄
    updated_products = []
    print(f"\n{'商品UID':<10} {'SKU':<20} {'更新庫存':<10}")
    print("-" * 40)
    
    for p in products:
        sku = p['sku']
        stock = stock_dict.get(sku, '查無')
        if stock != '查無':
            print(f"{p['ID']:<10} {sku:<20} {stock:<10}")
            mysql_updated = update_mysql_stock(p['ID'], stock)
            ragic_updated = update_ragic_stock(p['ID'], stock)
            if mysql_updated and ragic_updated:
                updated_products.append({
                    'id': p['ID'],
                    'sku': sku,
                    'stock': stock
                })

    # 6. 發送更新通知email
    if updated_products:
        send_update_email(updated_products)
        print(f"\n✅ 已成功更新 {len(updated_products)} 筆商品並發送通知郵件")

if __name__ == "__main__":
    main()
