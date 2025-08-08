import mysql.connector
import requests
import base64
import math
from datetime import datetime
import os
from pathlib import Path
from dotenv import load_dotenv

# 載入.env檔案
env_path = (
    Path(__file__).resolve().parents[1]  # 往上兩層 -> /python
    / ".env"
)
load_dotenv(dotenv_path=env_path)

# 資料庫設定（從.env檔案讀取）
DB_CONFIG = {
    'host': os.getenv("MYSQL_HOST"),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE")
}

# Ragic配置（從.env檔案讀取）
RAGIC_EMAIL = os.getenv("RAGIC_EMAIL")
RAGIC_PASSWORD = os.getenv("RAGIC_PASSWORD")
RAGIC_BASE_URL = os.getenv("RAGIC_BASE_URL")
RAGIC_INVENTORY_URL = f"{RAGIC_BASE_URL}/goodmoonmood/ragicinventory/20008"

# Google Chat webhook URL
CHAT_WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/AAQAeZVeIiE/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=2RYGjPvUKuyZuGORBHBfbzuBH48kaoe2Qjc8zoR5ESE"

API_ID = '03e0704b56-svpw-405o-nfcm-fmmbvb'
API_KEY = '1X5Kl5THj7VAfZ5m6Nv5BGnqb2n8VK8MlKCO2NePb8fYa55Xx38STID7Lt5mNdrB'
BASE_URL = 'https://hdw001.changliu.com.tw/api_v1'

def send_chat_report(updated_products):
    """發送更新通知到 Google Chat webhook"""
    try:
        # 組訊息內容
        body = f"""
倉庫庫存更新通知 - {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}

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

        # Google Chat訊息格式
        payload = {
            "text": body
        }
        resp = requests.post(CHAT_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code == 200:
            print("✅ 更新通知已發送至 Google Chat")
        else:
            print(f"❌ Google Chat 通知失敗: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"❌ Google Chat 通知發送失敗: {str(e)}")

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

    # 6. 發送更新通知（改為Google Chat）
    if updated_products:
        send_chat_report(updated_products)
        print(f"\n✅ 已成功更新 {len(updated_products)} 筆商品並發送通知")

if __name__ == "__main__":
    main()
