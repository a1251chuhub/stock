import requests
import base64
import time
import mysql.connector
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Tuple

# API 設定
API_ID = "03e0704b56-svpw-405o-nfcm-fmmbvb"
API_KEY = "1X5Kl5THj7VAfZ5m6Nv5BGnqb2n8VK8MlKCO2NePb8fYa55Xx38STID7Lt5mNdrB"

# API URL
TOKEN_URL = "https://hdw001.changliu.com.tw/api_v1/token/authorize.php"
STOCKIN_URL = "https://hdw001.changliu.com.tw/api_v1/inventory/stockin_record.php"

# MySQL 設定
DB_CONFIG = {
    "host": "34.136.7.211",
    "user": "a1251chu",
    "password": "Skc6168jemq0!~!",
    "database": "ragic_database"
}

# Ragic 設定
RAGIC_URL = "https://ap9.ragic.com/goodmoonmood/ragicinventory/12"
RAGIC_HEADERS = {
    "Authorization": "Basic aEJ0ellNcGVGbDVhR2pYRGtBbk5kNnFYTUtNL2FWL0VNNkkvSXE5emF6WXB0Y20xejhsSlI0SjJXTUNhUExWREQ1ajFvd2xUVHZVPQ==",
    "Content-Type": "application/json"
}

# Email 設定
EMAIL_CONFIG = {
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
    "sender_email": "richard@goodmoonmood.com",
    "sender_password": "wcbncemxyirbsspy",
    "receiver_emails": ["richard@goodmoonmood.com", "terry@goodmoonmood.com"]
}

def send_email(subject: str, content: str):
    """發送 Email 通知"""
    msg = MIMEMultipart()
    msg['From'] = EMAIL_CONFIG['sender_email']
    msg['To'] = ", ".join(EMAIL_CONFIG['receiver_emails'])
    msg['Subject'] = subject
    
    msg.attach(MIMEText(content, 'plain', 'utf-8'))
    
    try:
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
        server.send_message(msg)
        print("✅ Email 發送成功")
    except Exception as e:
        print(f"❌ Email 發送失敗: {str(e)}")
    finally:
        server.quit()

def get_api_token():
    """取得 API Token"""
    auth_string = f"{API_ID}:{API_KEY}"
    auth_encoded = base64.b64encode(auth_string.encode()).decode()
    
    headers = {
        "Authorization": f"Basic {auth_encoded}",
        "Content-Type": "application/json"
    }
    
    response = requests.get(TOKEN_URL, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        if data["result"]["ok"]:
            return data["data"]["access_token"], int(data["data"]["expires_in"])
    return None, 0

def get_db_connection():
    """建立資料庫連線"""
    return mysql.connector.connect(**DB_CONFIG)

def process_single_day(year: int, month: int, day: int, access_token: str) -> Tuple[int, int, List[str]]:
    """處理單一天的資料"""
    conn = get_db_connection()
    found_records = 0
    updated_records = 0
    errors = []
    
    try:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        params = {
            "y": year,
            "m": month,
            "d": day,
            "pagesize": 100,
            "nowpage": 1
        }
        
        while True:
            response = requests.get(STOCKIN_URL, headers=headers, params=params)
            
            if response.status_code != 200:
                error_msg = f"{year}-{month:02d}-{day:02d} API錯誤: {response.status_code}"
                errors.append(error_msg)
                break
                
            data = response.json()
            if not data.get("result", {}).get("ok", False):
                break
                
            stockin_data = data.get("data", {})
            rows = stockin_data.get("rows", [])
            
            if not rows:
                break
                
            for record in rows:
                stock_no = record.get('stock_no', '無')
                for item in record.get("items", []):
                    cursor = conn.cursor()
                    try:
                        # 查詢 RagicID
                        cursor.execute("""
                            SELECT ID 
                            FROM transferStock 
                            WHERE RAGIC_1004558 = %s 
                            AND RAGIC_1000500 = %s
                        """, (stock_no, item['sku']))
                        
                        result = cursor.fetchone()
                        if result:
                            ragic_id = result[0]
                            found_records += 1
                            
                            # 更新 Ragic
                            update_data = {
                                "1004849": str(item['qty']),
                                "1004850": "Yes"
                            }
                            
                            ragic_response = requests.post(
                                f"{RAGIC_URL}/{ragic_id}",
                                headers=RAGIC_HEADERS,
                                json=update_data,
                                params={"api": ""}
                            )
                            
                            if ragic_response.status_code == 200:
                                updated_records += 1
                            else:
                                errors.append(f"Ragic更新失敗 - ID:{ragic_id}, 商品:{item['sku']}")
                    finally:
                        cursor.close()
            
            if params["nowpage"] >= stockin_data.get("maxpage", 1):
                break
                
            params["nowpage"] += 1
            
    except Exception as e:
        errors.append(f"{year}-{month:02d}-{day:02d} 處理失敗: {str(e)}")
    finally:
        conn.close()
        
    return found_records, updated_records, errors

def main():
    execution_summary = []
    total_found = 0
    total_updated = 0
    all_errors = []
    
    try:
        # 取得今天日期
        today = datetime.now()
        
        # 取得 API Token
        access_token, _ = get_api_token()
        if not access_token:
            raise Exception("無法取得 API Token")
        
        # 處理最近15天的資料
        for i in range(15, -1, -1):
            process_date = today - timedelta(days=i)
            print(f"\n處理 {process_date.strftime('%Y-%m-%d')} 的資料...")
            
            found, updated, errors = process_single_day(
                process_date.year,
                process_date.month,
                process_date.day,
                access_token
            )
            
            total_found += found
            total_updated += updated
            all_errors.extend(errors)
            
            execution_summary.append(
                f"日期: {process_date.strftime('%Y-%m-%d')}\n"
                f"找到記錄: {found}\n"
                f"更新成功: {updated}\n"
                f"{'無錯誤' if not errors else '錯誤: ' + ', '.join(errors)}\n"
            )
            
            # 避免請求過快
            time.sleep(1)
        
        # 準備 Email 內容
        email_content = (
            f"進庫資料處理報告\n\n"
            f"處理期間: {(today - timedelta(days=15)).strftime('%Y-%m-%d')} 到 {today.strftime('%Y-%m-%d')}\n"
            f"總計找到記錄: {total_found}\n"
            f"總計更新成功: {total_updated}\n\n"
            f"詳細執行記錄:\n\n" + "\n".join(execution_summary)
        )
        
        if all_errors:
            email_content += f"\n錯誤記錄:\n" + "\n".join(all_errors)
        
        # 發送 Email
        send_email(
            f"進庫資料處理報告 - {today.strftime('%Y-%m-%d')}",
            email_content
        )
        
    except Exception as e:
        error_content = f"程式執行失敗:\n{str(e)}"
        send_email("❌ 進庫資料處理失敗", error_content)
        raise

if __name__ == "__main__":
    main() 
