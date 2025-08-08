import requests
import base64
import time
import mysql.connector
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import os
from pathlib import Path
from dotenv import load_dotenv

# 載入.env檔案
env_path = (
    Path(__file__).resolve().parents[1]  # 往上兩層 -> /python
    / ".env"
)
load_dotenv(dotenv_path=env_path)

# API 設定（從.env檔案讀取）
API_ID = os.getenv("API_ID")
API_KEY = os.getenv("API_KEY")

# API URL（從.env檔案讀取）
TOKEN_URL = os.getenv("TOKEN_URL")
STOCKIN_URL = os.getenv("STOCKIN_URL")

# MySQL 設定（從.env檔案讀取）
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE")
}

# Ragic 設定（從.env檔案讀取）
RAGIC_EMAIL = os.getenv("RAGIC_EMAIL")
RAGIC_PASSWORD = os.getenv("RAGIC_PASSWORD")
RAGIC_BASE_URL = os.getenv("RAGIC_BASE_URL")
RAGIC_INVENTORY_URL = f"{RAGIC_BASE_URL}/goodmoonmood/ragicinventory/12"

# Google Chat webhook URL
CHAT_WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/AAQAeZVeIiE/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=2RYGjPvUKuyZuGORBHBfbzuBH48kaoe2Qjc8zoR5ESE"

def send_chat_report(subject: str, content: str):
    """發送通知到 Google Chat webhook"""
    try:
        # 組訊息內容
        body = f"{subject}\n\n{content}"
        
        # Google Chat訊息格式
        payload = {
            "text": body
        }
        resp = requests.post(CHAT_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code == 200:
            print("✅ 通知已發送至 Google Chat")
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

def update_ragic_stock(ragic_id, qty):
    """更新Ragic庫存（使用帳號密碼方式）"""
    try:
        # 獲取session ID
        session_id = get_ragic_session_id()
        if not session_id:
            return False

        update_url = f"{RAGIC_INVENTORY_URL}/{ragic_id}?sid={session_id}"
        update_data = {
            "1004849": str(qty),  # 數量欄位
            "1004850": "Yes"       # 確認欄位
        }
        
        response = requests.post(
            update_url,
            json=update_data,
            headers={'Content-Type': 'application/json'}
        )
        
        return response.status_code == 200
    except Exception as e:
        print(f"❌ 更新Ragic庫存時發生錯誤 (ID: {ragic_id}): {str(e)}")
        return False

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
                            
                            # 更新 Ragic（使用新的帳號密碼方式）
                            if update_ragic_stock(ragic_id, item['qty']):
                                updated_records += 1
                                print(f"✅ 成功更新 Ragic ID: {ragic_id}, 商品: {item['sku']}, 數量: {item['qty']}")
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
        
        # 準備通知內容
        report_content = (
            f"進庫資料處理報告\n\n"
            f"處理期間: {(today - timedelta(days=15)).strftime('%Y-%m-%d')} 到 {today.strftime('%Y-%m-%d')}\n"
            f"總計找到記錄: {total_found}\n"
            f"總計更新成功: {total_updated}\n\n"
            f"詳細執行記錄:\n\n" + "\n".join(execution_summary)
        )
        
        if all_errors:
            report_content += f"\n錯誤記錄:\n" + "\n".join(all_errors)
        
        # 發送通知（改為Google Chat）
        send_chat_report(
            f"進庫資料處理報告 - {today.strftime('%Y-%m-%d')}",
            report_content
        )
        
    except Exception as e:
        error_content = f"程式執行失敗:\n{str(e)}"
        send_chat_report("❌ 進庫資料處理失敗", error_content)
        raise

if __name__ == "__main__":
    main() 
