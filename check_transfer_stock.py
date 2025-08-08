import mysql.connector
import requests
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

# MySQL 設定（從.env檔案讀取）
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE")
}

# Google Chat webhook URL
CHAT_WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/6c_4iUAAAAE/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=xrKuuCGaZYFj37GoJYS-QhxqZ1J2J8eivCuNx--bHt4"

def send_chat_report(transfer_numbers):
    """發送調撥申請編號通知到 Google Chat webhook"""
    try:
        if not transfer_numbers:
            # 沒有找到符合條件的記錄
            body = f"""
調撥入貨達倉庫檢查報告

📅 檢查時間: {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}
✅ 狀態: 所有調撥申請都有入庫編號或已結案
            """
        else:
            # 有找到符合條件的記錄
            body = f"""
調撥入貨達倉庫檢查報告

📅 檢查時間: {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}
⚠️ 發現 {len(transfer_numbers)} 筆調撥申請未結案且無入庫編號

調撥申請編號:
{chr(10).join([f"• {number}" for number in transfer_numbers])}

請檢查這些調撥申請的入庫狀況。
            """

        # Google Chat訊息格式
        payload = {
            "text": body
        }
        resp = requests.post(CHAT_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code == 200:
            print("✅ 調撥檢查通知已發送至 Google Chat")
        else:
            print(f"❌ Google Chat 通知失敗: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"❌ Google Chat 通知發送失敗: {str(e)}")

def check_transfer_stock():
    """查詢調撥入貨達倉庫但沒有入庫編號且未結案的記錄"""
    try:
        # 建立資料庫連線
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # 執行查詢
        query = """
        SELECT RAGIC_3001934 
        FROM ragic_database.transferStock 
        WHERE RAGIC_3001932 = '00055' 
        AND RAGIC_1004558 = '' 
        AND RAGIC_1004850 = 'No'
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        # 提取調撥申請編號
        transfer_numbers = [row['RAGIC_3001934'] for row in results if row['RAGIC_3001934']]
        
        cursor.close()
        conn.close()
        
        print(f"查詢完成，找到 {len(transfer_numbers)} 筆符合條件的調撥申請")
        
        return transfer_numbers
        
    except Exception as e:
        print(f"❌ 資料庫查詢失敗: {str(e)}")
        return []

def main():
    print("開始檢查調撥入貨達倉庫記錄...")
    
    # 查詢符合條件的調撥申請
    transfer_numbers = check_transfer_stock()
    
    # 發送 Google Chat 通知
    send_chat_report(transfer_numbers)
    
    print("檢查完成！")

if __name__ == "__main__":
    main() 