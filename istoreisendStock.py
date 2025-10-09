# istoreisendStock.py
import os
import sys
import json
import time
import hashlib
import requests
import mysql.connector
from typing import Iterator, Dict, Any, Optional, List
from pathlib import Path
from dotenv import load_dotenv

# ======== 必填：環境與帳密 ========
# iStore iSend API
# 正式機 API Base（注意：這裡已含 /IsisWMS-War）
BASE_URL = os.environ.get("ISTOREISEND_BASE_URL", "https://webapi.istoreisend-wms.com/IsisWMS-War")

# iStore iSend 登入帳號（例：SSL****）
USER_NO = os.environ.get("ISTOREISEND_USER_NO", "SSL2233")
# 密碼：可填明碼或 MD5；若填明碼，程式會自動再嘗試 MD5
USER_PASSWORD = os.environ.get("ISTOREISEND_USER_PASSWORD", "c0de3d4ec7931a71b18494028fab07e9")

# 你的 Storage Client（例：SSL****）
STORAGE_CLIENT_NO = os.environ.get("ISTOREISEND_STORAGE_CLIENT_NO", "SSL2233")

# 可選：只撈這個國家（如 "MALAYSIA"），不填就撈有庫存的所有國家
COUNTRY_FILTER = os.environ.get("ISTOREISEND_COUNTRY", "")  # "" 表示不指定
# 可選：ACTIVE / INACTIVE；不填預設為 ACTIVE
SKU_STATUS = os.environ.get("ISTOREISEND_SKU_STATUS", "ACTIVE")

# 每頁筆數（API 要求：只能 1000 或 50000）
PAGE_SIZE = int(os.environ.get("ISTOREISEND_PAGE_SIZE", "1000"))

# --- .env 檔案載入 ---
try:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        print(f"[INFO] Loaded .env file from: {env_path}")
    else:
        print(f"[WARN] .env file not found at: {env_path}. Relying on system environment variables.")
except Exception as e:
    print(f"[WARN] Could not load .env file. Error: {e}")

# --- MySQL 資料庫連線設定 ---
DB_HOST = os.getenv("MYSQL_HOST")
DB_USER = os.getenv("MYSQL_USER")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD")
DB_NAME = os.environ.get("DB_DATABASE", "ragic_database") # 維持原樣

# --- Ragic API 設定 ---
RAGIC_API_URL = "https://ap14.ragic.com/goodmoonmood/ragicinventory/20008"
RAGIC_API_KEY = os.environ.get("RAGIC_API_KEY", "aEJ0ellNcGVGbDVhR2pYRGtBbk5kOWc0V3RIVUkzS0JGdllFMVhhYjJVVnMvT1M2K00rR3ZzMjl1K2F4clowVGRRdmppQXlpSGFvPQ==")

# --- n8n Webhook 設定 ---
N8N_WEBHOOK_URL = "http://34.136.7.211:5678/webhook/b632384a-f258-4060-abf3-91a3872488ee"


# ======== API 路徑（注意：不要重複 /IsisWMS-War） ========
LOGIN_PATH = "/Json/Public/login/"
INV_QUERY_PATH = "/Json/InvEntity/doQueryStorageClientInventoryPage"

# ======== 小工具 ========
def _is_md5_hex(s: str) -> bool:
    return isinstance(s, str) and len(s) == 32 and all(c in "0123456789abcdefABCDEF" for c in s)

def _attach_session_cookies(sess: requests.Session, data: dict):
    """把回傳的 sessionId/sessionPassword 放進 requests session cookie（不帶 domain）"""
    ro = (data or {}).get("returnObject") or {}
    sid = ro.get("sessionId")
    spw = ro.get("sessionPassword")
    if sid:
        sess.cookies.set("sessionId", sid)
    if spw:
        sess.cookies.set("sessionPassword", spw)

# ======== 登入並回傳 session（穩健版） ========
def login(base_url: str, user_no: str, user_password: str) -> requests.Session:
    """
    依序嘗試 4 種登入方式（以提升相容性）：
    1) JSON + 原密碼
    2) JSON + MD5(原密碼)
    3) FORM + 原密碼
    4) FORM + MD5(原密碼)
    成功即回傳帶好 cookie 的 requests.Session()
    """
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    url = base_url.rstrip("/") + LOGIN_PATH

    raw_pwd = user_password or ""
    md5_pwd = raw_pwd if _is_md5_hex(raw_pwd) else hashlib.md5(raw_pwd.encode("utf-8")).hexdigest()

    tries = [
        ("json-raw", "json", {"userNo": user_no, "userPassword": raw_pwd}),
        ("json-md5", "json", {"userNo": user_no, "userPassword": md5_pwd}),
        ("form-raw", "form", {"userNo": user_no, "userPassword": raw_pwd}),
        ("form-md5", "form", {"userNo": user_no, "userPassword": md5_pwd}),
    ]

    last_error = None
    for label, mode, payload in tries:
        try:
            if mode == "json":
                r = s.post(url, json=payload, timeout=30, allow_redirects=True)
            else:
                r = s.post(url, data=payload, timeout=30, allow_redirects=True)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"[WARN] login try={label} http error: {e}")
            last_error = e
            continue

        if data.get("success") is True:
            print(f"[INFO] login try={label} success")
            _attach_session_cookies(s, data)
            return s

        # 顯示伺服器訊息（若有），方便排錯
        msg_list = (((data or {}).get("msgList") or {}).get("msgList")) or []
        if msg_list:
            for m in msg_list:
                print(f"[WARN] login try={label} server msg: {m.get('msgType')} - {m.get('msgCode')}")
        else:
            print(f"[WARN] login try={label} server said success=false (no message). payload_mode={mode}")

        last_error = RuntimeError(f"server rejected login on try={label}")

    raise RuntimeError(f"Login failed after {len(tries)} attempts. Last error: {last_error}")

# ======== 分頁產生器：逐頁查詢庫存 ========
def iter_inventory_pages(
    s: requests.Session,
    base_url: str,
    storage_client_no: str,
    country: str = "",
    sku_status: str = "ACTIVE",
    page_size: int = 100
) -> Iterator[Dict[str, Any]]:
    """
    呼叫 doQueryStorageClientInventoryPage 以分頁取得庫存
    參數：
      storage_client_no: 你的 Storage Client No
      country: 可留空字串（不指定）
      sku_status: ACTIVE / INACTIVE
      page_size: 每頁筆數
    產出：包含目前 offset、頁長度、總筆數、records 的 dict
    """
    url = base_url.rstrip("/") + INV_QUERY_PATH
    current_offset = 0
    total_size: Optional[int] = None

    while True:
        payload = {
            "storageClientInventoryQuery": {
                "storageClientNo": storage_client_no,
                "country": country if country is not None else "",
                # 如需限定單一 SKU 可放入 storageClientSkuNo（字串）；不放則抓全部
                # "storageClientSkuNo": "TESTSKU001",
                "skuStatus": sku_status or "ACTIVE",
            },
            "pageData": {
                "currentLength": page_size,
                "currentOffset": current_offset
            }
        }

        r = s.post(url, json=payload, timeout=60)
        # 若遇到 Unauthorize access 等非 2xx，raise_for_status 會拋例外
        r.raise_for_status()
        data = r.json()

        if not data.get("success"):
            # 伺服器錯誤時把訊息印出
            raise RuntimeError(f"Inventory query failed at offset={current_offset}: {json.dumps(data, ensure_ascii=False)}")

        ro = data.get("returnObject", {}) or {}
        page = ro.get("currentPageData") or []
        total_size = ro.get("totalRecord") or ro.get("totalSize") or total_size

        yield {
            "offset": current_offset,
            "length": ro.get("currentLength"),
            "total": total_size,
            "records": page
        }

        # 決定是否還有下一頁
        if total_size is None:
            if not page or len(page) < page_size:
                break
            current_offset += page_size
        else:
            fetched = current_offset + (len(page) or 0)
            if fetched >= total_size or not page:
                break
            current_offset += page_size

# 避免過快打太多請求
        time.sleep(0.1)

# ======== 資料庫與外部服務互動 ========

def get_existing_inventory_details(
    conn: mysql.connector.MySQLConnection, sc_skus: List[str]
) -> Dict[str, Dict[str, Any]]:
    """從 stockInventory 查詢現有庫存的詳細資訊"""
    if not sc_skus:
        return {}
    
    inventory_map = {}
    try:
        with conn.cursor(dictionary=True) as cursor:
            placeholders = ", ".join(["%s"] * len(sc_skus))
            query = (
                f"SELECT `ID`, `RAGIC_1000602`, `RAGIC_3001106`, `RAGIC_3000606` "
                f"FROM `stockInventory` "
                f"WHERE `RAGIC_1000602` IN ({placeholders}) AND `RAGIC_3001104` = '00070'"
            )
            cursor.execute(query, tuple(sc_skus))
            for row in cursor.fetchall():
                sku = str(row['RAGIC_1000602'])
                inventory_map[sku] = {
                    "db_id": row['ID'],
                    "inventory_id": str(row['RAGIC_3001106']) if row['RAGIC_3001106'] else 'N/A',
                    "p_code": str(row['RAGIC_3000606']) if row['RAGIC_3000606'] else 'N/A'
                }
    except mysql.connector.Error as err:
        print(f"[ERROR] Failed to query existing inventory: {err}")
    return inventory_map

def get_p_codes_from_product_stock(
    conn: mysql.connector.MySQLConnection, sc_skus: List[str]
) -> Dict[str, str]:
    """從 productStock 查詢 P 碼"""
    if not sc_skus:
        return {}
    
    p_code_map = {}
    try:
        with conn.cursor() as cursor:
            placeholders = ", ".join(["%s"] * len(sc_skus))
            query = (
                f"SELECT `RAGIC_1000238`, `RAGIC_3000593` FROM `productStock` "
                f"WHERE `RAGIC_1000238` IN ({placeholders})"
            )
            cursor.execute(query, tuple(sc_skus))
            for sku, p_code in cursor.fetchall():
                if sku and p_code:
                    p_code_map[str(sku)] = str(p_code)
    except mysql.connector.Error as err:
        print(f"[ERROR] Failed to query P-codes: {err}")
    return p_code_map

def create_ragic_inventory_record(sc_sku: str, p_code: str):
    """呼叫 Ragic API 建立一筆新的庫存資料"""
    headers = {"Authorization": f"Ragic-Api-Key {RAGIC_API_KEY}"}
    payload = {
        "1000602": sc_sku,       # 廠商商品料號
        "3001104": "00070",      # 倉庫
        "3000606": p_code,       # P碼庫存編號
        "3001106": f"00070-{p_code}", # 庫存編號
    }
    try:
        r = requests.post(RAGIC_API_URL, data=payload, headers=headers, timeout=30)
        r.raise_for_status()
        response_data = r.json()
        if "status" in response_data and response_data["status"] == "SUCCESS":
            print(f"[INFO] Successfully created Ragic record for scSku: {sc_sku}")
            return True
        else:
            print(f"[WARN] Ragic API reported failure for scSku {sc_sku}: {response_data}")
            return False
    except requests.RequestException as e:
        print(f"[ERROR] Failed to call Ragic API for scSku {sc_sku}: {e}")
        return False

def update_stock_via_webhook(updates: List[Dict[str, Any]]):
    """透過 n8n Webhook 批次更新庫存"""
    if not updates:
        return
    
    print(f"[INFO] Sending {len(updates)} stock updates to n8n webhook...")
    for update in updates:
        try:
            payload = {"id": update["id"], "stock": update["stock"]}
            r = requests.post(N8N_WEBHOOK_URL, json=payload, timeout=15)
            # 不檢查 raise_for_status，因為 webhook 可能回傳非 2xx 但仍成功處理
            print(f"[DEBUG] Webhook for ID {update['id']} sent. Status: {r.status_code}, Response: {r.text[:100]}")
        except requests.RequestException as e:
            print(f"[ERROR] Webhook call failed for ID {update['id']}: {e}")
        time.sleep(0.05) # 輕微延遲避免瞬間流量過大


# ======== 主流程：登入、撈全庫存並印出 ========
def main():
    # 基本檢查
    if not BASE_URL.startswith("https://"):
        print(f"[WARN] BASE_URL 看起來不正確：{BASE_URL}")

    if any(v in ["YOUR_USER_NO", "YOUR_PASSWORD_OR_HASH", "YOUR_STORAGE_CLIENT_NO"] 
           for v in [USER_NO, USER_PASSWORD, STORAGE_CLIENT_NO]):
        print("請先設定 ISTOREISEND_USER_NO / ISTOREISEND_USER_PASSWORD / ISTOREISEND_STORAGE_CLIENT_NO 或直接在檔案頂端填入。")
        sys.exit(1)

    if not all([DB_HOST, DB_USER, DB_PASSWORD]):
        print("[ERROR] Database credentials (MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD) are not fully configured.")
        print("[HINT] Please check your .env file or system environment variables.")
        sys.exit(1)

    print(f"[INFO] Login to {BASE_URL} ...")
    s = login(BASE_URL, USER_NO, USER_PASSWORD)
    print("[INFO] Login OK. cookies:", s.cookies.get_dict())
    print("[INFO] 開始查詢庫存...")

    total_printed = 0
    page_count = 0
    db_conn = None

    try:
        # --- 連線到 MySQL ---
        print(f"[INFO] Connecting to database '{DB_NAME}' on '{DB_HOST}'...")
        db_conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        print("[INFO] Database connected.")

        for page in iter_inventory_pages(
            s, BASE_URL, STORAGE_CLIENT_NO,
            country=COUNTRY_FILTER, sku_status=SKU_STATUS, page_size=PAGE_SIZE
        ):
            page_count += 1
            records = page["records"]
            if not records:
                print(f"[INFO] 第 {page_count} 頁為空（offset={page['offset']}）")
                continue

            sc_skus_on_page = [r.get("storageClientSkuNo") for r in records if r.get("storageClientSkuNo")]
            
            # 1. 查詢現有庫存
            existing_inventory = get_existing_inventory_details(db_conn, sc_skus_on_page)
            
            # 2. 找出需要新建的 SKU
            skus_to_create = [sku for sku in sc_skus_on_page if sku not in existing_inventory]

            if skus_to_create:
                print(f"[INFO] Found {len(skus_to_create)} new SKUs to process: {skus_to_create}")
                # 3. 為新 SKU 查詢 P 碼
                p_codes = get_p_codes_from_product_stock(db_conn, skus_to_create)
                
                # 4. 建立新庫存
                newly_created_skus = []
                for sku in skus_to_create:
                    p_code = p_codes.get(sku)
                    if p_code:
                        if create_ragic_inventory_record(sku, p_code):
                            newly_created_skus.append(sku)
                    else:
                        print(f"[WARN] No P-code found for new SKU '{sku}'. Cannot create Ragic record.")
                
                # 如果有成功建立新紀錄，短暫延遲後重新獲取資訊，以確保拿到 DB ID
                if newly_created_skus:
                    print("[INFO] Re-fetching inventory details to include newly created records...")
                    time.sleep(2) # 等待資料庫同步
                    newly_fetched_details = get_existing_inventory_details(db_conn, newly_created_skus)
                    existing_inventory.update(newly_fetched_details)

            # 5. 準備回寫庫存
            webhook_updates = []
            for rec in records:
                scSku = rec.get("storageClientSkuNo")
                if not scSku:
                    continue
                
                db_info = existing_inventory.get(scSku)
                if db_info and db_info.get("db_id"):
                    webhook_updates.append({
                        "id": db_info["db_id"],
                        "stock": rec.get("availableQty", 0)
                    })
                else:
                    print(f"[WARN] No database ID found for scSku '{scSku}'. Cannot schedule stock update.")

            # 執行 webhook 更新
            if webhook_updates:
                update_stock_via_webhook(webhook_updates)

            # --- 印出本頁結果 ---
            for rec in records:
                scSku = rec.get("storageClientSkuNo")
                db_info = existing_inventory.get(scSku, {})
                
                print(
                    f"[{rec.get('country') or '-'}][{rec.get('skuStatus') or '-'}] "
                    f"ID={db_info.get('db_id', 'N/A')}  "
                    f"P碼={db_info.get('p_code', 'N/A')}  "
                    f"庫存編號={db_info.get('inventory_id', 'N/A')}  "
                    f"scSku={scSku}  desc={rec.get('skuDesc')}  "
                    f"available={rec.get('availableQty')}"
                )
                total_printed += 1

        print(f"\n[INFO] 完成，總筆數（螢幕列出）：{total_printed}")

    except mysql.connector.Error as db_e:
        print(f"[ERROR] Database connection failed: {db_e}")
        if "Access denied" in str(db_e):
            print("[HINT] 請檢查 DB_HOST, DB_USER, DB_PASSWORD 環境變數是否正確。")
        elif "Unknown database" in str(db_e):
            print(f"[HINT] 請檢查資料庫 '{DB_NAME}' 是否存在。")
        sys.exit(4)

    except requests.HTTPError as http_e:
        # 例如 401/403/500 等
        print("[ERROR] HTTP error:", http_e)
        if http_e.response is not None:
            try:
                print("[ERROR] Response JSON:", http_e.response.json())
            except Exception:
                print("[ERROR] Response Text:", http_e.response.text)
        sys.exit(2)

    except Exception as e:
        print("[ERROR] Unexpected error:", repr(e))
        sys.exit(3)
        
    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()
            print("[INFO] Database connection closed.")

if __name__ == "__main__":
    main()
