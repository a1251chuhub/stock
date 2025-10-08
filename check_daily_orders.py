#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
檢查指定客戶在上個月是否每天都有訂單
檢查 ragic_database.purchaseOrder 資料表中的訂單資料
"""

import mysql.connector
from datetime import datetime, timedelta
import calendar
from typing import List, Dict, Set
import sys
import os
import requests
import json

# 添加父目錄到路徑以導入配置
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import DB_CONFIG
except ImportError:
    # 如果無法導入配置，使用硬編碼的配置
    DB_CONFIG = {
        "host": "34.136.7.211",
        "user": "a1251chu", 
        "password": "Skc6168jemq0!~!",
        "database": "ragic_database"
    }

class DailyOrderChecker:
    def __init__(self):
        self.connection = None
        self.target_customers = [
            "waca商城",
            "富邦媒體科技股份有限公司", 
            "新加坡商蝦皮娛樂電商有限公司_倉庫",
            "寶雅國際股份有限公司(門市)"
        ]
        
        # Ragic API 配置
        self.ragic_url = "https://ap14.ragic.com/goodmoonmood/ragicsales-order-management/20001/"
        self.ragic_headers = {
            "Authorization": "Bearer aEJ0ellNcGVGbDVhR2pYRGtBbk5kOWc0V3RIVUkzS0JGdllFMVhhYjJVVnMvT1M2K00rR3ZzMjl1K2F4clowVGRRdmppQXlpSGFvPQ==",
            "Content-Type": "application/json"
        }
        # Google Chat webhook (provided)
        self.google_chat_webhook = "https://chat.googleapis.com/v1/spaces/AAQAFAUPizM/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=tVUYN4kakI3VVERSgpo2Sf8FjsjeGg4BWEBL6f1UgUg"
    
    def connect_database(self):
        """連接到MySQL資料庫"""
        try:
            self.connection = mysql.connector.connect(**DB_CONFIG)
            print("✅ 資料庫連接成功")
            return True
        except mysql.connector.Error as e:
            print(f"❌ 資料庫連接失敗: {e}")
            return False
    
    def get_last_month_date_range(self) -> tuple:
        """獲取上個月的日期範圍"""
        today = datetime.now()
        
        # 計算上個月的第一天和最後一天
        if today.month == 1:
            last_month = 12
            last_year = today.year - 1
        else:
            last_month = today.month - 1
            last_year = today.year
        
        # 上個月第一天
        first_day = datetime(last_year, last_month, 1)
        
        # 上個月最後一天
        last_day_num = calendar.monthrange(last_year, last_month)[1]
        last_day = datetime(last_year, last_month, last_day_num)
        
        print(f"📅 檢查期間: {first_day.strftime('%Y-%m-%d')} 到 {last_day.strftime('%Y-%m-%d')}")
        return first_day, last_day
    
    def get_all_dates_in_range(self, start_date: datetime, end_date: datetime) -> Set[str]:
        """獲取日期範圍內的所有日期"""
        dates = set()
        current_date = start_date
        
        while current_date <= end_date:
            dates.add(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        return dates
    
    def get_customer_orders(self, customer_name: str, start_date: datetime, end_date: datetime) -> Dict[str, int]:
        """查詢指定客戶在指定期間的訂單資料"""
        if not self.connection:
            return {}
        
        try:
            cursor = self.connection.cursor()
            
            # 查詢SQL - 注意RAGIC_3000813是文字格式的日期，需要轉換
            query = """
            SELECT 
                DATE(STR_TO_DATE(RAGIC_3000813, '%Y/%m/%d')) as order_date,
                COUNT(*) as order_count
            FROM purchaseOrder 
            WHERE RAGIC_3000815 = %s
                AND STR_TO_DATE(RAGIC_3000813, '%Y/%m/%d') >= %s
                AND STR_TO_DATE(RAGIC_3000813, '%Y/%m/%d') <= %s
                AND RAGIC_3000813 IS NOT NULL
                AND RAGIC_3000813 != ''
            GROUP BY DATE(STR_TO_DATE(RAGIC_3000813, '%Y/%m/%d'))
            ORDER BY order_date
            """
            
            cursor.execute(query, (customer_name, start_date, end_date))
            results = cursor.fetchall()
            
            # 轉換為字典格式 {日期: 訂單數量}
            orders = {}
            for row in results:
                order_date = row[0].strftime('%Y-%m-%d')
                order_count = row[1]
                orders[order_date] = order_count
            
            cursor.close()
            return orders
            
        except mysql.connector.Error as e:
            print(f"❌ 查詢客戶 {customer_name} 訂單時發生錯誤: {e}")
            return {}
    
    def fetch_waca_sales_data(self, date: str) -> List[tuple]:
        """從wacaProductSaleCount表獲取指定日期的銷售數據"""
        if not self.connection:
            return []
        
        try:
            cursor = self.connection.cursor()
            query = "SELECT productId, num FROM `wacaProductSaleCount` WHERE date = %s"
            cursor.execute(query, (date,))
            sales_data = cursor.fetchall()
            cursor.close()
            return sales_data
        except mysql.connector.Error as e:
            print(f"❌ 查詢waca銷售數據時發生錯誤: {e}")
            return []

    # ----- Shopee helpers (based on existing shopee script) -----
    def fetch_shopee_order_details(self, date_str: str) -> List[Dict]:
        """
        從 shopeeOrder / shopeeOrderDetail 中取得指定日期的商品明細
        回傳每一筆明細 dict 含 product_id, quantity
        date_str: 'YYYY-MM-DD'
        """
        if not self.connection:
            return []
        try:
            formatted = date_str.replace('-', '/')
            cursor = self.connection.cursor(dictionary=True)
            sql = """
                SELECT d.RAGIC_1003252 AS product_id, d.RAGIC_1003253 AS quantity
                FROM shopeeOrderDetail d
                JOIN shopeeOrder o ON d.RAGIC_1003245 = o.RAGIC_1003206
                WHERE o.RAGIC_1003207 != '不成立'
                  AND LEFT(o.RAGIC_1003211, 10) = %s
            """
            cursor.execute(sql, (formatted,))
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except mysql.connector.Error as e:
            print(f"❌ 查詢 Shopee 訂單明細時發生錯誤: {e}")
            return []

    def get_product_stock(self) -> Set[str]:
        """取得 productStock 中的商品編號集合"""
        if not self.connection:
            return set()
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT RAGIC_1000238 FROM productStock")
            product_stock = {row[0] for row in cursor.fetchall()}
            cursor.close()
            return product_stock
        except mysql.connector.Error as e:
            print(f"❌ 查詢 productStock 時發生錯誤: {e}")
            return set()

    def get_product_combinations(self) -> Dict[str, List[tuple]]:
        """取得 productCombination 的組合映射：{combo_id: [(sub_id, qty), ...]}"""
        combos = {}
        if not self.connection:
            return combos
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute("SELECT RAGIC_1004455, RAGIC_1004457, RAGIC_1004459 FROM productCombination")
            for row in cursor.fetchall():
                combo = row.get('RAGIC_1004455', '').strip()
                sub = row.get('RAGIC_1004457', '').strip()
                qty_raw = str(row.get('RAGIC_1004459', '')).strip()
                try:
                    qty = int(qty_raw) if qty_raw and qty_raw.isdigit() else 0
                except:
                    qty = 0
                if combo:
                    combos.setdefault(combo, []).append((sub, qty))
            cursor.close()
            return combos
        except mysql.connector.Error as e:
            print(f"❌ 查詢 productCombination 時發生錯誤: {e}")
            return {}

    def build_shopee_ragic_order(self, date_str: str, product_sales: Dict[str, int]) -> Dict:
        """建立 Shopee 專用的 Ragic 訂單 payload"""
        formatted_date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y/%m/%d')
        post_data = {
            "1000296": f"shopee{date_str}銷售統計",
            "1002169": "業務行銷部",
            "3000815": "新加坡商蝦皮娛樂電商有限公司_倉庫",
            "1001244": "林叡 Jacky",
            "3000813": formatted_date,
            "3000812": "一般訂單",
            "_subtable_3000842": {}
        }
        subtable_entries = {}
        for idx, (pid, qty) in enumerate(product_sales.items()):
            subtable_entries[f"-{idx+1}"] = {"1000349": pid, "1000297": "8", "3000833": str(qty)}
        post_data["_subtable_3000842"] = subtable_entries
        return post_data
    # ----- end Shopee helpers -----
    
    def create_ragic_order(self, date: str, sales_data: List[tuple]) -> bool:
        """為waca商城創建ragic訂單"""
        try:
            # 準備ragic訂單數據
            post_data = {
                "1000296": f"waca{date}銷售統計",
                "1002169": "業務行銷部",
                "3000815": "waca商城",
                "1001244": "竺佳慶 Richard",
                "3000813": f"{date}/01",
                "3000812": "一般訂單",
                "_subtable_3000842": {}
            }
            
            # 創建子表條目
            subtable_entries = {}
            for index, row in enumerate(sales_data):
                product_id, quantity = row
                if quantity != 0:
                    subtable_entries["-" + str(index + 1)] = {
                        "1000349": product_id,  # 商品編號
                        "1000297": "10",        # 固定值
                        "3000833": str(quantity)  # 銷售數量
                    }
            
            post_data["_subtable_3000842"] = subtable_entries
            
            # 發送POST請求到Ragic
            json_data = json.dumps(post_data)
            response = requests.post(self.ragic_url, headers=self.ragic_headers, data=json_data)
            
            if response.status_code == 200:
                print(f"   ✅ 成功為 {date} 創建ragic訂單")
                return True
            else:
                print(f"   ❌ 創建ragic訂單失敗: {response.text}")
                return False
                
        except Exception as e:
            print(f"   ❌ 創建ragic訂單時發生錯誤: {e}")
            return False
    
    def check_daily_orders(self):
        """檢查所有指定客戶的每日訂單"""
        if not self.connect_database():
            return
        
        try:
            # 獲取上個月的日期範圍
            start_date, end_date = self.get_last_month_date_range()
            all_dates = self.get_all_dates_in_range(start_date, end_date)
            
            print(f"\n🔍 檢查 {len(self.target_customers)} 個客戶的每日訂單...")
            print("=" * 60)
            
            # summary accumulator for notification
            run_summary = {
                'period': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                'customers': {}
            }

            for customer in self.target_customers:
                print(f"\n📋 {customer}")
                
                # 查詢該客戶的訂單
                customer_orders = self.get_customer_orders(customer, start_date, end_date)
                
                if not customer_orders:
                    print("   ⚠️  沒有找到任何訂單資料")
                    continue
                
                # 找出缺失的日期
                missing_dates = all_dates - set(customer_orders.keys())
                
                if not missing_dates:
                    print("   ✅ 每天都有訂單")
                    run_summary['customers'][customer] = {'missing': 0, 'auto_created': 0}
                else:
                    print(f"   ❌ 缺少 {len(missing_dates)} 天的訂單:")
                    run_summary['customers'][customer] = {'missing': len(missing_dates), 'auto_created': 0}
                    
                    # 按日期排序並顯示
                    sorted_missing_dates = sorted(missing_dates)
                    for missing_date in sorted_missing_dates:
                        print(f"      {missing_date}")
                    
                    # 如果是waca商城，嘗試自動補單
                    if customer == "waca商城":
                        print(f"\n   🔧 嘗試為waca商城自動補單...")
                        success_count = 0
                        for missing_date in sorted_missing_dates:
                            # 查詢該日期的waca銷售數據
                            sales_data = self.fetch_waca_sales_data(missing_date)
                            
                            if sales_data:
                                # 創建ragic訂單
                                if self.create_ragic_order(missing_date, sales_data):
                                    success_count += 1
                            else:
                                print(f"   ⚠️  {missing_date} 沒有waca銷售數據，無法補單")
                        
                        if success_count > 0:
                            print(f"   ✅ 成功補單 {success_count} 天")
                            run_summary['customers'][customer]['auto_created'] += success_count
                        else:
                            print(f"   ❌ 無法補單，請手動處理")
                    
                    # 如果是 Shopee 倉庫客戶，嘗試自動補單（使用 shopee 的邏輯）
                    if customer == "新加坡商蝦皮娛樂電商有限公司_倉庫":
                        print(f"\n   🔧 嘗試為 Shopee 倉庫自動補單...")
                        success_count = 0

                        # 預先取得商品庫存與組合資訊
                        product_stock = self.get_product_stock()
                        product_combinations = self.get_product_combinations()

                        for missing_date in sorted_missing_dates:
                            # 從 shopee 訂單明細來統計該日銷售數量
                            details = self.fetch_shopee_order_details(missing_date)
                            if not details:
                                print(f"   ⚠️  {missing_date} 沒有 Shopee 訂單明細，無法補單")
                                continue

                            # 統計商品銷售數量，處理組合商品
                            product_sales: Dict[str, int] = {}
                            unknown_products = []
                            excluded = {
                                "promotionsN", "redeem_discount",
                                "memberLevel-0", "memberLevel-1", "memberLevel-2",
                                "memberLevel-3", "memberLevel-4", "memberLevel-5",
                                "promotionsTotal", "coupon", "promotionsBundle", "promotionsAB"
                            }

                            for row in details:
                                pid = str(row.get('product_id', '')).strip()
                                qty_raw = row.get('quantity', 1)
                                try:
                                    qty = int(qty_raw)
                                except:
                                    qty = 1
                                if not pid or pid in excluded or pid.startswith('coupon-'):
                                    continue

                                if pid in product_stock:
                                    product_sales[pid] = product_sales.get(pid, 0) + qty
                                elif pid in product_combinations:
                                    for sub_pid, sub_qty in product_combinations[pid]:
                                        product_sales[sub_pid] = product_sales.get(sub_pid, 0) + sub_qty * qty
                                else:
                                    unknown_products.append(pid)

                            if unknown_products:
                                print(f"   ⚠️  {missing_date} 有無法識別的商品: {', '.join(unknown_products)}")

                            if not product_sales:
                                print(f"   ⚠️  {missing_date} 無銷售數據，跳過建立 Ragic 訂單")
                                continue

                            ragic_order = self.build_shopee_ragic_order(missing_date, product_sales)
                            # 顯示要發送的payload（debug）
                            print("\n   [Ragic 訂單預覽]")
                            print(json.dumps(ragic_order, ensure_ascii=False, indent=2))

                            # 發送到 Ragic
                            try:
                                resp = requests.post(self.ragic_url, headers=self.ragic_headers, data=json.dumps(ragic_order))
                                if resp.status_code == 200:
                                    print(f"   ✅ 成功為 {missing_date} 建立 Ragic 訂單")
                                    success_count += 1
                                else:
                                    print(f"   ❌ 建立 Ragic 訂單失敗: {resp.text}")
                            except Exception as e:
                                print(f"   ❌ 發送 Ragic 請求時發生錯誤: {e}")

                        if success_count > 0:
                            print(f"   ✅ 成功補單 {success_count} 天")
                        else:
                            print(f"   ❌ 無法補單，請手動處理")
                        run_summary['customers'][customer]['auto_created'] += success_count
            
            print("\n" + "=" * 60)
            print("✅ 檢查完成！")

            # 發送 Google Chat 摘要通知
            try:
                self.send_google_chat_summary(run_summary)
            except Exception as e:
                print(f"⚠️ 發送 Google Chat 摘要時發生錯誤: {e}")
            
        except Exception as e:
            print(f"❌ 檢查過程中發生錯誤: {e}")
        finally:
            if self.connection:
                self.connection.close()

    def send_google_chat_summary(self, summary: Dict):
        """將執行摘要發送到 Google Chat webhook"""
        if not self.google_chat_webhook:
            print("⚠️ 未設定 Google Chat webhook，跳過通知")
            return

        # 建立簡短的訊息內容
        lines = []
        lines.append(f"Daily orders check ({summary.get('period')}) 完成")
        for customer, info in summary.get('customers', {}).items():
            lines.append(f"- {customer}: 缺 {info.get('missing', 0)} 天，已自動建立 {info.get('auto_created', 0)} 天")

        payload = {
            "text": "\n".join(lines)
        }

        headers = {"Content-Type": "application/json"}
        try:
            resp = requests.post(self.google_chat_webhook, headers=headers, data=json.dumps(payload), timeout=10)
            if resp.status_code in (200, 201):
                print("✅ 已發送 Google Chat 摘要通知")
            else:
                print(f"⚠️ Google Chat 回應: {resp.status_code} {resp.text}")
        except Exception as e:
            print(f"⚠️ 發送 Google Chat 時發生例外: {e}")

def main():
    """主程式"""
    print("🏪 客戶每日訂單檢查系統")
    print("=" * 50)
    
    checker = DailyOrderChecker()
    checker.check_daily_orders()

if __name__ == "__main__":
    main()
