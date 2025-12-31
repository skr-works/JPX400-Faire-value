import os
import sys
import datetime
import pytz
import requests
import pandas as pd
import yfinance as yf
import json
import concurrent.futures
import jpholiday
import time
import random
import math  # 追加: 平方根計算用
from io import StringIO
import logging

# --- 設定 ---
try:
    config_json = os.environ["SYNC_CONFIG"]
    config = json.loads(config_json)
    
    API_ENDPOINT = config["endpoint"]
    API_USER = config["user"]
    API_TOKEN = config["token"]
    TARGET_ID = config["resource_id"]

except KeyError:
    print("Configuration not found.")
    sys.exit(1)
except json.JSONDecodeError:
    print("Invalid configuration format.")
    sys.exit(1)

# --- ログ抑制 ---
logger = logging.getLogger('yfinance')
logger.setLevel(logging.CRITICAL)

# --- 1. カレンダーチェック ---
def check_calendar():
    jst_tz = pytz.timezone('Asia/Tokyo')
    today = datetime.datetime.now(jst_tz).date()

    if today.weekday() >= 5:
        print("Weekend. Skipping.")
        sys.exit(0)

    if jpholiday.is_holiday(today):
        print(f"Holiday ({jpholiday.holiday_name(today)}). Skipping.")
        sys.exit(0)

    print(f"Market Open: {today}")

# --- 個別銘柄処理 (グレアム数版) ---
def analyze_stock(args):
    code, jp_name = args
    ticker_symbol = f"{code}.T"
    
    time.sleep(0.05) # 高速モード

    info = None
    for i in range(2):
        try:
            stock = yf.Ticker(ticker_symbol)
            info = stock.info
            if info and 'currentPrice' in info:
                break
        except Exception:
            time.sleep(0.5)
            pass
    
    if info is None:
        return {'status': 'error', 'code': code, 'reason': 'Fetch Failed'}

    try:
        price = info.get('currentPrice')
        if price is None:
            return {'status': 'error', 'code': code, 'reason': 'No Price'}

        # --- 1. EPS (1株当たり利益) の取得 ---
        # 予想EPSを優先、なければ実績EPS
        eps = info.get('forwardEps')
        if eps is None:
            eps = info.get('trailingEps')
        
        # それでもなければPERから逆算
        if eps is None:
            pe = info.get('trailingPE')
            if pe and pe > 0:
                eps = price / pe

        # EPSがない、または赤字の場合は計算不能 (グレアム数はルート計算するため正の数必須)
        if eps is None or eps <= 0:
            return {'status': 'error', 'code': code, 'reason': 'Red Ink (EPS <= 0)'}

        # --- 2. BPS (1株当たり純資産) の取得 ---
        bps = info.get('bookValue')
        
        # BPSがない場合、PBRから逆算 (BPS = 株価 / PBR)
        if bps is None:
            pbr = info.get('priceToBook')
            if pbr and pbr > 0:
                bps = price / pbr
        
        # 債務超過(BPSマイナス)の場合は計算不能
        if bps is None or bps <= 0:
            return {'status': 'error', 'code': code, 'reason': 'Deficit (BPS <= 0)'}

        # --- 3. グレアム数 (理論株価) の計算 ---
        # 公式: √ (22.5 * EPS * BPS)
        # 意味: PER 15倍 × PBR 1.5倍 = 22.5 を基準とした理論値
        try:
            graham_number = math.sqrt(22.5 * eps * bps)
        except ValueError:
            return {'status': 'error', 'code': code, 'reason': 'Math Domain Error'}

        fair_value = graham_number
        
        # 割安度 (%)
        upside = ((fair_value - price) / price) * 100
        
        # それでも異常値(例えば+500%など)が出る場合は、データミスの可能性が高いので弾く
        # ※グレアム数で+300%以上はよほどの資産バリュー株でない限り稀
        if upside > 300: 
             return {'status': 'error', 'code': code, 'reason': f'Too High (>300%): {upside:.0f}%'}

        return {
            'status': 'success',
            'id': code,
            'label': jp_name,
            'val': price,
            'target': fair_value,
            'diff': upside,
            'eps': eps,  # 参考データ
            'bps': bps   # 参考データ
        }
        
    except Exception as e:
        return {'status': 'error', 'code': code, 'reason': str(e)}

# --- 2. データ取得 ---
def fetch_target_list():
    print("Fetching index data from SBI Source...")
    url = "https://site1.sbisec.co.jp/ETGate/WPLETmgR001Control?OutSide=on&getFlg=on&burl=search_market&cat1=market&cat2=info&dir=info&file=market_meigara_400.html"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        res = requests.get(url, headers=headers, timeout=20)
        res.encoding = "cp932"
        
        dfs = pd.read_html(StringIO(res.text), attrs={"class": "md-l-table-01"}, header=0)
        
        if not dfs:
            print("Error: Table not found.")
            sys.exit(1)
            
        target_df = dfs[0]
        
        if '銘柄コード' in target_df.columns and '銘柄名' in target_df.columns:
            codes = target_df['銘柄コード'].astype(str).str.strip().tolist()
            names = target_df['銘柄名'].astype(str).str.strip().tolist()
            
            clean_list = []
            for c, n in zip(codes, names):
                if c.isdigit() and len(c) == 4:
                    clean_list.append((c, n))
            
            return clean_list
        else:
            print("Error: Columns mismatch.")
            sys.exit(1)

    except Exception as e:
        print(f"Error fetching list: {e}")
        sys.exit(1)

# --- 3. レポート生成 ---
def build_payload(data):
    today = datetime.datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d')

    html = f"""
    <h3>JPX400 理論株価 ({today})</h3>
    <p style="font-size: 0.8em; margin-bottom: 10px;">
    ベンジャミン・グレアムのミックス係数に基づき算出。<br>
    <span style="background:#eee; padding:2px;">理論値 = √(22.5 × EPS × BPS)</span><br>
    ※資産と利益の両面から見た保守的な適正価格です。
    </p>
    """
    
    html += '<table style="font-size: 10px; line-height: 1.1; border-collapse: collapse; width: 100%; text-align: left;">'
    html += """
    <thead style="background-color: #f4f4f4;">
        <tr>
            <th style="padding: 2px 4px;">コード</th>
            <th style="padding: 2px 4px;">銘柄名</th>
            <th style="padding: 2px 4px;">株価</th>
            <th style="padding: 2px 4px;">理論値</th>
            <th style="padding: 2px 4px;">乖離</th>
        </tr>
    </thead>
    <tbody>
    """
    
    for item in data:
        diff_val = item['diff']
        diff_str = f"{diff_val:+.0f}%"
        
        # 割安(プラス)は赤、割高(マイナス)は青
        color = "#d32f2f" if diff_val > 0 else "#1976d2"
        # 乖離が0に近い場合(適正圏内)は黒にする
        if -10 < diff_val < 10:
            color = "#333"

        diff_html = f'<span style="color: {color}; font-weight: bold;">{diff_str}</span>'
            
        row = f"""
        <tr style="border-bottom: 1px solid #eee;">
            <td style="padding: 2px 4px;"><strong>{item['id']}</strong></td>
            <td style="padding: 2px 4px;">{item['label']}</td>
            <td style="padding: 2px 4px;">{item['val']:,.0f}</td>
            <td style="padding: 2px 4px;">{item['target']:,.0f}</td>
            <td style="padding: 2px 4px;">{diff_html}</td>
        </tr>
        """
        html += row

    html += "</tbody></table>"
    html += f"<br><small style='font-size:9px; color:#777;'>分析対象: {len(data)}銘柄 (除外: 赤字/債務超過/データ欠損)</small>"
    
    return html

# --- 4. リモート同期 ---
def sync_remote_node(content_body):
    print("Syncing with remote node...")
    
    target_url = f"{API_ENDPOINT}/wp-json/wp/v2/pages/{TARGET_ID}"
    
    headers = {
        "Content-Type": "application/json"
    }
    
    payload = {
        'content': content_body
    }
    
    try:
        res = requests.post(
            target_url, 
            json=payload, 
            auth=(API_USER, API_TOKEN),
            headers=headers
        )
        if res.status_code == 200:
            print("Sync complete.")
        else:
            print(f"Sync failed: {res.status_code}")
            sys.exit(1)
    except Exception as e:
        print(f"Connection error: {e}")
        sys.exit(1)

# --- Main ---
if __name__ == "__main__":
    check_calendar()
    
    target_list = fetch_target_list()
    print(f"List loaded: {len(target_list)} stocks found.")
    
    success_results = []
    error_log = []
    
    print(f"Processing {len(target_list)} stocks (Graham Method)...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = list(executor.map(analyze_stock, target_list))
        
    for res in futures:
        if res is None: continue
        if res['status'] == 'success':
            success_results.append(res)
        else:
            error_log.append(res)

    print("-" * 30)
    print(f"Analysis Finished.")
    print(f"Success: {len(success_results)}")
    print(f"Skipped: {len(error_log)}")
    
    # エラー詳細(トップ10)
    if error_log:
        print("\n--- Skip Reasons (Top 10) ---")
        for err in error_log[:10]:
            print(f"[{err['code']}] {err['reason']}")
    print("-" * 30)

    if not success_results:
        print("No valid data found.")
        sys.exit(0)

    # 割安度順にソート
    sorted_data = sorted(success_results, key=lambda x: x['diff'], reverse=True)
    
    report_html = build_payload(sorted_data)
    sync_remote_node(report_html)
