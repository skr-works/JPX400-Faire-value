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

# --- グローバル変数: スキップ理由の集計用 ---
skip_reasons = {
    "No_Price": 0,
    "No_EPS": 0,
    "Abnormal_Data": 0,
    "Fetch_Error": 0
}

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

# --- 個別銘柄処理 (リトライと強化版) ---
def analyze_stock(args):
    code, jp_name = args
    ticker_symbol = f"{code}.T"
    
    # ブロック回避のためのランダム待機
    time.sleep(random.uniform(1.0, 3.0))

    # リトライ処理 (最大3回)
    info = None
    for i in range(3):
        try:
            stock = yf.Ticker(ticker_symbol)
            # info取得を試みる
            info = stock.info
            if info and 'currentPrice' in info:
                break # 成功したらループを抜ける
        except Exception:
            time.sleep(2) # エラー時は少し待って再試行
            pass
    
    # 3回やってもダメ、または必須データがない場合
    if info is None:
        skip_reasons["Fetch_Error"] += 1
        return None

    try:
        price = info.get('currentPrice')
        if price is None:
            skip_reasons["No_Price"] += 1
            return None

        # EPS取得 (優先順位: forward -> trailing -> 0)
        # 日本株はforwardEpsが欠損していることが多い
        eps = info.get('forwardEps')
        if eps is None:
            eps = info.get('trailingEps')
            
        # EPSがどうしても取れない、または赤字(マイナス)の場合は計算不能
        if eps is None or eps <= 0:
            skip_reasons["No_EPS"] += 1
            return None

        # 成長率 (earningsGrowthがない場合が多いので、revenueGrowthを見る。なければ0とする)
        growth_raw = info.get('earningsGrowth')
        if growth_raw is None:
            growth_raw = info.get('revenueGrowth')
        
        # 成長率不明時は 0% (保守的) とする ※以前は5%だったが、より確実に通すため0%採用
        if growth_raw is None:
            growth_raw = 0.0

        # 配当利回り
        yield_raw = info.get('dividendYield', 0)
        if yield_raw is None: yield_raw = 0

        # 計算
        growth_pct = growth_raw * 100
        yield_pct = yield_raw * 100
        
        # 成長率キャップ (最大25%)
        capped_growth = min(growth_pct, 25.0)
        if capped_growth < 0: capped_growth = 0
        
        # ピーター・リンチ式 適正株価 = EPS * (成長率 + 配当利回り)
        # 成長率+配当が低すぎる(例えば合計3%未満)と適正株価が極端に低くなるが、計算としては正しい
        multiplier = capped_growth + yield_pct
        
        # マルチプライヤーが低すぎる場合の補正（最低でもPER 5倍程度はあると仮定するなどの調整も可能だが、今回は式の通りにする）
        if multiplier < 1.0: 
             # 成長+配当がほぼ0の企業は適正株価が出ないためスキップ
             skip_reasons["Abnormal_Data"] += 1
             return None

        fair_value = eps * multiplier
        
        # 割安度
        upside = ((fair_value - price) / price) * 100
        
        # 異常値除外 (1000%以上はデータエラーの可能性が高いので弾く)
        if upside > 1000: 
            skip_reasons["Abnormal_Data"] += 1
            return None

        return {
            'id': code,
            'label': jp_name,
            'val': price,
            'target': fair_value,
            'diff': upside
        }
        
    except Exception:
        skip_reasons["Abnormal_Data"] += 1
        return None

# --- 2. データ取得 (SBIソース) ---
def fetch_target_list():
    print("Fetching index data from SBI Source...")
    url = "https://site1.sbisec.co.jp/ETGate/WPLETmgR001Control?OutSide=on&getFlg=on&burl=search_market&cat1=market&cat2=info&dir=info&file=market_meigara_400.html"
    
    # 偽装ヘッダーを追加
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        res = requests.get(url, headers=headers, timeout=15)
        res.encoding = "cp932"
        
        dfs = pd.read_html(res.text)
        target_df = None
        for df in dfs:
            if df.shape[1] >= 2 and df.iloc[:, 0].astype(str).str.match(r'\d{4}').any():
                target_df = df
                break
        
        if target_df is None:
            if len(dfs) > 1:
                target_df = dfs[1]
            else:
                print("Error: Table not found.")
                sys.exit(1)

        codes = target_df.iloc[:, 0].astype(str).str.zfill(4).tolist()
        names = target_df.iloc[:, 1].astype(str).tolist()
        
        # 重複削除
        unique_list = []
        seen = set()
        for c, n in zip(codes, names):
            if c not in seen:
                unique_list.append((c, n))
                seen.add(c)
        
        return unique_list

    except Exception as e:
        print(f"Error fetching list: {e}")
        sys.exit(1)

# --- 3. レポート生成 (日本語・超コンパクト) ---
def build_payload(data):
    today = datetime.datetime.now(pytz.timezone('Asia/Tokyo')).strftime('%Y/%m/%d')

    html = f"""
    <h3>JPX400 適正株価分析 ({today})</h3>
    <p style="font-size: 0.8em; margin-bottom: 10px;">ピーター・リンチ指標(成長率上限25%)に基づく試算。<br>※投資判断の参考情報であり、正確性を保証しません。</p>
    """
    
    html += '<table style="font-size: 10px; line-height: 1.1; border-collapse: collapse; width: 100%; text-align: left;">'
    html += """
    <thead style="background-color: #f4f4f4;">
        <tr>
            <th style="padding: 2px 4px;">コード</th>
            <th style="padding: 2px 4px;">銘柄名</th>
            <th style="padding: 2px 4px;">株価</th>
            <th style="padding: 2px 4px;">適正</th>
            <th style="padding: 2px 4px;">割安度</th>
        </tr>
    </thead>
    <tbody>
    """
    
    for item in data:
        diff_val = item['diff']
        diff_str = f"{diff_val:+.0f}%"
        
        color = "#d32f2f" if diff_val > 0 else "#1976d2"
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
    html += f"<br><small style='font-size:9px; color:#777;'>分析対象: {len(data)}銘柄 (除外: 赤字/データ欠損)</small>"
    
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
    print(f"List loaded: {len(target_list)} stocks.")
    
    results = []
    
    # 並列数を少し制限して確実に取得する (max_workers=4)
    # 時間がかかっても良いとのことなので、安全策をとる
    print("Processing stocks... (This may take a few minutes)")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = list(executor.map(analyze_stock, target_list))
        
    for res in futures:
        if res is not None:
            results.append(res)

    print("-" * 30)
    print(f"Analysis Finished.")
    print(f"Success: {len(results)}")
    print(f"Skipped Details:")
    print(f"  - No EPS/Red Ink: {skip_reasons['No_EPS']}")
    print(f"  - No Price Data : {skip_reasons['No_Price']}")
    print(f"  - API Error     : {skip_reasons['Fetch_Error']}")
    print(f"  - Data Abnormal : {skip_reasons['Abnormal_Data']}")
    print("-" * 30)

    if not results:
        print("No valid data found.")
        sys.exit(0)

    sorted_data = sorted(results, key=lambda x: x['diff'], reverse=True)
    
    report_html = build_payload(sorted_data)
    sync_remote_node(report_html)
