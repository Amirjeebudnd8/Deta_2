import os
import requests
import zipfile
import io
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import sys

# ------------------------------------------------------------
# تنظیمات
# ------------------------------------------------------------
SYMBOL = "BTCUSDT"
INTERVAL = "5m"
START_YEAR = 2018
END_YEAR = 2026  # سال جاری (تا امروز)
BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"

# ------------------------------------------------------------
# توابع کمکی
# ------------------------------------------------------------
def generate_month_list():
    """تولید لیست سال‌ها و ماه‌ها از START_YEAR تا END_YEAR"""
    months = []
    start_date = datetime(START_YEAR, 1, 1)
    end_date = datetime(END_YEAR, 12, 1)
    current = start_date
    while current <= end_date:
        months.append((current.year, current.month))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months

def download_zip(year, month, max_retries=3):
    """دانلود فایل زیپ یک ماه"""
    file_name = f"{SYMBOL}-{INTERVAL}-{year}-{month:02d}.zip"
    url = f"{BASE_URL}/{SYMBOL}/{INTERVAL}/{file_name}"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return file_name, response.content
            else:
                print(f"⚠️ 404 برای {file_name} (تلاش {attempt+1})")
                return file_name, None
        except Exception as e:
            print(f"❌ خطا در دانلود {file_name}: {e} (تلاش {attempt+1})")
            time.sleep(2)
    return file_name, None

def process_zip(content, year, month):
    """استخراج و تبدیل به DataFrame"""
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f, header=None)
                df.columns = [
                    'open_time', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                    'taker_buy_quote', 'ignore'
                ]
                # تبدیل timestamp به datetime
                df['timestamp'] = pd.to_datetime(df['open_time'], unit='ms')
                # انتخاب ستون‌های اصلی
                df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                # تبدیل به عدد (برای اطمینان)
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    df[col] = pd.to_numeric(df[col])
                return df
    except Exception as e:
        print(f"❌ خطا در پردازش زیپ {year}-{month}: {e}")
        return None

def save_csv_for_month(df, year, month, output_dir="."):
    """ذخیره دیتافریم یک ماه در فایل CSV"""
    if df is None or df.empty:
        return None
    filename = f"{SYMBOL}_{INTERVAL}_{year}_{month:02d}.csv"
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath, index=False, encoding='utf-8')
    print(f"✅ ذخیره شد: {filepath}")
    return filepath

def process_month(year, month):
    """پردازش کامل یک ماه: دانلود، تبدیل، ذخیره"""
    print(f"🔄 شروع پردازش {year}-{month:02d}")
    filename, content = download_zip(year, month)
    if content is None:
        print(f"❌ فایل موجود نیست: {year}-{month:02d}")
        return None
    df = process_zip(content, year, month)
    if df is None:
        return None
    out_file = save_csv_for_month(df, year, month)
    return out_file

# ------------------------------------------------------------
# بخش اصلی: توزیع کار بر اساس chunk
# ------------------------------------------------------------
def main():
    # دریافت شماره چانک از آرگومان خط فرمان
    chunk_id = 0
    if len(sys.argv) > 2 and sys.argv[1] == "--chunk":
        chunk_id = int(sys.argv[2])
    
    all_months = generate_month_list()
    total_months = len(all_months)
    num_chunks = 20  # تعداد کل چانک‌ها (همانند تعریف شده در ماتریکس)
    
    # محاسبه محدوده ماه‌ها برای این چانک
    chunk_size = (total_months + num_chunks - 1) // num_chunks
    start_idx = chunk_id * chunk_size
    end_idx = min(start_idx + chunk_size, total_months)
    my_months = all_months[start_idx:end_idx]
    
    print(f"🔹 چانک {chunk_id}: پردازش {len(my_months)} ماه (از {len(all_months)} ماه کل)")
    
    # پردازش موازی هر ماه با ThreadPool (اختیاری)
    # ولی چون دانلودها سبک هستند، می‌توان پشت سر هم انجام داد
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_month, year, month): (year, month) for year, month in my_months}
        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
    
    # ایجاد یک فایل CSV خلاصه برای این چانک (در صورت نیاز)
    if results:
        all_dfs = []
        for f in results:
            df = pd.read_csv(f)
            all_dfs.append(df)
        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)
            combined['timestamp'] = pd.to_datetime(combined['timestamp'])
            combined.sort_values('timestamp', inplace=True)
            chunk_filename = f"output_chunk_{chunk_id}.csv"
            combined.to_csv(chunk_filename, index=False)
            print(f"📦 فایل ترکیبی چانک {chunk_id} ذخیره شد: {chunk_filename}")
    
    print(f"✅ چانک {chunk_id} تمام شد.")

if __name__ == "__main__":
    main()
