import os
import sys
import time
import pandas as pd
from datetime import datetime
from collections import defaultdict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine

output_dir = r"D:\tiger\realestate"
os.makedirs(output_dir, exist_ok=True)

def is_valid_data_row(row_data):
    return len(row_data) == 9 and row_data[0].endswith("区")

def scrape_data_by_title_blocks(driver):
    rows = driver.find_elements(By.XPATH, "//tr")
    section_name = None
    data_blocks = defaultdict(list)

    title_map = {
        "每日新建商品房可售信息": "可售信息",
        "每日新建商品房未售信息": "未售信息",
        "每日新建商品房签约信息": "签约信息"
    }

    for row in rows:
        text = row.text.strip()

        for k in title_map:
            if k in text:
                section_name = title_map[k]
                print(f"🔍 检测到段落标题：{section_name}")
                break

        cols = row.find_elements(By.TAG_NAME, "td")
        row_data = [col.text.strip() for col in cols]

        if section_name and is_valid_data_row(row_data):
            data_blocks[section_name].append(row_data)
        else:
            if len(row_data) > 0 and section_name:
                print(f"⚠️ 跳过无效行（{len(row_data)}列）: {row_data}")

    return data_blocks

def scrape_housing_data():
    chrome_options = Options()
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    service = Service(executable_path=r"D:\tiger\realestate\chromedriver-win64\chromedriver.exe")
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        driver.get("https://zfcj.gz.gov.cn/zfcj/tjxx/spfxstjxx")
        time.sleep(3)
        print("✅ 页面加载完毕，准备提取数据")

        headers = ["行政区", "住宅套数", "住宅面积", "商业套数", "商业面积",
                   "办公套数", "办公面积", "车位套数", "车位面积"]

        raw_data = scrape_data_by_title_blocks(driver)
        final_data = {}
        for section, rows in raw_data.items():
            df = pd.DataFrame(rows, columns=headers)
            final_data[section] = df

        return final_data

    except Exception as e:
        import traceback
        traceback.print_exc()
        return None
    finally:
        driver.quit()

def save_to_excel(data_dict, output_path):
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for sheet, df in data_dict.items():
                df.to_excel(writer, sheet_name=sheet, index=False)
        print(f"✅ 数据已保存到：{output_path}")
    except PermissionError:
        print(f"❌ 无法保存 Excel 文件（权限错误），请检查是否已打开：\n➡️ {output_path}")
        print("🔒 请关闭 Excel 中相关文件后重试，或修改文件名。")
    except Exception as e:
        print("❌ 保存 Excel 文件时发生未知错误：", str(e))
        import traceback
        traceback.print_exc()


def save_to_mysql(data_dict, data_date):
    from sqlalchemy import text
    engine = create_engine("mysql+pymysql://root:Tiger20041130#@localhost:3306/housing?charset=utf8mb4")

    try:
        with engine.begin() as conn:  # 使用事务管理器，自动提交或回滚
            for data_type, df in data_dict.items():
                # 1️⃣ 删除旧数据
                delete_sql = text("""
                    DELETE FROM housing_stats 
                    WHERE data_date = :data_date AND data_type = :data_type
                """)
                conn.execute(delete_sql, {"data_date": data_date, "data_type": data_type})
                print(f"🧹 已清除旧数据：{data_date} - {data_type}")

                # 2️⃣ 准备插入新数据
                df = df.copy()
                df.insert(0, "data_type", data_type)
                df.insert(0, "data_date", data_date)
                df.columns = [
                    "data_date", "data_type", "region",
                    "residential_units", "residential_area",
                    "commercial_units", "commercial_area",
                    "office_units", "office_area",
                    "parking_units", "parking_area"
                ]

                # 3️⃣ 插入新数据
                df.to_sql("housing_stats", con=conn, if_exists="append", index=False)
                print(f"✅ 已写入数据库表 housing_stats: {data_type}")

    except Exception as e:
        print("❌ 数据库写入失败，已自动回滚：", str(e))
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_date = sys.argv[1]
        print(f"📅 使用命令行参数日期：{input_date}")
    else:
        input_date = input("请输入爬取日期（如 20250415）：")

    try:
        datetime.strptime(input_date, "%Y%m%d")
    except ValueError:
        print("❌ 日期格式不正确，请输入如 20250415")
        exit()

    filename = f"guangzhou_housing_data_{input_date}.xlsx"
    output_path = os.path.join(output_dir, filename)

    result = scrape_housing_data()
    if result:
        save_to_excel(result, output_path)
        save_to_mysql(result, datetime.strptime(input_date, "%Y%m%d").date())
        print("📊 抓取、保存 Excel 和写入数据库成功。")
        for sheet, df in result.items():
            print(f"\n[{sheet}] 前3行示例：\n{df.head(3)}")
    else:
        print("⚠️ 抓取失败，未获取到数据。")
