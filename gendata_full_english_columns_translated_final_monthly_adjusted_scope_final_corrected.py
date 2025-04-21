import calendar
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 原始数据
base_data = {
    "可售信息": {
        "columns": ["行政区", "residential_units", "residential_area", "commercial_units", "commercial_area", "office_units", "office_area", "parking_units", "parking_area"],
        "data": [
            ["越秀区", 522, 60246, 93, 71014, 265, 24436, 1265, 35553],
            ["荔湾区", 5220, 659759, 1612, 178913, 1571, 113789, 17215, 994708],
            ["海珠区", 3730, 494485, 1560, 263624, 2465, 460907, 10933, 419523],
            ["天河区", 3930, 535509, 708, 185842, 4409, 517585, 11073, 319901],
            ["白云区", 9794, 1022891, 921, 116548, 7672, 507880, 21077, 567039],
            ["黄埔区", 10223, 1162160, 992, 200491, 2520, 380531, 48306, 1022810],
            ["番禺区", 12000, 1555258, 3304, 388547, 3101, 297979, 96392, 1741274],
            ["花都区", 9501, 1073667, 1003, 149883, 286, 84230, 30993, 550380],
            ["南沙区", 11782, 1324279, 3303, 394180, 2767, 384890, 84831, 1237991],
            ["从化区", 4652, 485309, 481, 51356, 11, 685, 20073, 331333],
            ["增城区", 19700, 1934656, 2695, 318982, 1774, 158479, 116282, 1493805]
        ]
    },
    "未售信息": {
        "columns": ["行政区", "residential_units", "residential_area", "commercial_units", "commercial_area", "office_units", "office_area", "parking_units", "parking_area"],
        "data": [
            ["越秀区", 2032, 259474, 2206, 422684, 2402, 360664, 4996, 191271],
            ["荔湾区", 9189, 1049442, 7408, 962002, 5490, 410737, 39835, 1622296],
            ["海珠区", 5901, 776795, 5056, 1362973, 9010, 1820574, 33209, 913437],
            ["天河区", 10653, 1457645, 2601, 1019095, 12956, 3084333, 37267, 1400843],
            ["白云区", 16040, 1687076, 2953, 583323, 13097, 1063657, 35323, 1165345],
            ["黄埔区", 21641, 2610618, 5635, 1154040, 9380, 1746692, 103468, 2595825],
            ["番禺区", 21126, 2766511, 8642, 1343963, 8522, 767601, 130998, 2628243],
            ["花都区", 21502, 2529057, 5646, 1423445, 2383, 269468, 86572, 1472313],
            ["南沙区", 24696, 2788810, 5209, 785011, 8242, 1004492, 100150, 2592657],
            ["从化区", 13227, 1610143, 3124, 352719, 971, 109485, 35352, 768223],
            ["增城区", 47805, 4858689, 8948, 1399254, 6800, 733749, 222358, 3119280]
        ]
    }
}

# 初始化数据结构
districts = [row[0] for row in base_data["可售信息"]["data"]]
date_range = pd.date_range(start="2024-01-01", end="2024-12-31")
data_types = ["可售信息", "未售信息", "签约信息"]
columns = ["data_date", "region", "data_type"] + base_data["可售信息"]["columns"][1:]

# 准备结果容器
result = []

# 初始化每日数据存储
daily_data = {
    date: {
        district: {
            "可售信息": None,
            "未售信息": None,
            "签约信息": None
        } for district in districts
    } for date in date_range
}

# 生成初始数据 (1月1日)
for district_idx, district in enumerate(districts):
    # 可售信息 (60%)
    sale_values = [int(val * 0.6) for val in base_data["可售信息"]["data"][district_idx][1:]]
    daily_data[date_range[0]][district]["可售信息"] = sale_values
    
    # 未售信息 (60%)
    unsold_values = [int(val * 0.6) for val in base_data["未售信息"]["data"][district_idx][1:]]
    daily_data[date_range[0]][district]["未售信息"] = unsold_values
    
    # 签约信息 (初始为0)
    daily_data[date_range[0]][district]["签约信息"] = [0] * 8

# 生成后续每日数据
for day_idx in range(1, len(date_range)):
    current_date = date_range[day_idx]
    prev_date = date_range[day_idx - 1]
    
    # 计算年度进度
    progress = day_idx / len(date_range)
    
    for district in districts:
        # 可售信息 - 只增不减
        prev_sale = daily_data[prev_date][district]["可售信息"]
        increment = np.random.uniform(0, 0.004)  # 每天最多增长0.4%
        current_sale = [int(val * (1 + increment)) for val in prev_sale]
        daily_data[current_date][district]["可售信息"] = current_sale
        
        # 未售信息 - 波动±5%
        prev_unsold = daily_data[prev_date][district]["未售信息"]
        change = np.random.uniform(-0.05, 0.05)
        if progress < 0.5 and np.random.rand() > 0.7:  # 上半年更可能增长
            change = abs(change)
        current_unsold = [int(val * (1 + change)) for val in prev_unsold]
        daily_data[current_date][district]["未售信息"] = current_unsold
        


# 生成签约信息（可售/未售完成后统一处理）
for month in range(1, 13):
    month_days = [d for d in date_range if d.month == month]
    for district in districts:
        # 签约类型索引 [(套数_idx, 面积_idx)]
        sign_types = [(0, 1), (2, 3), (4, 5), (6, 7)]  # 住宅、商业、办公、车位

        for unit_idx, area_idx in sign_types:
            total_units = sum(daily_data[day][district]["可售信息"][unit_idx] for day in month_days)
            avg_units = total_units / len(month_days)
            month_signed_total = int(avg_units * 0.05)
            unit_distribution = np.random.multinomial(month_signed_total, [1/len(month_days)] * len(month_days))

            for i, day in enumerate(month_days):
                signed_units = unit_distribution[i]
                sale_units = daily_data[day][district]["可售信息"][unit_idx]
                sale_area = daily_data[day][district]["可售信息"][area_idx]
                avg_area = sale_area / sale_units if sale_units > 0 else 0
                signed_area = int(signed_units * avg_area)
                if daily_data[day][district]["签约信息"] is None:
                    daily_data[day][district]["签约信息"] = [0] * 8
                daily_data[day][district]["签约信息"][unit_idx] = signed_units
                daily_data[day][district]["签约信息"][area_idx] = signed_area
# 整理结果
for date in date_range:
    for district in districts:
        for data_type in data_types:
            
            row = [
                date.strftime("%Y-%m-%d"),
                {
                    "越秀区": "Yuexiu", "荔湾区": "Liwan", "海珠区": "Haizhu", "天河区": "Tianhe", "白云区": "Baiyun",
                    "黄埔区": "Huangpu", "番禺区": "Panyu", "花都区": "Huadu", "南沙区": "Nansha", "从化区": "Conghua", "增城区": "Zengcheng"
                }.get(district, district),
                {
                    "可售信息": "For Sale",
                    "未售信息": "Unsold",
                    "签约信息": "Signed"
                }.get(data_type, data_type)
            ]

            row.extend(daily_data[date][district][data_type])
            result.append(row)

# 创建DataFrame并保存
df = pd.DataFrame(result, columns=columns)
df.to_csv("guangzhou_housing_2024_complete.csv", index=False, encoding="utf-8-sig")
print("✅ Daily data saved as guangzhou_housing_2024_complete.csv")


# ========== 生成月度统计数据 ==========
print("📊 Generating monthly summary...")

df = pd.read_csv("guangzhou_housing_2024_complete.csv", encoding="utf-8-sig")

district_map = {
    "越秀区": "Yuexiu",
    "荔湾区": "Liwan",
    "海珠区": "Haizhu",
    "天河区": "Tianhe",
    "白云区": "Baiyun",
    "黄埔区": "Huangpu",
    "番禺区": "Panyu",
    "花都区": "Huadu",
    "南沙区": "Nansha",
    "从化区": "Conghua",
    "增城区": "Zengcheng"
}

data_type_map = {
    "可售信息": "For Sale",
    "未售信息": "Unsold",
    "签约信息": "Signed"
}

df["data_date"] = pd.to_datetime(df["data_date"]).dt.to_period("M").astype(str)
df["month"] = df["data_date"]

unit_area_cols = [
    "residential_units", "residential_area",
    "commercial_units", "commercial_area",
    "office_units", "office_area",
    "parking_units", "parking_area"
]

monthly_summary = []

for (month, district, data_type), group in df.groupby(["month", "region", "data_type"]):
    summary = {
        "data": month,
        "region": district,
        "data_type": data_type
    }

    
    if data_type in ["For Sale", "Unsold"]:
        year, month = map(int, month.split("-"))
        days_in_month = calendar.monthrange(year, month)[1]
        for col in unit_area_cols:
            summary[col] = round(group[col].sum() / days_in_month, 2)
            if "units" in col:
                summary[col] = int(round(summary[col]))
    else:
        for col in unit_area_cols:
            summary[col] = int(group[col].sum())

    monthly_summary.append(summary)

monthly_df = pd.DataFrame(monthly_summary)
monthly_df.to_csv("guangzhou_housing_2024_monthly_summary.csv", index=False, encoding="utf-8-sig")
print("✅ Monthly summary saved as guangzhou_housing_2024_monthly_summary.csv")


# ========== 修正所有“套数”字段为整数 ==========
print("🔧 Fixing unit fields to integers...")
unit_columns = ["residential_units", "commercial_units", "office_units", "parking_units"]
df = pd.read_csv("guangzhou_housing_2024_complete.csv", encoding="utf-8-sig")
for col in unit_columns:
    df[col] = df[col].round().astype(int)
df.to_csv("guangzhou_housing_2024_complete.csv", index=False, encoding="utf-8-sig")
print("✅ Daily unit columns converted to integer")


# ========== 强制将可售/未售信息套数列转为整数 ==========
df = pd.read_csv("guangzhou_housing_2024_complete.csv", encoding="utf-8-sig")
for col in ["residential_units", "commercial_units", "office_units", "parking_units"]:
    df.loc[df["data_type"].isin(["可售信息", "未售信息"]), col] = (
        df.loc[df["data_type"].isin(["可售信息", "未售信息"]), col]
        .round().astype(int)
    )
df.to_csv("guangzhou_housing_2024_complete.csv", index=False, encoding="utf-8-sig")
print("✅ For Sale and Unsold unit columns rounded to integer")
