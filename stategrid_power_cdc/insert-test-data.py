#!/usr/bin/env python3
"""
批量插入测试数据到 PostgreSQL
"""

import psycopg2
import random
from datetime import datetime, timedelta
import time

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'stategrid_db',
    'user': 'postgres',
    'password': 'postgres'
}

# 测试数据配置
NUM_USERS = 100
NUM_CONSUMPTIONS = 1000
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 1, 31)

# 地区数据
REGIONS = [
    (1, '北京市'), (2, '天津市'), (3, '河北省'), (4, '山西省'),
    (5, '内蒙古自治区'), (6, '辽宁省'), (7, '吉林省'), (8, '黑龙江省'),
    (9, '上海市'), (10, '江苏省'), (11, '浙江省'), (12, '安徽省'),
    (13, '福建省'), (14, '江西省'), (15, '山东省')
]

# 用电类型
USAGE_TYPES = ['居民', '商业', '工业']

def generate_users():
    """生成用户数据"""
    users = []
    for i in range(1, NUM_USERS + 1):
        region = random.choice(REGIONS)
        usage_type = random.choice(USAGE_TYPES)
        users.append((
            i,
            f'用户_{i}',
            usage_type,
            region[0],
            region[1],
            f'地址_{i}',
            f'138{random.randint(10000000, 99999999)}',
            START_DATE,
            datetime.now()
        ))
    return users

def generate_consumptions():
    """生成消费记录"""
    consumptions = []
    consumption_id = 1

    for _ in range(NUM_CONSUMPTIONS):
        user_id = random.randint(1, NUM_USERS)
        usage_type = '工业' if random.random() < 0.3 else ('商业' if random.random() < 0.6 else '居民')

        # 根据用电类型设置用电量
        if usage_type == '工业':
            amount = random.uniform(500, 5000)
            rate = 1.0  # 工业电价
        elif usage_type == '商业':
            amount = random.uniform(100, 1000)
            rate = 0.9  # 商业电价
        else:
            amount = random.uniform(50, 500)
            rate = 0.7  # 居民电价

        cost = round(amount * rate, 2)
        consumption_date = START_DATE + timedelta(
            days=random.randint(0, (END_DATE - START_DATE).days),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )

        meter_before = random.uniform(0, 100000)
        meter_after = meter_before + amount

        consumptions.append((
            consumption_id,
            user_id,
            round(amount, 2),
            cost,
            consumption_date,
            round(meter_before, 2),
            round(meter_after, 2),
            '测试数据'
        ))
        consumption_id += 1

    return consumptions

def insert_data():
    """插入数据到数据库"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        print("==========================================")
        print("批量插入测试数据")
        print("==========================================")

        # 插入用户数据
        print(f"\n[1/2] 插入 {NUM_USERS} 个用户...")
        users = generate_users()
        insert_user_query = """
            INSERT INTO power_user
            (user_id, user_name, usage_type, region_id, region_name, address, phone, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING
        """
        cur.executemany(insert_user_query, users)
        print(f"用户插入完成：{cur.rowcount} 条")

        # 插入消费记录
        print(f"\n[2/2] 插入 {NUM_CONSUMPTIONS} 条消费记录...")
        consumptions = generate_consumptions()
        insert_consumption_query = """
            INSERT INTO power_consumption
            (consumption_id, user_id, consumption_amount, consumption_cost, consumption_date,
             meter_reading_before, meter_reading_after, remark)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        start_time = time.time()
        cur.executemany(insert_consumption_query, consumptions)
        end_time = time.time()

        print(f"消费记录插入完成：{cur.rowcount} 条")
        print(f"耗时：{end_time - start_time:.2f} 秒")
        print(f"TPS：{NUM_CONSUMPTIONS / (end_time - start_time):.2f}")

        conn.commit()

        print("\n==========================================")
        print("测试数据插入完成！")
        print("==========================================")
        print(f"用户总数：{NUM_USERS}")
        print(f"消费记录数：{NUM_CONSUMPTIONS}")
        print(f"覆盖地区：{len(REGIONS)}")
        print(f"时间范围：{START_DATE.date()} ~ {END_DATE.date()}")
        print("==========================================")

    except Exception as e:
        print(f"错误：{e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    insert_data()
