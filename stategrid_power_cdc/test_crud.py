#!/usr/bin/env python3
"""
CRUD 测试和性能验证
"""

import psycopg2
import time
from datetime import datetime
import statistics

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'stategrid_db',
    'user': 'postgres',
    'password': 'postgres'
}

class CRUDTest:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.results = []

    def test_create(self):
        """测试插入（Create）"""
        print("\n[CREATE] 测试插入操作...")

        cur = self.conn.cursor()

        # 插入测试用户
        test_user = (99999, '测试用户', '居民', 1, '北京市', '测试地址', '13800000000',
                     datetime.now(), datetime.now())

        start_time = time.time()
        cur.execute("""
            INSERT INTO power_user
            (user_id, user_name, usage_type, region_id, region_name, address, phone, create_time, update_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, test_user)
        self.conn.commit()
        end_time = time.time()

        latency = (end_time - start_time) * 1000  # 转换为毫秒
        self.results.append(('CREATE', latency))
        print(f"  插入用户完成，延迟：{latency:.2f} ms")

        # 插入测试消费记录
        cur.execute("SELECT MAX(consumption_id) FROM power_consumption")
        max_id = cur.fetchone()[0] or 0
        test_consumption = (max_id + 1, 99999, 100.5, 70.35, datetime.now(),
                           0, 100.5, 'CRUD测试')

        start_time = time.time()
        cur.execute("""
            INSERT INTO power_consumption
            (consumption_id, user_id, consumption_amount, consumption_cost, consumption_date,
             meter_reading_before, meter_reading_after, remark)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, test_consumption)
        self.conn.commit()
        end_time = time.time()

        latency = (end_time - start_time) * 1000
        self.results.append(('CREATE', latency))
        print(f"  插入消费记录完成，延迟：{latency:.2f} ms")

        cur.close()

    def test_read(self):
        """测试查询（Read）"""
        print("\n[READ] 测试查询操作...")

        cur = self.conn.cursor()

        # 测试单条查询
        start_time = time.time()
        cur.execute("SELECT * FROM power_user WHERE user_id = 99999")
        result = cur.fetchone()
        end_time = time.time()

        latency = (end_time - start_time) * 1000
        self.results.append(('READ', latency))
        print(f"  单条查询完成，延迟：{latency:.2f} ms")

        # 测试聚合查询
        start_time = time.time()
        cur.execute("""
            SELECT region_id, COUNT(*) as user_count
            FROM power_user
            GROUP BY region_id
        """)
        results = cur.fetchall()
        end_time = time.time()

        latency = (end_time - start_time) * 1000
        self.results.append(('READ', latency))
        print(f"  聚合查询完成，返回 {len(results)} 条，延迟：{latency:.2f} ms")

        # 测试 ADS 层数据查询
        start_time = time.time()
        cur.execute("""
            SELECT * FROM ads_power_dashboard
            ORDER BY stat_date DESC, region_id
            LIMIT 10
        """)
        results = cur.fetchall()
        end_time = time.time()

        latency = (end_time - start_time) * 1000
        self.results.append(('READ', latency))
        print(f"  ADS 数据查询完成，返回 {len(results)} 条，延迟：{latency:.2f} ms")

        cur.close()

    def test_update(self):
        """测试更新（Update）"""
        print("\n[UPDATE] 测试更新操作...")

        cur = self.conn.cursor()

        # 更新用户信息
        start_time = time.time()
        cur.execute("""
            UPDATE power_user
            SET user_name = '测试用户_已更新', update_time = %s
            WHERE user_id = 99999
        """, (datetime.now(),))
        self.conn.commit()
        end_time = time.time()

        latency = (end_time - start_time) * 1000
        self.results.append(('UPDATE', latency))
        print(f"  更新用户完成，延迟：{latency:.2f} ms")

        cur.close()

    def test_delete(self):
        """测试删除（Delete）"""
        print("\n[DELETE] 测试删除操作...")

        cur = self.conn.cursor()

        # 删除用户（级联删除消费记录）
        start_time = time.time()
        cur.execute("DELETE FROM power_user WHERE user_id = 99999")
        self.conn.commit()
        end_time = time.time()

        latency = (end_time - start_time) * 1000
        self.results.append(('DELETE', latency))
        print(f"  删除用户完成，延迟：{latency:.2f} ms")

        cur.close()

    def test_e2e_latency(self):
        """测试端到端延迟"""
        print("\n[E2E Latency] 测试端到端延迟...")

        cur = self.conn.cursor()

        # 插入测试数据
        cur.execute("SELECT MAX(consumption_id) FROM power_consumption")
        max_id = cur.fetchone()[0] or 0
        test_consumption = (max_id + 1, 1, 50.0, 35.0, datetime.now(),
                           1000.0, 1050.0, 'E2E测试')

        insert_time = datetime.now()
        cur.execute("""
            INSERT INTO power_consumption
            (consumption_id, user_id, consumption_amount, consumption_cost, consumption_date,
             meter_reading_before, meter_reading_after, remark)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, test_consumption)
        self.conn.commit()

        # 等待数据流过 CDC 和 Fluss 分层
        print("  等待数据流处理...")
        time.sleep(5)

        # 检查 ADS 层数据
        cur.execute("""
            SELECT update_time FROM ads_power_dashboard
            WHERE stat_date = CURRENT_DATE
            ORDER BY update_time DESC
            LIMIT 1
        """)
        result = cur.fetchone()

        if result and result[0]:
            sink_time = result[0]
            e2e_latency = (sink_time - insert_time).total_seconds()
            self.results.append(('E2E', e2e_latency * 1000))  # 转换为毫秒
            print(f"  端到端延迟：{e2e_latency:.2f} 秒")
        else:
            print(f"  端到端延迟：数据尚未到达 ADS 层")

        cur.close()

    def generate_report(self):
        """生成测试报告"""
        print("\n==========================================")
        print("CRUD 测试报告")
        print("==========================================")

        # 按操作类型分组
        operations = {}
        for op, latency in self.results:
            if op not in operations:
                operations[op] = []
            operations[op].append(latency)

        for op, latencies in operations.items():
            avg = statistics.mean(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            count = len(latencies)

            print(f"\n[{op}]")
            print(f"  操作次数: {count}")
            print(f"  平均延迟: {avg:.2f} ms")
            print(f"  最小延迟: {min_latency:.2f} ms")
            print(f"  最大延迟: {max_latency:.2f} ms")

        # 总体统计
        all_latencies = [latency for _, latency in self.results]
        print(f"\n[总体]")
        print(f"  总操作数: {len(all_latencies)}")
        print(f"  平均延迟: {statistics.mean(all_latencies):.2f} ms")
        print(f"  最小延迟: {min(all_latencies):.2f} ms")
        print(f"  最大延迟: {max(all_latencies):.2f} ms")

        # 性能评级
        avg_latency = statistics.mean(all_latencies)
        if avg_latency < 10:
            rating = "优秀 ⭐⭐⭐⭐⭐"
        elif avg_latency < 50:
            rating = "良好 ⭐⭐⭐⭐"
        elif avg_latency < 100:
            rating = "一般 ⭐⭐⭐"
        else:
            rating = "需优化 ⭐⭐"

        print(f"\n性能评级: {rating}")
        print("==========================================")

    def run_all_tests(self):
        """运行所有测试"""
        print("==========================================")
        print("开始 CRUD 测试")
        print("==========================================")

        try:
            self.test_create()
            time.sleep(2)
            self.test_read()
            self.test_update()
            time.sleep(2)
            self.test_read()
            self.test_delete()
            self.test_e2e_latency()

            self.generate_report()

        except Exception as e:
            print(f"\n错误：{e}")
            self.conn.rollback()
        finally:
            self.conn.close()

if __name__ == '__main__':
    test = CRUDTest()
    test.run_all_tests()
