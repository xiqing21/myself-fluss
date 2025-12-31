#!/usr/bin/env python3
"""
性能监控脚本
监控数据从 PostgreSQL Source 到 PostgreSQL Sink 的端到端延迟和吞吐量
"""

import psycopg2
import requests
import time
import json
from datetime import datetime, timedelta
import pandas as pd

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'stategrid_db',
    'user': 'postgres',
    'password': 'postgres'
}

# Flink Web UI
FLINK_UI_URL = 'http://localhost:8081'

def get_source_record_count():
    """获取源表记录数"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM power_consumption")
    source_count = cur.fetchone()[0]

    cur.close()
    conn.close()
    return source_count

def get_sink_record_count():
    """获取 Sink 表记录数"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM ads_power_dashboard")
    sink_count = cur.fetchone()[0]

    cur.close()
    conn.close()
    return sink_count

def get_flink_job_status():
    """获取 Flink 作业状态"""
    try:
        response = requests.get(f'{FLINK_UI_URL}/jobs')
        if response.status_code == 200:
            jobs = response.json().get('jobs', [])
            return jobs
    except Exception as e:
        print(f"获取 Flink 作业状态失败：{e}")
    return []

def calculate_latency():
    """计算端到端延迟"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        # 获取最新数据的更新时间
        cur.execute("""
            SELECT
                MAX(pc.consumption_date) AS source_time,
                MAX(apd.update_time) AS sink_time
            FROM power_consumption pc
            LEFT JOIN ads_power_dashboard apd
                ON DATE(pc.consumption_date) = apd.stat_date
        """)

        result = cur.fetchone()
        if result[0] and result[1]:
            source_time = result[0]
            sink_time = result[1]
            latency = (sink_time - source_time).total_seconds()
            return latency
    except Exception as e:
        print(f"计算延迟失败：{e}")
    finally:
        cur.close()
        conn.close()

    return None

def monitor(interval=5, duration=600):
    """监控性能指标"""
    print("==========================================")
    print("国网电力 CDC 性能监控")
    print("==========================================")
    print(f"监控间隔：{interval} 秒")
    print(f"监控时长：{duration} 秒")
    print("==========================================\n")

    start_time = time.time()
    iteration = 0

    metrics = []

    while time.time() - start_time < duration:
        iteration += 1
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 获取记录数
        source_count = get_source_record_count()
        sink_count = get_sink_record_count()

        # 计算 TPS（基于上一次记录）
        if iteration == 1:
            tps = 0
            last_source_count = source_count
        else:
            time_diff = interval
            count_diff = source_count - last_source_count
            tps = count_diff / time_diff if time_diff > 0 else 0
            last_source_count = source_count

        # 计算延迟
        latency = calculate_latency()

        # 获取 Flink 作业状态
        jobs = get_flink_job_status()
        running_jobs = [j for j in jobs if j['status'] == 'RUNNING']
        failed_jobs = [j for j in jobs if j['status'] == 'FAILED']

        # 打印监控数据
        print(f"[{iteration}] {timestamp}")
        print(f"  源表记录数: {source_count}")
        print(f"  Sink 表记录数: {sink_count}")
        print(f"  当前 TPS: {tps:.2f}")
        if latency:
            print(f"  端到端延迟: {latency:.2f} 秒")
        print(f"  Flink 作业: 运行 {len(running_jobs)}/{len(jobs)}")
        if failed_jobs:
            print(f"  ⚠️  失败作业数: {len(failed_jobs)}")
        print("-" * 50)

        # 保存指标
        metrics.append({
            'timestamp': timestamp,
            'source_count': source_count,
            'sink_count': sink_count,
            'tps': tps,
            'latency': latency if latency else None,
            'running_jobs': len(running_jobs),
            'total_jobs': len(jobs)
        })

        time.sleep(interval)

    # 生成汇总报告
    print("\n==========================================")
    print("监控汇总报告")
    print("==========================================")

    df = pd.DataFrame(metrics)
    print(f"\n统计信息：")
    print(f"  监控时长：{duration} 秒")
    print(f"  源表最终记录数：{source_count}")
    print(f"  Sink 表最终记录数：{sink_count}")
    print(f"  平均 TPS：{df['tps'].mean():.2f}")
    print(f"  最大 TPS：{df['tps'].max():.2f}")
    if df['latency'].notna().any():
        print(f"  平均延迟：{df['latency'].mean():.2f} 秒")
        print(f"  最小延迟：{df['latency'].min():.2f} 秒")
        print(f"  最大延迟：{df['latency'].max():.2f} 秒")

    print("\n==========================================")

if __name__ == '__main__':
    # 监控 5 分钟，每 5 秒采样一次
    monitor(interval=5, duration=300)
