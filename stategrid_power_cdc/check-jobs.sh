#!/bin/bash
# 验证 Flink 作业是否成功运行
# 通过 REST API 检查作业状态，如果作业失败则删除

FLINK_REST_API="http://localhost:8081"
MAX_RETRIES=30
RETRY_INTERVAL=3

# 颜色输出
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查 Flink REST API 是否可用
check_flink_api() {
    for i in $(seq 1 $MAX_RETRIES); do
        if curl -s "$FLINK_REST_API/overview" > /dev/null 2>&1; then
            return 0
        fi
        echo -n "."
        sleep $RETRY_INTERVAL
    done
    return 1
}

# 获取所有作业及其状态（不依赖 jq）
get_jobs_status() {
    local json=$(curl -s "$FLINK_REST_API/jobs")
    # 使用 grep/sed 解析 JSON
    echo "$json" | grep -o '"id":"[^"]*","name":"[^"]*","state":"[^"]*"' | \
        sed 's/"id":"//;s/","name":"/|/;s/","state":"/|/;s/"$//'
}

# 获取作业异常信息
get_job_exceptions() {
    local job_id=$1
    curl -s "$FLINK_REST_API/jobs/$job_id/exceptions"
}

# 删除作业
delete_job() {
    local job_id=$1
    echo -e "${YELLOW}正在删除失败作业: $job_id${NC}"
    curl -s -X PATCH "$FLINK_REST_API/jobs/$job_id" -H 'Content-Type: application/json' -d '{"cancellationReason":"Automatically cancelled due to failure"}' > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓${NC} 作业已删除"
    else
        echo -e "${RED}✗${NC} 作业删除失败"
    fi
}

# 主函数
main() {
    JOB_NAME_PATTERN=${1:-".*"}  # 默认匹配所有作业

    echo "=========================================="
    echo "检查 Flink 作业状态"
    echo "=========================================="
    echo "Flink Web UI: $FLINK_REST_API"
    echo ""

    # 检查 Flink API
    echo "检查 Flink REST API..."
    if check_flink_api; then
        echo -e "${GREEN}✓${NC} Flink REST API 可用"
    else
        echo -e "${RED}✗${NC} 无法连接到 Flink REST API"
        exit 1
    fi
    echo ""

    # 获取作业列表
    echo "获取作业列表..."
    JOBS=$(get_jobs_status)
    JOB_COUNT=$(echo "$JOBS" | wc -l)

    if [ "$JOB_COUNT" -eq 0 ]; then
        echo -e "${YELLOW}!${NC} 没有找到运行的作业"
        exit 1
    fi

    echo "找到 $JOB_COUNT 个作业"
    echo ""

    # 检查每个作业
    FAILED_JOBS=0
    RUNNING_JOBS=0
    OTHER_JOBS=0
    MATCHED_JOBS=0

    echo "=========================================="
    echo "作业状态详情"
    echo "=========================================="

    while IFS='|' read -r job_id job_name job_status; do
        if [[ "$job_name" =~ $JOB_NAME_PATTERN ]]; then
            ((MATCHED_JOBS++))
            echo ""
            echo "作业: $job_name"
            echo "ID: $job_id"
            echo "状态: $job_status"

            case "$job_status" in
                "RUNNING")
                    echo -e "${GREEN}✓${NC} 作业运行中"
                    ((RUNNING_JOBS++))
                    ;;
                "FINISHED")
                    echo -e "${GREEN}✓${NC} 作业已完成"
                    ((RUNNING_JOBS++))
                    ;;
                "FAILED"|"CANCELED")
                    echo -e "${RED}✗${NC} 作业失败/已取消"
                    ((FAILED_JOBS++))

                    # 获取异常信息
                    EXCEPTIONS=$(get_job_exceptions "$job_id")
                    if [ -n "$EXCEPTIONS" ] && [ "$EXCEPTIONS" != "null" ] && [ "$EXCEPTIONS" != "{}" ]; then
                        echo ""
                        echo "异常信息:"
                        echo "$EXCEPTIONS" | grep -o '"exception":"[^"]*"' | sed 's/"exception":"//;s/"$//' | sed 's/^/  /'
                    fi

                    # 删除失败作业
                    delete_job "$job_id"
                    ;;
                *)
                    echo -e "${YELLOW}!${NC} 作业状态: $job_status"
                    ((OTHER_JOBS++))
                    ;;
            esac
        fi
    done <<< "$JOBS"

    echo ""
    echo "=========================================="
    echo "统计摘要"
    echo "=========================================="
    echo "匹配作业数: $MATCHED_JOBS"
    echo -e "运行中/成功: ${GREEN}$RUNNING_JOBS${NC}"
    echo -e "失败/取消: ${RED}$FAILED_JOBS${NC}"
    echo -e "其他状态: ${YELLOW}$OTHER_JOBS${NC}"
    echo ""

    if [ $MATCHED_JOBS -eq 0 ]; then
        echo -e "${RED}=========================================="
        echo "未找到匹配的作业！作业名称: $JOB_NAME_PATTERN"
        echo "==========================================${NC}"
        exit 1
    elif [ $FAILED_JOBS -gt 0 ]; then
        echo -e "${RED}=========================================="
        echo "有作业失败！已自动删除失败作业"
        echo "==========================================${NC}"
        exit 1
    elif [ $RUNNING_JOBS -eq 0 ]; then
        echo -e "${RED}=========================================="
        echo "没有作业正在运行！"
        echo "==========================================${NC}"
        exit 1
    else
        echo -e "${GREEN}=========================================="
        echo "作业运行正常"
        echo "==========================================${NC}"
        exit 0
    fi
}

# 使用说明
if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    echo "用法: $0 [作业名称匹配模式]"
    echo ""
    echo "示例:"
    echo "  $0                          # 检查所有作业"
    echo "  $0 'StateGrid DataGen: ODS'      # 检查 ODS 层作业"
    echo "  $0 'StateGrid CDC: DWD'         # 检查 DWD 层作业"
    echo "  $0 'StateGrid CDC: DWS'         # 检查 DWS 层作业"
    echo "  $0 'StateGrid CDC: ADS'         # 检查 ADS 层作业"
    exit 0
fi

main "$@"
