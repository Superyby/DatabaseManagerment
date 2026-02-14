#!/bin/bash
# 快速启动脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_help() {
    echo "DatabaseManager 启动脚本"
    echo ""
    echo "用法: ./start.sh [选项]"
    echo ""
    echo "选项:"
    echo "  single    单容器模式（推荐开发使用，内存占用低）"
    echo "  multi     多容器模式（生产推荐）"
    echo "  local     本地直接运行（不使用 Docker）"
    echo "  build     仅构建镜像"
    echo "  stop      停止所有服务"
    echo "  logs      查看日志"
    echo "  status    查看服务状态"
    echo "  help      显示帮助"
    echo ""
    echo "示例:"
    echo "  ./start.sh single     # 单容器快速启动"
    echo "  ./start.sh local      # 本地编译运行"
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}错误: Docker 未安装${NC}"
        exit 1
    fi
}

start_single() {
    check_docker
    echo -e "${GREEN}启动单容器模式...${NC}"
    docker compose -f docker-compose.single.yml up -d --build
    echo ""
    echo -e "${GREEN}服务已启动！${NC}"
    echo "Gateway:    http://localhost:8080"
    echo "健康检查:   http://localhost:8080/api/health"
}

start_multi() {
    check_docker
    echo -e "${GREEN}启动多容器模式...${NC}"
    docker compose up -d --build
    echo ""
    echo -e "${GREEN}服务已启动！${NC}"
    echo "Gateway:    http://localhost:8080"
    echo "健康检查:   http://localhost:8080/api/health/all"
}

start_local() {
    echo -e "${GREEN}本地启动服务...${NC}"

    if ! command -v cargo &> /dev/null; then
        echo -e "${RED}错误: Rust/Cargo 未安装${NC}"
        echo "请先安装: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
        exit 1
    fi

    # 编译
    echo "编译中..."
    cargo build --release --workspace

    # 启动服务（后台运行）
    echo "启动 connection-service..."
    SERVER_PORT=8081 ./target/release/connection-service &
    sleep 1

    echo "启动 query-service..."
    SERVER_PORT=8082 CONNECTION_SERVICE_URL=http://127.0.0.1:8081 ./target/release/query-service &
    sleep 1

    echo "启动 ai-service..."
    SERVER_PORT=8083 CONNECTION_SERVICE_URL=http://127.0.0.1:8081 QUERY_SERVICE_URL=http://127.0.0.1:8082 ./target/release/ai-service &
    sleep 1

    echo "启动 gateway..."
    SERVER_PORT=8080 CONNECTION_SERVICE_URL=http://127.0.0.1:8081 QUERY_SERVICE_URL=http://127.0.0.1:8082 AI_SERVICE_URL=http://127.0.0.1:8083 ./target/release/gateway &

    echo ""
    echo -e "${GREEN}服务已启动！${NC}"
    echo "Gateway:    http://localhost:8080"
    echo ""
    echo -e "${YELLOW}停止服务: pkill -f 'target/release'${NC}"
}

build_only() {
    check_docker
    echo -e "${GREEN}构建镜像...${NC}"
    docker compose build
    echo -e "${GREEN}构建完成！${NC}"
}

stop_all() {
    check_docker
    echo -e "${YELLOW}停止所有服务...${NC}"
    docker compose down 2>/dev/null || true
    docker compose -f docker-compose.single.yml down 2>/dev/null || true
    echo -e "${GREEN}已停止${NC}"
}

show_logs() {
    check_docker
    if docker compose ps --quiet 2>/dev/null | grep -q .; then
        docker compose logs -f
    elif docker compose -f docker-compose.single.yml ps --quiet 2>/dev/null | grep -q .; then
        docker compose -f docker-compose.single.yml logs -f
    else
        echo -e "${YELLOW}没有运行中的容器${NC}"
    fi
}

show_status() {
    check_docker
    echo "=== 多容器模式 ==="
    docker compose ps 2>/dev/null || echo "未运行"
    echo ""
    echo "=== 单容器模式 ==="
    docker compose -f docker-compose.single.yml ps 2>/dev/null || echo "未运行"
}

# 主逻辑
case "${1:-help}" in
    single)
        start_single
        ;;
    multi)
        start_multi
        ;;
    local)
        start_local
        ;;
    build)
        build_only
        ;;
    stop)
        stop_all
        ;;
    logs)
        show_logs
        ;;
    status)
        show_status
        ;;
    help|*)
        print_help
        ;;
esac
