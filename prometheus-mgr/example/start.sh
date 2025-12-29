#!/bin/bash

# 游戏服务器 Prometheus 监控演示启动脚本

set -e

echo "🎮 游戏服务器 Prometheus 监控演示"
echo "===================================="

# 检查 Docker 和 Docker Compose
if ! command -v docker &> /dev/null; then
    echo "❌ Docker 未安装，请先安装 Docker"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose 未安装，请先安装 Docker Compose"
    exit 1
fi

# 检查端口是否被占用
check_port() {
    local port=$1
    local service=$2
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo "⚠️  端口 $port 已被占用 ($service)，请先关闭占用该端口的程序"
        return 1
    fi
    return 0
}

echo "🔍 检查端口占用情况..."
ports_ok=true

if ! check_port 18080 "游戏服务器"; then
    ports_ok=false
fi

if ! check_port 19090 "Prometheus 指标端点"; then
    ports_ok=false
fi

if ! check_port 19091 "Prometheus 服务器"; then
    ports_ok=false
fi

if ! check_port 13000 "Grafana"; then
    ports_ok=false
fi

if ! check_port 19093 "AlertManager"; then
    ports_ok=false
fi

if ! check_port 19100 "Node Exporter"; then
    ports_ok=false
fi

if [ "$ports_ok" = false ]; then
    echo "❌ 存在端口冲突，请解决后重试"
    exit 1
fi

echo "✅ 端口检查通过"

# 创建必要的目录
echo "📁 创建必要的目录..."
mkdir -p rules
mkdir -p grafana/provisioning/datasources
mkdir -p grafana/provisioning/dashboards
mkdir -p grafana/dashboards

echo "✅ 目录创建完成"

# 启动 Docker Compose 服务
echo "🚀 启动监控服务..."
docker-compose up -d

echo "⏳ 等待服务启动..."
sleep 10

# 检查服务状态
echo "🔍 检查服务状态..."
docker-compose ps

echo ""
echo "🎉 监控服务启动完成！"
echo ""
echo "📊 访问地址："
echo "  - Prometheus:    http://localhost:19091"
echo "  - Grafana:       http://localhost:13000 (admin/admin123)"
echo "  - AlertManager:  http://localhost:19093"
echo "  - Node Exporter: http://localhost:19100"
echo ""
echo "🎮 现在可以启动游戏服务器："
echo "  cd $(pwd)"
echo "  go run main.go"
echo ""
echo "📈 游戏服务器启动后访问："
echo "  - 游戏服务器:     http://localhost:18080"
echo "  - 指标端点:       http://localhost:19090/metrics"
echo "  - 健康检查:       http://localhost:18080/health"
echo ""
echo "🔧 API 测试命令："
echo "  # 玩家登录"
echo "  curl -X POST http://localhost:18080/api/v1/player/login -d 'player_id=test001&player_name=TestPlayer&level=10'"
echo ""
echo "  # 查看在线玩家"
echo "  curl http://localhost:18080/api/v1/players/online"
echo ""
echo "  # 玩家登出"
echo "  curl -X POST http://localhost:18080/api/v1/player/logout -d 'player_id=test001'"
echo ""
echo "📋 停止服务："
echo "  ./stop.sh"
echo ""