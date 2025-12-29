#!/bin/bash

# 停止 Prometheus 监控服务脚本

set -e

echo "🛑 停止 Prometheus 监控服务..."
echo "=============================="

# 检查 Docker Compose 是否存在
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose 未安装"
    exit 1
fi

# 停止并移除容器
echo "🔄 停止容器..."
docker-compose down

echo "🧹 清理未使用的资源..."
docker system prune -f

echo "✅ 服务已停止"
echo ""
echo "💡 如需完全清理（包括数据卷），请运行："
echo "   docker-compose down -v"
echo "   docker volume prune -f"
echo ""