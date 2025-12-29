#!/bin/bash

# API 测试脚本

set -e

BASE_URL="http://localhost:18080"
API_URL="$BASE_URL/api/v1"

echo "🧪 游戏服务器 API 测试"
echo "====================="

# 检查服务器是否运行
echo "🔍 检查服务器状态..."
if ! curl -s "$BASE_URL/health" > /dev/null; then
    echo "❌ 游戏服务器未运行，请先启动服务器："
    echo "   go run main.go"
    exit 1
fi

echo "✅ 服务器运行正常"
echo ""

# 健康检查
echo "🏥 健康检查:"
curl -s "$BASE_URL/health" | jq .
echo ""

# 测试玩家登录
echo "👤 测试玩家登录..."
for i in {1..5}; do
    player_id="test$(printf "%03d" $i)"
    player_name="TestPlayer$i"
    level=$((RANDOM % 50 + 1))
    
    echo "  登录玩家: $player_name (ID: $player_id, Level: $level)"
    curl -s -X POST "$API_URL/player/login" \
        -d "player_id=$player_id&player_name=$player_name&level=$level" | jq .
    
    sleep 0.5
done

echo ""

# 查看在线玩家
echo "👥 查看在线玩家:"
curl -s "$API_URL/players/online" | jq .
echo ""

# 测试获取玩家信息
echo "📋 测试获取玩家信息:"
for player_id in "test001" "test002" "test999"; do
    echo "  查询玩家: $player_id"
    response=$(curl -s "$API_URL/player/$player_id")
    if echo "$response" | jq -e '.error' > /dev/null 2>&1; then
        echo "    ❌ $(echo "$response" | jq -r '.error')"
    else
        echo "    ✅ $(echo "$response" | jq -r '.name') (Level: $(echo "$response" | jq -r '.level'))"
    fi
    sleep 0.3
done

echo ""

# 模拟一些随机活动
echo "🎮 模拟游戏活动..."
for i in {1..10}; do
    # 随机登录新玩家
    if [ $((RANDOM % 3)) -eq 0 ]; then
        player_id="random$(printf "%04d" $((RANDOM % 9999)))"
        player_name="RandomPlayer$((RANDOM % 1000))"
        level=$((RANDOM % 100 + 1))
        
        echo "  🔑 随机玩家登录: $player_name"
        curl -s -X POST "$API_URL/player/login" \
            -d "player_id=$player_id&player_name=$player_name&level=$level" > /dev/null
    fi
    
    # 随机查询玩家
    if [ $((RANDOM % 2)) -eq 0 ]; then
        player_id="test$(printf "%03d" $((RANDOM % 5 + 1)))"
        echo "  🔍 查询玩家: $player_id"
        curl -s "$API_URL/player/$player_id" > /dev/null
    fi
    
    # 随机查看在线玩家列表
    if [ $((RANDOM % 4)) -eq 0 ]; then
        echo "  👥 查看在线玩家列表"
        curl -s "$API_URL/players/online" > /dev/null
    fi
    
    sleep 0.2
done

echo ""

# 测试玩家登出
echo "👋 测试玩家登出..."
for i in {1..3}; do
    player_id="test$(printf "%03d" $i)"
    echo "  登出玩家: $player_id"
    curl -s -X POST "$API_URL/player/logout" \
        -d "player_id=$player_id" | jq .
    sleep 0.5
done

echo ""

# 最终状态检查
echo "📊 最终在线玩家状态:"
curl -s "$API_URL/players/online" | jq .
echo ""

# 检查指标
echo "📈 检查 Prometheus 指标:"
echo "  访问: http://localhost:19090/metrics"
echo "  在线玩家数: $(curl -s http://localhost:19090/metrics | grep 'players_online_count' | tail -1)"
echo "  HTTP请求总数: $(curl -s http://localhost:19090/metrics | grep 'http_requests_total' | wc -l) 个时间序列"
echo ""

echo "✅ API 测试完成！"
echo ""
echo "🔗 相关链接:"
echo "  - 游戏服务器:     http://localhost:18080"
echo "  - Prometheus:     http://localhost:19091"
echo "  - Grafana:        http://localhost:13000 (admin/admin123)"
echo "  - AlertManager:   http://localhost:19093"
echo "  - 指标端点:       http://localhost:19090/metrics"
echo ""