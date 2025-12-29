# 游戏服务器 Prometheus 监控演示

这是一个完整的游戏服务器 Prometheus 监控演示环境，包含了游戏服务器、Prometheus、Grafana、AlertManager 等组件的完整配置。

## 🎯 功能特性

- **完整的游戏服务器**: 模拟真实的游戏服务器 API
- **Prometheus 监控**: 完整的指标采集和存储
- **Grafana 仪表板**: 美观的可视化监控面板
- **告警系统**: AlertManager 告警规则和通知
- **系统监控**: Node Exporter 系统指标
- **自动化部署**: Docker Compose 一键部署

## 📋 系统要求

- Docker 20.0+
- Docker Compose 2.0+
- Go 1.19+ (运行游戏服务器)
- 可用端口: 18080, 19090, 19091, 13000, 19093, 19100

## 🚀 快速开始

### 1. 启动监控服务

```bash
# 进入示例目录
cd /Users/zyq/workspace/mc-server/infrastructure/component/prometheus-mgr/example

# 启动监控服务
./start.sh
```

### 2. 启动游戏服务器

```bash
# 在新的终端窗口中运行
go run main.go
```

### 3. 访问监控界面

- **Grafana 仪表板**: http://localhost:13000 (admin/admin123)
- **Prometheus**: http://localhost:19091
- **AlertManager**: http://localhost:19093
- **游戏服务器**: http://localhost:18080
- **指标端点**: http://localhost:19090/metrics

## 🎮 API 测试

### 玩家登录
```bash
curl -X POST http://localhost:18080/api/v1/player/login \
  -d 'player_id=test001&player_name=TestPlayer&level=10'
```

### 查看在线玩家
```bash
curl http://localhost:18080/api/v1/players/online
```

### 获取玩家信息
```bash
curl http://localhost:18080/api/v1/player/test001
```

### 玩家登出
```bash
curl -X POST http://localhost:18080/api/v1/player/logout \
  -d 'player_id=test001'
```

### 健康检查
```bash
curl http://localhost:18080/health
```

## 📊 监控指标

### HTTP 指标
- `http_requests_total`: HTTP 请求总数
- `http_request_duration_seconds`: HTTP 请求耗时分布

### 游戏业务指标
- `players_online_count`: 当前在线玩家数量
- `player_actions_total`: 玩家操作总数
- `game_operation_duration_seconds`: 游戏操作耗时分布

### 系统指标
- `memory_usage_bytes`: 内存使用量
- `node_*`: 系统级指标（CPU、内存、磁盘等）

## 📈 Grafana 仪表板

预配置的仪表板包含以下面板：

1. **在线玩家数量**: 实时显示当前在线玩家数
2. **HTTP 请求速率**: 按端点分组的请求速率
3. **HTTP 成功率**: 请求成功率仪表盘
4. **HTTP 响应时间分位数**: 50th、95th、99th 百分位响应时间
5. **玩家操作速率**: 玩家操作的成功/失败率
6. **内存使用量**: 应用程序内存使用情况

## 🚨 告警规则

### HTTP 相关告警
- **HighErrorRate**: 错误率超过 5%
- **HighResponseTime**: 95th 响应时间超过 1 秒
- **LowRequestRate**: 请求率过低

### 玩家相关告警
- **PlayerCountDrop**: 在线玩家数大幅下降
- **NoPlayersOnline**: 没有玩家在线
- **TooManyPlayersOnline**: 在线玩家数过多

### 系统相关告警
- **HighMemoryUsage**: 内存使用量过高
- **SlowGameOperations**: 游戏操作响应缓慢
- **HighPlayerActionFailureRate**: 玩家操作失败率过高

## 🔧 配置文件说明

### Docker Compose 配置
- `docker-compose.yml`: 定义所有服务容器
- 包含 Prometheus、Grafana、AlertManager、Node Exporter

### Prometheus 配置
- `prometheus.yml`: Prometheus 主配置文件
- `rules/game-server-alerts.yml`: 告警规则定义

### Grafana 配置
- `grafana/provisioning/datasources/`: 数据源配置
- `grafana/provisioning/dashboards/`: 仪表板配置
- `grafana/dashboards/`: 仪表板 JSON 文件

### AlertManager 配置
- `alertmanager.yml`: 告警管理器配置
- 支持邮件、Webhook 等多种通知方式

## 🎯 自定义配置

### 添加新的指标

1. 在 `main.go` 中注册新指标：
```go
err := prometheus_mgr.RegisterGauge("custom_metric", prometheus_mgr.MetricOptions{
    Help:   "自定义指标描述",
    Labels: []string{"label1", "label2"},
})
```

2. 在业务代码中更新指标：
```go
labels := prometheus_mgr.MetricLabels{
    "label1": "value1",
    "label2": "value2",
}
_ = prometheus_mgr.SetGauge("custom_metric", 42.0, labels)
```

### 添加新的告警规则

在 `rules/game-server-alerts.yml` 中添加：
```yaml
- alert: CustomAlert
  expr: custom_metric > 100
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "自定义告警"
    description: "自定义指标值为 {{ $value }}"
```

### 修改 Grafana 仪表板

1. 访问 Grafana (http://localhost:3000)
2. 编辑现有仪表板或创建新仪表板
3. 导出 JSON 配置
4. 保存到 `grafana/dashboards/` 目录

## 🔍 故障排除

### 常见问题

1. **端口被占用**
   ```bash
   # 查看端口占用
   lsof -i :18080
   lsof -i :19090
   lsof -i :19091
   lsof -i :13000
   ```

2. **Docker 服务启动失败**
   ```bash
   # 查看服务日志
   docker-compose logs prometheus
   docker-compose logs grafana
   docker-compose logs alertmanager
   ```

3. **指标数据不显示**
   - 检查游戏服务器是否正常运行
   - 访问 http://localhost:19090/metrics 确认指标可用
   - 检查 Prometheus targets 状态: http://localhost:19091/targets

4. **Grafana 无法连接 Prometheus**
   - 检查数据源配置
   - 确认 Prometheus 服务正常运行
   - 检查网络连接

### 日志查看

```bash
# 查看所有服务日志
docker-compose logs -f

# 查看特定服务日志
docker-compose logs -f prometheus
docker-compose logs -f grafana
docker-compose logs -f alertmanager
```

### 重启服务

```bash
# 重启所有服务
docker-compose restart

# 重启特定服务
docker-compose restart prometheus
docker-compose restart grafana
```

## 🛑 停止服务

```bash
# 停止所有服务
./stop.sh

# 或者手动停止
docker-compose down

# 完全清理（包括数据卷）
docker-compose down -v
docker volume prune -f
```

## 📚 扩展阅读

- [Prometheus 官方文档](https://prometheus.io/docs/)
- [Grafana 官方文档](https://grafana.com/docs/)
- [AlertManager 官方文档](https://prometheus.io/docs/alerting/latest/alertmanager/)
- [PromQL 查询语言](https://prometheus.io/docs/prometheus/latest/querying/)

## 🤝 贡献

欢迎提交 Issue 和 Pull Request 来改进这个演示环境！

## 📄 许可证

本项目遵循项目的许可证协议。