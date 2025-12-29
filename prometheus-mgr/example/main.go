package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/NumberMan1/component/prometheus-mgr"
	zaplog "github.com/NumberMan1/component/zap-logger"
	"github.com/gin-gonic/gin"
)

// 模拟玩家数据
type Player struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Level    int       `json:"level"`
	LoginAt  time.Time `json:"login_at"`
	IsOnline bool      `json:"is_online"`
}

// 游戏服务器
type GameServer struct {
	players map[string]*Player
	logger  zaplog.Logger
}

func NewGameServer() *GameServer {
	return &GameServer{
		players: make(map[string]*Player),
		logger:  zaplog.DefaultLogger(),
	}
}

// 初始化Prometheus指标
func initMetrics() error {
	// HTTP请求指标
	err := prometheus_mgr.RegisterCounter("http_requests_total", prometheus_mgr.MetricOptions{
		Help:   "HTTP请求总数",
		Labels: []string{"method", "endpoint", "status"},
	})
	if err != nil {
		return err
	}

	err = prometheus_mgr.RegisterHistogram("http_request_duration_seconds", prometheus_mgr.MetricOptions{
		Help:    "HTTP请求耗时（秒）",
		Labels:  []string{"method", "endpoint"},
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	})
	if err != nil {
		return err
	}

	// 游戏业务指标
	err = prometheus_mgr.RegisterGauge("players_online_count", prometheus_mgr.MetricOptions{
		Help:   "当前在线玩家数量",
		Labels: []string{},
	})
	if err != nil {
		return err
	}

	err = prometheus_mgr.RegisterCounter("player_actions_total", prometheus_mgr.MetricOptions{
		Help:   "玩家操作总数",
		Labels: []string{"action", "result"},
	})
	if err != nil {
		return err
	}

	err = prometheus_mgr.RegisterHistogram("game_operation_duration_seconds", prometheus_mgr.MetricOptions{
		Help:    "游戏操作耗时（秒）",
		Labels:  []string{"operation"},
		Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})
	if err != nil {
		return err
	}

	// 系统指标
	err = prometheus_mgr.RegisterGauge("memory_usage_bytes", prometheus_mgr.MetricOptions{
		Help:   "内存使用量（字节）",
		Labels: []string{"type"},
	})
	if err != nil {
		return err
	}

	return nil
}

// Prometheus中间件
func prometheusMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method

		// 处理请求
		c.Next()

		// 记录指标
		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())

		labels := prometheus_mgr.MetricLabels{
			"method":   method,
			"endpoint": path,
			"status":   status,
		}

		// 记录请求总数
		_ = prometheus_mgr.IncCounter("http_requests_total", labels)

		// 记录请求耗时
		durationLabels := prometheus_mgr.MetricLabels{
			"method":   method,
			"endpoint": path,
		}
		_ = prometheus_mgr.ObserveHistogram("http_request_duration_seconds", duration, durationLabels)
	}
}

// 玩家登录
func (gs *GameServer) playerLogin(c *gin.Context) {
	start := time.Now()

	playerID := c.PostForm("player_id")
	playerName := c.PostForm("player_name")
	levelStr := c.DefaultPostForm("level", "1")

	if playerID == "" || playerName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing required fields"})
		return
	}

	level, _ := strconv.Atoi(levelStr)

	// 模拟登录处理时间
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)+50))

	player := &Player{
		ID:       playerID,
		Name:     playerName,
		Level:    level,
		LoginAt:  time.Now(),
		IsOnline: true,
	}

	gs.players[playerID] = player

	// 记录业务指标
	result := "success"
	if rand.Float32() < 0.05 { // 5%失败率
		result = "failed"
		c.JSON(http.StatusInternalServerError, gin.H{"error": "login failed"})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"message":   "login successful",
			"player_id": playerID,
			"level":     level,
		})
	}

	// 记录玩家操作
	actionLabels := prometheus_mgr.MetricLabels{
		"action": "login",
		"result": result,
	}
	_ = prometheus_mgr.IncCounter("player_actions_total", actionLabels)

	// 记录操作耗时
	duration := time.Since(start).Seconds()
	operationLabels := prometheus_mgr.MetricLabels{
		"operation": "login",
	}
	_ = prometheus_mgr.ObserveHistogram("game_operation_duration_seconds", duration, operationLabels)

	// 更新在线玩家数
	gs.updateOnlinePlayerCount()
}

// 玩家登出
func (gs *GameServer) playerLogout(c *gin.Context) {
	start := time.Now()

	playerID := c.PostForm("player_id")
	if playerID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing player_id"})
		return
	}

	// 模拟登出处理时间
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(50)+20))

	if player, exists := gs.players[playerID]; exists {
		player.IsOnline = false
		delete(gs.players, playerID)
		c.JSON(http.StatusOK, gin.H{"message": "logout successful"})
	} else {
		c.JSON(http.StatusNotFound, gin.H{"error": "player not found"})
	}

	// 记录玩家操作
	actionLabels := prometheus_mgr.MetricLabels{
		"action": "logout",
		"result": "success",
	}
	_ = prometheus_mgr.IncCounter("player_actions_total", actionLabels)

	// 记录操作耗时
	duration := time.Since(start).Seconds()
	operationLabels := prometheus_mgr.MetricLabels{
		"operation": "logout",
	}
	_ = prometheus_mgr.ObserveHistogram("game_operation_duration_seconds", duration, operationLabels)

	// 更新在线玩家数
	gs.updateOnlinePlayerCount()
}

// 获取玩家信息
func (gs *GameServer) getPlayerInfo(c *gin.Context) {
	start := time.Now()

	playerID := c.Param("id")

	// 模拟数据库查询时间
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(30)+10))

	if player, exists := gs.players[playerID]; exists {
		c.JSON(http.StatusOK, player)
	} else {
		c.JSON(http.StatusNotFound, gin.H{"error": "player not found"})
	}

	// 记录操作耗时
	duration := time.Since(start).Seconds()
	operationLabels := prometheus_mgr.MetricLabels{
		"operation": "get_player_info",
	}
	_ = prometheus_mgr.ObserveHistogram("game_operation_duration_seconds", duration, operationLabels)
}

// 获取在线玩家列表
func (gs *GameServer) getOnlinePlayers(c *gin.Context) {
	start := time.Now()

	// 模拟数据处理时间
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(50)+20))

	onlinePlayers := make([]*Player, 0, len(gs.players))
	for _, player := range gs.players {
		if player.IsOnline {
			onlinePlayers = append(onlinePlayers, player)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"count":   len(onlinePlayers),
		"players": onlinePlayers,
	})

	// 记录操作耗时
	duration := time.Since(start).Seconds()
	operationLabels := prometheus_mgr.MetricLabels{
		"operation": "get_online_players",
	}
	_ = prometheus_mgr.ObserveHistogram("game_operation_duration_seconds", duration, operationLabels)
}

// 更新在线玩家数量指标
func (gs *GameServer) updateOnlinePlayerCount() {
	onlineCount := 0
	for _, player := range gs.players {
		if player.IsOnline {
			onlineCount++
		}
	}

	_ = prometheus_mgr.SetGauge("players_online_count", float64(onlineCount), nil)
}

// 模拟系统指标更新
func (gs *GameServer) startSystemMetricsUpdater(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 模拟内存使用量
			heapUsage := float64(rand.Intn(100*1024*1024) + 50*1024*1024) // 50-150MB
			stackUsage := float64(rand.Intn(10*1024*1024) + 5*1024*1024)  // 5-15MB

			heapLabels := prometheus_mgr.MetricLabels{"type": "heap"}
			stackLabels := prometheus_mgr.MetricLabels{"type": "stack"}

			_ = prometheus_mgr.SetGauge("memory_usage_bytes", heapUsage, heapLabels)
			_ = prometheus_mgr.SetGauge("memory_usage_bytes", stackUsage, stackLabels)

		case <-ctx.Done():
			return
		}
	}
}

// 模拟玩家活动
func (gs *GameServer) simulatePlayerActivity(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 随机模拟玩家登录
			if rand.Float32() < 0.3 { // 30%概率有新玩家登录
				playerID := fmt.Sprintf("player_%d", rand.Intn(10000))
				player := &Player{
					ID:       playerID,
					Name:     fmt.Sprintf("Player_%d", rand.Intn(1000)),
					Level:    rand.Intn(100) + 1,
					LoginAt:  time.Now(),
					IsOnline: true,
				}
				gs.players[playerID] = player

				// 记录登录操作
				actionLabels := prometheus_mgr.MetricLabels{
					"action": "login",
					"result": "success",
				}
				_ = prometheus_mgr.IncCounter("player_actions_total", actionLabels)
			}

			// 随机模拟玩家登出
			if len(gs.players) > 0 && rand.Float32() < 0.2 { // 20%概率有玩家登出
				for playerID := range gs.players {
					delete(gs.players, playerID)

					// 记录登出操作
					actionLabels := prometheus_mgr.MetricLabels{
						"action": "logout",
						"result": "success",
					}
					_ = prometheus_mgr.IncCounter("player_actions_total", actionLabels)
					break
				}
			}

			// 更新在线玩家数
			gs.updateOnlinePlayerCount()

		case <-ctx.Done():
			return
		}
	}
}

func main() {
	// 初始化日志
	logger := zaplog.DefaultLogger()

	// 初始化Prometheus Manager
	err := prometheus_mgr.InitDefaultManager(logger,
		prometheus_mgr.WithServiceName("game-server-example"),
		prometheus_mgr.WithHTTPPort(19090),
		prometheus_mgr.WithNamespace("game"),
		prometheus_mgr.WithDefaultLabel("server", "game-server-1"),
		prometheus_mgr.WithDefaultLabel("environment", "demo"),
	)
	if err != nil {
		log.Fatal("Failed to init prometheus manager:", err)
	}

	// 启动Prometheus Manager
	ctx := context.Background()
	manager := prometheus_mgr.GetDefaultManager()
	if err := manager.Start(ctx); err != nil {
		log.Fatal("Failed to start prometheus manager:", err)
	}
	defer manager.Stop(ctx)

	// 初始化指标
	if err := initMetrics(); err != nil {
		log.Fatal("Failed to init metrics:", err)
	}

	// 创建游戏服务器
	gameServer := NewGameServer()

	// 启动后台任务
	go gameServer.startSystemMetricsUpdater(ctx)
	go gameServer.simulatePlayerActivity(ctx)

	// 设置Gin为发布模式
	gin.SetMode(gin.ReleaseMode)

	// 创建HTTP服务器
	r := gin.New()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())
	r.Use(prometheusMiddleware())

	// API路由
	api := r.Group("/api/v1")
	{
		api.POST("/player/login", gameServer.playerLogin)
		api.POST("/player/logout", gameServer.playerLogout)
		api.GET("/player/:id", gameServer.getPlayerInfo)
		api.GET("/players/online", gameServer.getOnlinePlayers)
	}

	// 健康检查
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "ok",
			"timestamp": time.Now().Unix(),
			"service":   "game-server-example",
		})
	})

	// 启动HTTP服务器
	log.Println("🎮 Game Server starting on :18080")
	log.Println("📊 Metrics available at http://localhost:19090/metrics")
	log.Println("🏥 Health check at http://localhost:18080/health")
	log.Println("")
	log.Println("API Endpoints:")
	log.Println("  POST /api/v1/player/login")
	log.Println("  POST /api/v1/player/logout")
	log.Println("  GET  /api/v1/player/:id")
	log.Println("  GET  /api/v1/players/online")

	if err := http.ListenAndServe(":18080", r); err != nil {
		log.Fatal("Failed to start HTTP server:", err)
	}
}
