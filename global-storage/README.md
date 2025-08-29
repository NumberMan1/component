# Go Storage Library: Concurrently Safe Storage Abstraction Layer

[](https://www.google.com/search?q=https://goreportcard.com/report/github.com/NumberMan1/component/global-storage)
[](https://www.google.com/search?q=https://godoc.org/github.com/NumberMan1/component/global-storage)

这是一个为 Go 应用程序设计的、并发安全的通用存储抽象库。它提供了一个简洁的接口，用于操作多种数据结构（如 KV、Hash、Sorted Set），并将底层的 Redis 实现细节完全封装。

本库最大的特点是其内置的**基于 Redis `WATCH` 命令的乐观锁事务模型**，可以从根本上解决并发写入场景下的“更新丢失”问题，确保数据一致性。

## ✨ 主要特性

* **统一的存储管理器**：通过 `StorageManager` 统一创建和管理所有存储实例。
* **多种数据结构支持**：
  * **KV**：单键值对存储。
  * **Hash**：类似于 `map` 的字段-值存储。
  * **Sorted Set (ZSet)**：按分数排序的唯一成员集合。
* **并发安全的事务**：
  * 提供 `BeginTx()`, `Commit()`, `Rollback()` 事务接口。
  * `Commit` 方法内置了乐观锁，能在并发冲突时自动检测并返回 `ErrTransactionConflict` 错误。
* **接口驱动设计**：完全面向接口编程 (`KVTransactional`, `HashTransactional` 等)，易于扩展和模拟（Mock）测试。
* **清晰的错误处理**：定义了如 `ErrFieldNotFound` 和 `ErrTransactionConflict` 等标准错误，便于业务逻辑处理。

## 📦 安装

```bash
go get github.com/NumberMan1/component/global-storage
```

## 🚀 快速上手

### 1\. 定义你的数据结构

首先，定义你需要存储的数据结构，并实现 `storage.StorageData` 接口（如果需要用于 ZSet，则实现 `storage.SortedSetData`）。

```go
package main

import (
    "encoding/json"
    "github.com/NumberMan1/component/global-storage"
)

// UserProfile 实现了 storage.StorageData 接口
type UserProfile struct {
    ID   int    `json:"id"`
    Name string `json:"name"`
    // ZSet 相关字段
    score float64
}

// MarshalBinary 序列化方法（用于写入 Redis）
func (u *UserProfile) MarshalBinary() ([]byte, error) {
    return json.Marshal(map[string]interface{}{
        "id":   u.ID,
        "name": u.Name,
    })
}

// UnmarshalBinary 反序列化方法（用于从 Redis 读取）
func (u *UserProfile) UnmarshalBinary(data []byte) error {
    var tempMap map[string]interface{}
    if err := json.Unmarshal(data, &tempMap); err != nil {
        return err
    }
    if id, ok := tempMap["id"].(float64); ok {
        u.ID = int(id)
    }
    if name, ok := tempMap["name"].(string); ok {
        u.Name = name
    }
    return nil
}

// Score 返回 ZSet 分数
func (u *UserProfile) Score() float64 {
    return u.score
}

// SetScore 设置 ZSet 分数
func (u *UserProfile) SetScore(s float64) {
    u.score = s
}

// 工厂函数，用于在读取时创建 UserProfile 实例
func userProfileFactory() storage.StorageData {
    return &UserProfile{}
}

func sortedUserProfileFactory() storage.SortedSetData {
	return &UserProfile{}
}
```

### 2\. 初始化管理器并注册存储实例

在你的应用启动时，初始化 `StorageManager`。

```go
package main

import (
    "context"
    "fmt"
    "log"
    "github.com/NumberMan1/component/global-storage"
)

func main() {
    // 1. 配置管理器
    config := storage.ManagerConfig{
        RedisAddr: "localhost:6379",
        RedisPass: "123456",         // 如果没有密码则留空
        RedisDB:   1,                // 建议为测试和开发使用非 0 数据库
    }

    if err := storage.InitManager(config); err != nil {
        log.Fatalf("无法初始化存储管理器: %v", err)
    }
    defer storage.GlobalManager().Close() // 确保程序退出时关闭连接

    // 2. 注册不同类型的存储实例
    // Redis Key 将会是 "profiles"
    err := storage.GlobalManager().RegisterHashStorage("profiles", userProfileFactory)
    if err != nil {
        log.Fatalf("注册 Hash 存储失败: %v", err)
    }

    // Redis Key 将会是 "leaderboard"
    err = storage.GlobalManager().RegisterSortedSetStorage("leaderboard", sortedUserProfileFactory)
    if err != nil {
        log.Fatalf("注册 ZSet 存储失败: %v", err)
    }
}
```

### 3\. 基本操作

使用 `StorageManager` 来获取并操作已注册的存储实例。

```go
func basicOperations() {
    ctx := context.Background()

    // 获取已注册的 Hash 存储
    profiles, err := storage.GlobalManager().GetHash("profiles")
    if err != nil {
        // ...
    }

    // HSet 操作
    alice := &UserProfile{ID: 1, Name: "Alice"}
    err = profiles.HSet(ctx, "user:1", alice)
    if err != nil {
        // ...
    }

    // HGet 操作
    retrievedProfile := &UserProfile{}
    data, err := profiles.HGet(ctx, "user:1")
	if err != nil {
		// ...
	}
	retrievedProfile = data.(*UserProfile)

    fmt.Printf("成功获取用户: ID=%d, Name=%s\n", retrievedProfile.ID, retrievedProfile.Name)
}
```

### 4\. 使用事务（并发安全）

事务是本库的核心功能。以下示例展示了如何安全地更新一个共享资源（例如：一个计数器），并处理可能发生的并发冲突。

```go
// 假设有一个 KV 存储用于存储一个计数器
// key: "app:counter", value: UserProfile{ID: count}
storage.GlobalManager().RegisterKVStorage("app:counter")
counter, _ := storage.GlobalManager().GetKV("app:counter")
counter.Set(context.Background(), &UserProfile{ID: 0})

// --- 安全地增加计数器 ---
const maxRetries = 5
var success = false

for i := 0; i < maxRetries; i++ {
    tx, err := counter.BeginTx(context.Background())
    if err != nil {
        log.Printf("开始事务失败: %v", err)
        break
    }

    // 1. 在事务内读取当前值
    currentValue := &UserProfile{}
    if err := tx.Get(currentValue); err != nil {
        // ... 处理错误，可能是 key 不存在
    }

    // 2. 在内存中进行修改
    currentValue.ID++

    // 3. 将修改写入事务
    if err := tx.Set(currentValue); err != nil {
        // ...
    }

    // 4. 提交事务
    err = tx.Commit(context.Background())
    if err == nil {
        // 提交成功！
        fmt.Println("计数器更新成功！")
        success = true
        break
    }

    // 如果提交失败，检查是否是并发冲突错误
    if err == storage.ErrTransactionConflict {
        fmt.Printf("事务冲突，正在进行第 %d 次重试...\n", i+1)
        // 等待一个短暂的随机时间后重试
        // time.Sleep(time.Duration(20+rand.Intn(50)) * time.Millisecond)
        continue
    }

    // 如果是其他错误，则记录并终止
    log.Printf("发生未知错误，终止重试: %v", err)
    break
}

if !success {
    log.Println("更新计数器失败，已达到最大重试次数。")
}
```

## 🔬 核心概念

### 接口驱动

本库的核心是 `interface.go` 文件中定义的一系列接口。这种设计允许：

* **实现替换**：未来可以轻易地添加新的底层存储实现（如 `etcd`, `TiKV`），而无需改动业务代码。
* **轻松测试**：在单元测试中，可以轻松地模拟（Mock）这些接口，使测试不依赖于外部数据库。

### 乐观锁事务

本库的事务并非传统的关系型数据库的行级锁或表级锁。它采用的是一种**乐观锁**机制：

1.  **`BeginTx`**: 从 Redis 获取数据的**快照**到内存中。
2.  **事务内操作**: 所有的读写都发生在内存中的快照和操作队列上。
3.  **`Commit`**:
  * 向 Redis 发送 `WATCH` 命令，监视事务涉及的 key。
  * 将所有写操作放入 `MULTI...EXEC` 队列中。
  * 如果从 `WATCH` 到 `EXEC` 之间，被监视的 key 没有被其他客户端修改，则 `EXEC` 成功。
  * 如果 key 在此期间被修改，`EXEC` 将失败，`Commit` 方法返回 `ErrTransactionConflict` 错误。

这种无锁的设计在高并发读多写少的场景下性能极佳，并通过冲突检测和重试机制保证了最终的数据一致性。

## ✅ 运行测试

要运行本库的单元测试，您需要一个本地可访问的 Redis 实例。

1.  修改 `storage_test.go` 文件中的 `setupRedisClient` 函数，填入您的 Redis 连接信息。
2.  在项目根目录下运行：

<!-- end list -->

```bash
go test -v ./...
```