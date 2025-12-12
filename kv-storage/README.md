# KVStorage Component

`kvstorage` 是一个高性能、通用的键值存储组件。它屏蔽了底层存储的差异，提供了一致的 **Key-Field-Value** 操作接口，并内置了字段级过期管理、并发控制及 Cache Aside 模式支持。

## 主要特性

- **多后端支持**：
    - **Redis**：基于 Redis Hash 结构实现，支持持久化和分布式。
    - **Memory**：基于 Go Map + RWMutex 实现，零序列化开销，极速读写。
- **统一接口**：无论底层是 Redis 还是内存，对外暴露一致的 `Store` 接口。
- **字段级过期 (Field TTL)**：
    - 支持为 Hash 结构中的单个字段设置独立的过期时间。
    - 通过 `Entry` 结构体或 `Item` 接口强制管理生命周期。
- **原子惰性删除**：
    - 读取时自动检查过期状态。如果发现过期，会原子性地删除脏数据（Redis 使用 Lua 脚本，Memory 使用双重检查锁），防止内存泄漏。
- **并发控制**：
    - 提供 `WithLock` 选项，支持细粒度的锁保护。
- **自动回源 (Cache Aside)**：
    - 提供 `WithFetch` 选项，当缓存未命中时自动执行回调并回填缓存。

## 接口定义

核心接口 `Store` 定义如下：

```go
type Store interface {
    // Put 设置字段值。
    // item: 必须实现 Item 接口(如 kvstorage.Entry)，指定值和 TTL。
    Put(ctx context.Context, key, field string, item Item, opts ...Option) error

    // Get 获取字段值。
    // 返回 Item 接口，通过 item.GetValue() 获取原始数据。
    // 若数据不存在或已过期，返回 ErrMiss。如果是 Redis 实现，反序列化需要注意类型断言。
    Get(ctx context.Context, key, field string, opts ...Option) (Item, error)

    // Remove 删除字段。
    Remove(ctx context.Context, key string, fields []string, opts ...Option) error

    // Has 判断字段是否存在且未过期。
    Has(ctx context.Context, key, field string, opts ...Option) (bool, error)

    // Close 关闭连接。
    Close() error
}
```

## 快速开始

### 1. 初始化

```go
import "your/project/infrastructure/component/kvstorage"

// --- 初始化 Redis 版本 ---
redisStore, err := kvstorage.New(kvstorage.RedisConfig{
    Addr:         "127.0.0.1:6379",
    Password:     "123456",
    DB:           0,
    PoolSize:     100,
    MinIdleConns: 10,
})

// --- 初始化 内存 版本 ---
memStore, err := kvstorage.NewMem(kvstorage.MemConfig{})
```

### 2. 基础读写

组件强制使用 `kvstorage.Entry` 来明确数据的生命周期。

```go
ctx := context.Background()

// 写入数据：TTL 为 0 表示永不过期
err := store.Put(ctx, "user:101", "name", kvstorage.Entry{
    Val: "Alice",
    TTL: 0, 
})

// 读取数据
item, err := store.Get(ctx, "user:101", "name")
if err != nil {
    if errors.Is(err, kvstorage.ErrMiss) {
        // 数据不存在或已过期
    }
    return
}

// 获取原始值
fmt.Println(item.GetValue()) // "Alice"
```

### 3. 设置过期时间

```go
// 该字段 5 分钟后逻辑过期
store.Put(ctx, "session:1", "token", kvstorage.Entry{
    Val: "xyz-token",
    TTL: 5 * time.Minute,
})
```

### 4. 自动回源 (Cache Aside)

使用 `WithFetch` 可以在缓存未命中时自动调用函数获取数据并写入缓存，防止缓存击穿。

```go
// 定义回源函数
fetchUser := func() (kvstorage.Item, error) {
    // 模拟从数据库查询
    user := db.QueryUser(101)
    // 返回带 TTL 的数据
    return kvstorage.Entry{Val: user, TTL: 10 * time.Minute}, nil
}

// 获取数据，如果未命中则自动调用 fetchUser 并回填
item, err := store.Get(ctx, "cache:user", "101", kvstorage.WithFetch(fetchUser))
```

### 5. 并发控制 (锁)

如果需要防止并发写入冲突（例如：读取-修改-写入），可以使用 `WithLock` 选项。

```go
// 在操作前自动获取锁，操作后自动释放
// 锁的有效期为 2 秒 (防止死锁)
err := store.Put(ctx, "resource", "count", 
    kvstorage.Entry{Val: 100, TTL: 0}, 
    kvstorage.WithLock(2 * time.Second),
)
```

## 注意事项

1. **数据类型差异**：
    - **Redis 版**：底层使用 JSON 序列化存储。`GetValue()` 返回的复杂对象（如结构体）如果是 `interface{}` 接收，可能会变成 `map[string]interface{}`。建议尽量存储基本类型或字节流，或者在业务层进行转换。
    - **Memory 版**：直接存储 Go 对象引用。
2. **惰性删除**：过期数据只在访问（Get/Has）时才会被物理删除。对于 Redis 实现，建议配合定期的 Redis Key 清理策略（如 `LRU`）以防止冷数据堆积。