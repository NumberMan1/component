package storage

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- 测试环境准备 ---

type testData struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	score float64
}

func (td *testData) MarshalBinary() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"id":   td.ID,
		"name": td.Name,
	})
}

func (td *testData) UnmarshalBinary(data []byte) error {
	var tempMap map[string]interface{}
	if err := json.Unmarshal(data, &tempMap); err != nil {
		return err
	}
	if id, ok := tempMap["id"].(float64); ok {
		td.ID = int(id)
	}
	if name, ok := tempMap["name"].(string); ok {
		td.Name = name
	}
	return nil
}

func (td *testData) Score() float64 {
	return td.score
}

func (td *testData) SetScore(s float64) {
	td.score = s
}

func testDataFactory() StorageData {
	return &testData{}
}

func sortedTestDataFactory() SortedSetData {
	return &testData{}
}

func setupRedisClient(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "123456",
		DB:       1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := client.Ping(ctx).Err()
	require.NoError(t, err, "无法连接到 Redis 测试数据库")

	cleanupCtx := context.Background()
	t.Cleanup(func() {
		require.NoError(t, client.FlushDB(cleanupCtx).Err())
		require.NoError(t, client.Close())
	})

	return client
}

// --- KV 测试 ---

func TestRedisKV(t *testing.T) {
	client := setupRedisClient(t)
	key := "test:kv:user:1"
	kvStore := NewRedisKV(client, key)
	ctx := context.Background()

	t.Run("Set and Get", func(t *testing.T) {
		setData := &testData{ID: 1, Name: "Alice"}
		err := kvStore.Set(ctx, setData)
		require.NoError(t, err)

		getData := &testData{}
		err = kvStore.Get(ctx, getData)
		require.NoError(t, err)
		assert.Equal(t, setData.ID, getData.ID)
		assert.Equal(t, setData.Name, getData.Name)
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		err := client.Del(ctx, key).Err()
		require.NoError(t, err)

		getData := &testData{}
		err = kvStore.Get(ctx, getData)
		assert.Equal(t, ErrFieldNotFound, err)
	})

	t.Run("Transaction Commit", func(t *testing.T) {
		initialData := &testData{ID: 10, Name: "Initial"}
		err := kvStore.Set(ctx, initialData)
		require.NoError(t, err)

		tx, err := kvStore.BeginTx(ctx)
		require.NoError(t, err)
		txData := &testData{ID: 20, Name: "InTx"}
		err = tx.Set(txData)
		require.NoError(t, err)

		getTxData := &testData{}
		err = tx.Get(getTxData)
		require.NoError(t, err)
		assert.Equal(t, txData.ID, getTxData.ID)

		err = tx.Commit(ctx)
		require.NoError(t, err)

		getFinalData := &testData{}
		err = kvStore.Get(ctx, getFinalData)
		require.NoError(t, err)
		assert.Equal(t, txData.ID, getFinalData.ID)
	})

	t.Run("Transaction Rollback", func(t *testing.T) {
		initialData := &testData{ID: 30, Name: "BeforeRollback"}
		err := kvStore.Set(ctx, initialData)
		require.NoError(t, err)

		tx, err := kvStore.BeginTx(ctx)
		require.NoError(t, err)
		err = tx.Set(&testData{ID: 40, Name: "ShouldBeRolledBack"})
		require.NoError(t, err)

		tx.Rollback()

		getFinalData := &testData{}
		err = kvStore.Get(ctx, getFinalData)
		require.NoError(t, err)
		assert.Equal(t, initialData.ID, getFinalData.ID)
	})
}

// --- Hash 测试 ---

func TestRedisHash(t *testing.T) {
	client := setupRedisClient(t)
	key := "test:hash:user_profiles"
	hashStore := NewRedisHash(client, key, testDataFactory)
	ctx := context.Background()

	t.Run("HSet, HGet, HDel, and HGetAll", func(t *testing.T) {
		data1 := &testData{ID: 1, Name: "Alice"}
		data2 := &testData{ID: 2, Name: "Bob"}
		err := hashStore.HSet(ctx, "user:1", data1)
		require.NoError(t, err)
		err = hashStore.HSet(ctx, "user:2", data2)
		require.NoError(t, err)

		getData, err := hashStore.HGet(ctx, "user:1")
		require.NoError(t, err)
		assert.Equal(t, data1.ID, getData.(*testData).ID)

		allData, err := hashStore.HGetAll(ctx)
		require.NoError(t, err)
		require.Len(t, allData, 2)
		assert.Equal(t, data1.ID, allData["user:1"].(*testData).ID)
		assert.Equal(t, data2.ID, allData["user:2"].(*testData).ID)

		err = hashStore.HDel(ctx, "user:1")
		require.NoError(t, err)
		_, err = hashStore.HGet(ctx, "user:1")
		assert.Equal(t, ErrFieldNotFound, err)

		allData, err = hashStore.HGetAll(ctx)
		require.NoError(t, err)
		assert.Len(t, allData, 1)
	})

	t.Run("Hash Transaction Commit", func(t *testing.T) {
		client.Del(ctx, key)
		initialData := &testData{ID: 10, Name: "Initial"}
		err := hashStore.HSet(ctx, "user:10", initialData)
		require.NoError(t, err)

		tx, err := hashStore.BeginTx(ctx)
		require.NoError(t, err)

		updatedData := &testData{ID: 11, Name: "Updated"}
		newData := &testData{ID: 12, Name: "New"}
		err = tx.HSet("user:10", updatedData)
		require.NoError(t, err)
		err = tx.HSet("user:12", newData)
		require.NoError(t, err)
		err = tx.HDel("user:non-existent")
		require.NoError(t, err)

		allTxData, err := tx.HGetAll(testDataFactory)
		require.NoError(t, err)
		assert.Len(t, allTxData, 2)
		assert.Equal(t, updatedData.ID, allTxData["user:10"].(*testData).ID)

		err = tx.Commit(ctx)
		require.NoError(t, err)

		finalData, err := hashStore.HGetAll(ctx)
		require.NoError(t, err)
		assert.Len(t, finalData, 2)
		assert.Equal(t, updatedData.ID, finalData["user:10"].(*testData).ID)
		assert.Equal(t, newData.ID, finalData["user:12"].(*testData).ID)
	})
}

// --- SortedSet 测试 ---

func TestRedisZSet(t *testing.T) {
	client := setupRedisClient(t)
	key := "test:zset:leaderboard"
	zsetStore := NewRedisZSet(client, key, sortedTestDataFactory)
	ctx := context.Background()

	t.Run("ZAdd, ZRange, and ZRem", func(t *testing.T) {
		p1 := &testData{ID: 1, Name: "Player1", score: 100}
		p2 := &testData{ID: 2, Name: "Player2", score: 200}
		p3 := &testData{ID: 3, Name: "Player3", score: 50}
		require.NoError(t, zsetStore.ZAdd(ctx, p1))
		require.NoError(t, zsetStore.ZAdd(ctx, p2))
		require.NoError(t, zsetStore.ZAdd(ctx, p3))

		res, err := zsetStore.ZRange(ctx, 0, -1)
		require.NoError(t, err)
		require.Len(t, res, 3)
		assert.Equal(t, p3.ID, res[0].(*testData).ID)
		assert.Equal(t, p3.score, res[0].Score())
		assert.Equal(t, p1.ID, res[1].(*testData).ID)
		assert.Equal(t, p1.score, res[1].Score())
		assert.Equal(t, p2.ID, res[2].(*testData).ID)
		assert.Equal(t, p2.score, res[2].Score())
	})

	t.Run("ZTrim", func(t *testing.T) {
		client.Del(ctx, key)
		for i := 1; i <= 5; i++ {
			p := &testData{ID: i, Name: "P" + strconv.Itoa(i), score: float64(i * 10)}
			require.NoError(t, zsetStore.ZAdd(ctx, p))
		}
		require.NoError(t, zsetStore.ZTrimByTopN(ctx, 2))
		res, err := zsetStore.ZRange(ctx, 0, -1)
		require.NoError(t, err)
		require.Len(t, res, 2)
		assert.Equal(t, 10.0, res[0].Score())
		assert.Equal(t, 20.0, res[1].Score())

		client.Del(ctx, key)
		for i := 1; i <= 5; i++ {
			p := &testData{ID: i, Name: "P" + strconv.Itoa(i), score: float64(i * 10)}
			require.NoError(t, zsetStore.ZAdd(ctx, p))
		}
		require.NoError(t, zsetStore.ZRevTrimByTopN(ctx, 2))
		res, err = zsetStore.ZRange(ctx, 0, -1)
		require.NoError(t, err)
		require.Len(t, res, 2)
		assert.Equal(t, 40.0, res[0].Score())
		assert.Equal(t, 50.0, res[1].Score())
	})

	t.Run("ZSet Transaction Commit", func(t *testing.T) {
		client.Del(ctx, key)
		p1 := &testData{ID: 1, Name: "P1", score: 100}
		p2 := &testData{ID: 2, Name: "P2", score: 200}
		require.NoError(t, zsetStore.ZAdd(ctx, p1))
		require.NoError(t, zsetStore.ZAdd(ctx, p2))

		tx, err := zsetStore.BeginTx(ctx)
		require.NoError(t, err)

		p3 := &testData{ID: 3, Name: "P3", score: 50}
		require.NoError(t, tx.ZAdd(p3))
		require.NoError(t, tx.ZRem(p2))

		res, err := tx.ZRange(0, -1)
		require.NoError(t, err)
		require.Len(t, res, 2)
		assert.Equal(t, p3.ID, res[0].(*testData).ID)
		assert.Equal(t, p1.ID, res[1].(*testData).ID)

		require.NoError(t, tx.Commit(ctx))

		finalRes, err := zsetStore.ZRange(ctx, 0, -1)
		require.NoError(t, err)

		getIDs := func(data []SortedSetData) []int {
			ids := make([]int, len(data))
			for i, v := range data {
				ids[i] = v.(*testData).ID
			}
			return ids
		}
		assert.ElementsMatch(t, getIDs(res), getIDs(finalRes))
	})
}

// --- 并发安全验证 ---

func TestTransaction_KV_Conflict(t *testing.T) {
	client := setupRedisClient(t)
	key := "test:kv:conflict"
	kvStore := NewRedisKV(client, key)
	ctx := context.Background()

	// 初始状态
	err := kvStore.Set(ctx, &testData{ID: 1, Name: "Initial"})
	require.NoError(t, err)

	// 从同一状态开始两个事务
	tx1, err := kvStore.BeginTx(ctx)
	require.NoError(t, err)
	tx2, err := kvStore.BeginTx(ctx)
	require.NoError(t, err)

	// Tx1 修改并成功提交
	err = tx1.Set(&testData{ID: 2, Name: "From Tx1"})
	require.NoError(t, err)
	err = tx1.Commit(ctx)
	require.NoError(t, err)

	// Tx2 基于过时的数据进行修改并尝试提交
	err = tx2.Set(&testData{ID: 3, Name: "From Tx2"})
	require.NoError(t, err)
	err = tx2.Commit(ctx)

	// 断言：Tx2 的提交应该失败，并返回冲突错误
	require.Error(t, err, "Expected a transaction conflict error")
	assert.Equal(t, ErrTransactionConflict, err)

	// 验证：最终数据库中的值应为 Tx1 修改的值
	finalData := &testData{}
	err = kvStore.Get(ctx, finalData)
	require.NoError(t, err)
	assert.Equal(t, 2, finalData.ID)
	assert.Equal(t, "From Tx1", finalData.Name)
}

func TestTransaction_Hash_Conflict(t *testing.T) {
	client := setupRedisClient(t)
	key := "test:hash:conflict"
	hashStore := NewRedisHash(client, key, testDataFactory)
	ctx := context.Background()

	// 初始状态
	err := hashStore.HSet(ctx, "user:1", &testData{ID: 1, Name: "Initial"})
	require.NoError(t, err)

	// 从同一状态开始两个事务
	tx1, err := hashStore.BeginTx(ctx)
	require.NoError(t, err)
	tx2, err := hashStore.BeginTx(ctx)
	require.NoError(t, err)

	// Tx1 修改并成功提交
	err = tx1.HSet("user:1", &testData{ID: 2, Name: "From Tx1"})
	require.NoError(t, err)
	err = tx1.Commit(ctx)
	require.NoError(t, err)

	// Tx2 基于过时的数据进行修改并尝试提交
	err = tx2.HSet("user:1", &testData{ID: 3, Name: "From Tx2"})
	require.NoError(t, err)
	err = tx2.Commit(ctx)

	// 断言：Tx2 的提交应该失败，并返回冲突错误
	require.Error(t, err, "Expected a transaction conflict error")
	assert.Equal(t, ErrTransactionConflict, err)

	// 验证：最终数据库中的值应为 Tx1 修改的值
	finalData, err := hashStore.HGet(ctx, "user:1")
	require.NoError(t, err)
	assert.Equal(t, 2, finalData.(*testData).ID)
	assert.Equal(t, "From Tx1", finalData.(*testData).Name)
}
