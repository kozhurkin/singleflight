package singleflight

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkDo_NoCache_Deduplication проверяет производительность дедупликации
// при высокой конкурентности без кеширования.
func BenchmarkDo_NoCache_Deduplication(b *testing.B) {
	g := NewGroup[string, int]()

	var calls int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		// Симуляция работы функции (например, запрос к БД)
		time.Sleep(1 * time.Millisecond)
		return 42, nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = g.Do("key", fn)
		}
	})
}

// BenchmarkDo_WithCache_HitRate проверяет производительность доступа к кешу
// при повторных запросах (cache hit).
func BenchmarkDo_WithCache_HitRate(b *testing.B) {
	const cacheTime = 10 * time.Second
	g := NewGroupWithCache[string, int](cacheTime, false, 0)

	var calls int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 42, nil
	}

	// Предварительно заполняем кеш
	_, _ = g.Do("key", fn)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = g.Do("key", fn)
		}
	})
}

// BenchmarkDo_WithCache_Mixed проверяет производительность при смешанной нагрузке:
// часть запросов попадает в кеш, часть требует пересчета.
func BenchmarkDo_WithCache_Mixed(b *testing.B) {
	const cacheTime = 100 * time.Millisecond
	g := NewGroupWithCache[string, int](cacheTime, false, 0)

	var calls int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(1 * time.Millisecond)
		return 42, nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = g.Do("key", fn)
			// Периодически ждем, чтобы кеш протухал
			if b.N%100 == 0 {
				time.Sleep(cacheTime + 10*time.Millisecond)
			}
		}
	})
}

// BenchmarkDo_MultipleKeys проверяет производительность при работе
// с множеством разных ключей одновременно.
func BenchmarkDo_MultipleKeys(b *testing.B) {
	g := NewGroupWithCache[string, int](5*time.Second, false, 0)

	var calls int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(100 * time.Microsecond)
		return 42, nil
	}

	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = string(rune('a' + (i % 26)))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := keys[i%len(keys)]
			_, _ = g.Do(key, fn)
			i++
		}
	})
}

// BenchmarkDo_HighConcurrency проверяет производительность при очень высокой
// конкурентности (много горутин одновременно запрашивают один ключ).
func BenchmarkDo_HighConcurrency(b *testing.B) {
	g := NewGroup[string, int]()

	var calls int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(5 * time.Millisecond)
		return 42, nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var wg sync.WaitGroup
			// Запускаем 100 горутин одновременно для одного ключа
			for j := 0; j < 100; j++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, _ = g.Do("key", fn)
				}()
			}
			wg.Wait()
		}
	})
}

// BenchmarkDo_Warming проверяет производительность механизма прогрева.
func BenchmarkDo_Warming(b *testing.B) {
	const (
		cacheTime = 10 * time.Millisecond
		warmTime  = 5 * time.Millisecond
	)
	g := NewGroupWithCache[string, int](cacheTime, false, warmTime)

	var calls int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return int(atomic.LoadInt32(&calls)), nil
	}

	// Первый вызов для заполнения кеша
	_, _ = g.Do("key", fn)

	// Ждем истечения TTL
	time.Sleep(cacheTime + 1*time.Millisecond)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = g.Do("key", fn)
		}
	})
}

// BenchmarkDo_CacheErrors проверяет производительность при кешировании ошибок.
func BenchmarkDo_CacheErrors(b *testing.B) {
	const cacheTime = 10 * time.Second
	g := NewGroupWithCache[string, int](cacheTime, true, 0)

	var calls int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 0, nil // Симулируем ошибку
	}

	// Предварительно заполняем кеш ошибкой
	_, _ = g.Do("key", fn)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = g.Do("key", fn)
		}
	})
}

// BenchmarkDo_Sequential проверяет производительность последовательных вызовов
// (без конкурентности, для сравнения с параллельными тестами).
func BenchmarkDo_Sequential(b *testing.B) {
	g := NewGroupWithCache[string, int](5*time.Second, false, 0)

	var calls int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 42, nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = g.Do("key", fn)
	}
}

// BenchmarkDo_RealWorldSimulation симулирует реальный сценарий использования:
// множество разных ключей, смешанная нагрузка, кеширование.
func BenchmarkDo_RealWorldSimulation(b *testing.B) {
	const (
		cacheTime = 100 * time.Millisecond
		warmTime  = 50 * time.Millisecond
	)
	g := NewGroupWithCache[string, int](cacheTime, false, warmTime)

	var calls int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		// Симуляция запроса к внешнему API
		time.Sleep(2 * time.Millisecond)
		return 42, nil
	}

	// 100 различных ключей
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = string(rune('a'+(i%26))) + string(rune('0'+(i%10)))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := keys[i%len(keys)]
			_, _ = g.Do(key, fn)
			i++
		}
	})
}

// BenchmarkDo_ConcurrentDifferentKeys проверяет производительность при одновременной
// работе с разными ключами (каждый ключ обрабатывается независимо).
func BenchmarkDo_ConcurrentDifferentKeys(b *testing.B) {
	g := NewGroup[string, int]()

	var calls int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(1 * time.Millisecond)
		return 42, nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		keyID := 0
		for pb.Next() {
			key := string(rune('a' + (keyID % 26)))
			_, _ = g.Do(key, fn)
			keyID++
		}
	})
}
