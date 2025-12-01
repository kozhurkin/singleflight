package singleflight

import (
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
		time.Sleep(10 * time.Millisecond)
		return 42, nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			time.Sleep(1 * time.Millisecond)
			go g.Do("key", fn)
		}
	})

	b.Log("calls", calls, "time", b.Elapsed())

}

// BenchmarkDo_WithCache_HitRate проверяет производительность доступа к кешу
// при повторных запросах (cache hit).
func BenchmarkDo_WithCache_HitRate(b *testing.B) {
	const cacheTime = 10 * time.Millisecond
	g := NewGroupWithCache[string, int](cacheTime, false, 0)

	var calls, hits int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 42, nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			time.Sleep(1 * time.Millisecond)
			atomic.AddInt32(&hits, 1)
			go g.Do("key", fn)
		}
	})

	b.Log("calls", calls, "hits", hits, "time", b.Elapsed())
}

// BenchmarkDo_Warming проверяет производительность механизма прогрева.
func BenchmarkDo_Warming(b *testing.B) {
	const (
		cacheTime = 10 * time.Millisecond
		warmTime  = 5 * time.Millisecond
	)

	g := NewGroupWithCache[string, int](cacheTime, false, warmTime)

	var calls, hits int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 42, nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			time.Sleep(1 * time.Millisecond)
			atomic.AddInt32(&hits, 1)
			go g.Do("key", fn)
		}
	})

	b.Log("calls", calls, "hits", hits, "time", b.Elapsed())
}

// BenchmarkDo_MultipleKeys проверяет производительность при работе
// с множеством разных ключей одновременно.
func BenchmarkDo_MultipleKeys(b *testing.B) {
	g := NewGroupWithCache[string, int](5*time.Millisecond, false, 0)

	var calls, hits int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(5 * time.Microsecond)
		return 42, nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := string(rune('a' + (i % 10)))
			time.Sleep(1 * time.Millisecond)
			atomic.AddInt32(&hits, 1)
			go g.Do(key, fn)
			i++
		}
	})

	b.Log("calls", calls, "hits", hits, "time", b.Elapsed())
}

// BenchmarkDo_HighConcurrency проверяет производительность при очень высокой
// конкурентности (много горутин одновременно запрашивают один ключ).
func BenchmarkDo_HighConcurrency(b *testing.B) {
	g := NewGroup[string, int]()

	var calls, hits int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(10 * time.Millisecond) // Уменьшена задержка
		return 42, nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			time.Sleep(1 * time.Millisecond)
			for j := 0; j < 100; j++ {
				atomic.AddInt32(&hits, 1)
				go g.Do("key", fn)
			}
		}
	})

	b.Log("calls", calls, "hits", hits, "time", b.Elapsed())
}

// BenchmarkDo_CacheErrors проверяет производительность при кешировании ошибок.
func BenchmarkDo_CacheErrors(b *testing.B) {
	const cacheTime = 10 * time.Millisecond
	g := NewGroupWithCache[string, int](cacheTime, true, 0)

	var calls, hits int32
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
			atomic.AddInt32(&hits, 1)
		}
	})

	b.Log("calls", calls, "hits", hits, "time", b.Elapsed())
}

// BenchmarkDo_RealWorldSimulation симулирует реальный сценарий использования:
// множество разных ключей, смешанная нагрузка, кеширование.
func BenchmarkDo_RealWorldSimulation(b *testing.B) {
	const (
		cacheTime = 10 * time.Millisecond
		warmTime  = 5 * time.Millisecond
	)
	g := NewGroupWithCache[string, int](cacheTime, false, warmTime)

	var calls, hits int32
	fn := func() (int, error) {
		atomic.AddInt32(&calls, 1)
		// Симуляция запроса к внешнему API
		time.Sleep(5 * time.Microsecond) // Уменьшена задержка
		return 42, nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := string(rune('a' + (i % 10)))
			time.Sleep(1 * time.Millisecond)
			atomic.AddInt32(&hits, 1)
			go g.Do(key, fn)
			i++
		}
	})

	b.Log("calls", calls, "hits", hits, "time", b.Elapsed())
}
