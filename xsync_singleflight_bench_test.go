package singleflight_test

import (
	"sync/atomic"
	"testing"
	"time"

	xsingleflight "golang.org/x/sync/singleflight"
)

// BenchmarkDo_HighConcurrency бенчмарк для golang.org/x/sync/singleflight.Group.
// Сценарий максимально повторяет BenchmarkDo_HighConcurrency из этого репозитория:
// много горутин конкурентно вызывают Do() с одним и тем же ключом.
func BenchmarkDo_HighConcurrency(b *testing.B) {
	var g xsingleflight.Group

	var calls, hits int32
	fn := func() (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(10 * time.Millisecond)
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
