package flight_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kozhurkin/singleflight/flight"
)

// Эти тесты покрывают минимальный примитив Flight (без Flow-надстройки).

func TestFlight_CoreRunOnce(t *testing.T) {
	var calls int32

	f := flight.NewFlight(func() (int, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(5 * time.Millisecond)
		return 42, nil
	})

	// Последовательные вызовы: первый должен вернуть true, последующие — false.
	ok := f.Run()
	require.True(t, ok, "first Run() should return true")

	ok = f.Run()
	require.False(t, ok, "second Run() should return false")

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.Run()
		}()
	}
	wg.Wait()

	got := atomic.LoadInt32(&calls)
	require.Equal(t, int32(1), got, "fn should be called exactly once")
}

func TestFlight_CoreRunAsyncOnce(t *testing.T) {
	var calls int32

	f := flight.NewFlight(func() (int, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(5 * time.Millisecond)
		return 7, nil
	})

	const goroutines = 20
	var wg sync.WaitGroup
	var trueCount int32

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if f.RunAsync() {
				atomic.AddInt32(&trueCount, 1)
			}
		}()
	}

	wg.Wait()
	<-f.Done()

	require.Equal(t, int32(1), atomic.LoadInt32(&calls), "fn should be called exactly once with RunAsync")
	require.Equal(t, int32(1), atomic.LoadInt32(&trueCount), "exactly one RunAsync call should return true")
}

func TestFlight_CoreWaitAndDone(t *testing.T) {
	f := flight.NewFlight(func() (int, error) {
		time.Sleep(5 * time.Millisecond)
		return 13, nil
	})

	// До запуска Run канал не должен быть закрыт.
	select {
	case <-f.Done():
		require.FailNow(t, "Done closed before Run")
	default:
	}

	go f.Run()

	v, err := f.Wait()
	require.NoError(t, err, "Wait() should not return error")
	require.Equal(t, 13, v, "Wait() should return fn result")
}

func TestFlight_CoreCancelBeforeRun(t *testing.T) {
	var calls int32

	f := flight.NewFlight(func() (int, error) {
		atomic.AddInt32(&calls, 1)
		return 123, nil
	})

	ok := f.Cancel()
	require.True(t, ok, "first Cancel() before Run should return true")

	// Повторный Cancel должен вернуть false.
	ok = f.Cancel()
	require.False(t, ok, "second Cancel() should return false")

	// fn не должен был вызываться.
	require.Equal(t, int32(0), atomic.LoadInt32(&calls), "fn must not be called if Flight was canceled before Run")

	// Канал Done должен быть закрыт.
	select {
	case <-f.Done():
		// ok
	default:
		require.FailNow(t, "Done should be closed after Cancel")
	}
}
