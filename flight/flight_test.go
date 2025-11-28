package flight_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kozhurkin/singleflight/flight"
)

func TestFlight_RunOnce(t *testing.T) {
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
		go func(i int) {
			defer wg.Done()
			f.Run()
		}(i)
	}
	wg.Wait()

	got := atomic.LoadInt32(&calls)
	require.Equal(t, int32(1), got, "fn should be called exactly once")
}

func TestFlight_WaitAndHits(t *testing.T) {
	f := flight.NewFlight(func() (int, error) {
		return 42, nil
	})

	f.Run()

	v1, err1 := f.Wait()
	v2, err2 := f.Wait()

	require.NoError(t, err1, "first Wait() should not return error")
	require.NoError(t, err2, "second Wait() should not return error")
	require.Equal(t, 42, v1, "first Wait() should return 42")
	require.Equal(t, 42, v2, "second Wait() should return 42")
	require.Equal(t, int64(2), f.Hits(), "Hits() should equal number of Wait calls")
}

func TestFlight_DoneChannel(t *testing.T) {
	f := flight.NewFlight(func() (int, error) {
		time.Sleep(5 * time.Millisecond)
		return 1, nil
	})

	// До запуска Run канал не должен быть закрыт.
	select {
	case <-f.Done():
		require.FailNow(t, "Done closed before Run")
	default:
	}

	done := f.Done()

	// Повторный неблокирующий select по сохранённому каналу Done:
	// до запуска Run он также не должен быть закрыт.
	select {
	case <-done:
		require.FailNow(t, "Done closed before Run on saved channel")
	default:
	}

	go f.Run()

	select {
	case <-done:
		// ok
	case <-time.After(10 * time.Millisecond):
		require.FailNow(t, "timeout waiting for Done to be closed")
	}
}

func TestFlight_Wait_ConcurrentHits(t *testing.T) {
	f := flight.NewFlight(func() (int, error) {
		time.Sleep(5 * time.Millisecond)
		return 7, nil
	})

	f.RunAsync()

	// Ждём завершения вычисления.
	<-f.Done()

	const n = 10
	var wg sync.WaitGroup
	results := make([]int, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := f.Wait()
			require.NoError(t, err, "Wait() should not return error")
			results[i] = v
		}(i)
	}

	wg.Wait()

	for i, v := range results {
		require.Equalf(t, 7, v, "results[%d] should be 7", i)
	}
	require.Equal(t, int64(n), f.Hits(), "Hits() should equal number of Wait calls")
}

func TestFlight_Then_Success(t *testing.T) {
	base := flight.NewFlight(func() (int, error) {
		time.Sleep(10 * time.Millisecond)
		return 2, nil
	})

	next := base.Then(func(v int) (int, error) {
		return v * 10, nil
	})

	next.Run()
	res, err := next.Wait()
	require.NoError(t, err, "Then() chain should not return error")
	require.Equal(t, 20, res, "Then() should transform 2 into 20")
}

func TestFlight_ThenAny_Success(t *testing.T) {
	base := flight.NewFlight(func() (int, error) {
		return 5, nil
	})

	next := flight.ThenAny(base, func(v int) (string, error) {
		return fmt.Sprintf("%d", v*2), nil
	})

	next.Run()
	res, err := next.Wait()
	require.NoError(t, err, "ThenAny() chain should not return error")
	require.Equal(t, "10", res, "ThenAny() should transform 5 into \"10\"")
}

func TestFlight_Then_ErrorPropagation(t *testing.T) {
	someErr := errors.New("boom")
	base := flight.NewFlight(func() (int, error) {
		return 0, someErr
	})

	var called int32
	next := base.Then(func(v int) (int, error) {
		// не должен вызываться
		atomic.AddInt32(&called, 1)
		return v + 1, nil
	})

	next.Run()
	_, err := next.Wait()
	require.ErrorIs(t, err, someErr, "Then() should propagate base error")
	require.Equal(t, int32(0), atomic.LoadInt32(&called), "next should not be called on error")
}

func TestFlight_ThenAny_ErrorPropagation(t *testing.T) {
	someErr := errors.New("boom")
	base := flight.NewFlight(func() (int, error) {
		return 0, someErr
	})

	var called int32
	next := flight.ThenAny(base, func(v int) (string, error) {
		atomic.AddInt32(&called, 1)
		return fmt.Sprintf("%d", v), nil
	})

	next.Run()
	_, err := next.Wait()
	require.ErrorIs(t, err, someErr, "ThenAny() should propagate base error")
	require.Equal(t, int32(0), atomic.LoadInt32(&called), "next should not be called on error")
}

func TestFlight_Catch(t *testing.T) {
	someErr := errors.New("boom")
	base := flight.NewFlight(func() (int, error) {
		return 0, someErr
	})

	recovered := base.Catch(func(err error) (int, error) {
		if !errors.Is(err, someErr) {
			return 0, err
		}
		return 123, nil
	})

	recovered.Run()
	res, err := recovered.Wait()
	require.NoError(t, err, "Catch() should recover from error")
	require.Equal(t, 123, res, "Catch() should return recovered value")
}

func TestFlight_Catch_NoErrorPassthrough(t *testing.T) {
	const value = 7

	base := flight.NewFlight(func() (int, error) {
		return value, nil
	})

	var called int32
	recovered := base.Catch(func(err error) (int, error) {
		atomic.AddInt32(&called, 1)
		return 0, fmt.Errorf("unexpected handler call: %v", err)
	})

	recovered.Run()
	res, err := recovered.Wait()
	require.NoError(t, err, "Catch() should pass through successful result")
	require.Equal(t, value, res, "Catch() should keep original value when there is no error")
	require.Equal(t, int32(0), atomic.LoadInt32(&called), "handler must not be called on success")
}
