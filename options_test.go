package singleflight

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithCache_SetsTTLs(t *testing.T) {
	g := &Group[string, int]{}

	const (
		resultTTL = 5 * time.Second
		errorTTL  = 2 * time.Second
	)

	WithCache[string, int](resultTTL, errorTTL)(g)

	require.Equal(t, resultTTL, g.resultTTL, "expected resultTTL to be set by WithCache")
	require.Equal(t, errorTTL, g.errorTTL, "expected errorTTL to be set by WithCache")

	// Повторный вызов должен перезаписать значения.
	const (
		resultTTL2 = 10 * time.Second
		errorTTL2  = 3 * time.Second
	)
	WithCache[string, int](resultTTL2, errorTTL2)(g)

	require.Equal(t, resultTTL2, g.resultTTL, "expected resultTTL to be overwritten by second WithCache")
	require.Equal(t, errorTTL2, g.errorTTL, "expected errorTTL to be overwritten by second WithCache")
}

func TestWithWarmupWindow_InitializesAndClearsWarmings(t *testing.T) {
	g := &Group[string, int]{}

	d := 500 * time.Millisecond
	WithWarmupWindow[string, int](d)(g)

	require.Equal(t, d, g.warmupWindow, "expected warmupWindow to be set by WithWarmupWindow")
	require.NotNil(t, g.warmings, "expected warmings map to be initialized when warmupWindow > 0")

	// Выключаем прогрев.
	WithWarmupWindow[string, int](0)(g)

	require.Equal(t, time.Duration(0), g.warmupWindow, "expected warmupWindow to be 0 after disabling")
	require.Nil(t, g.warmings, "expected warmings map to be cleared when warmupWindow == 0")
}
