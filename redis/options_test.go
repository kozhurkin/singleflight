package redisflight

import (
	"testing"
	"time"

	"github.com/kozhurkin/singleflight"
	"github.com/stretchr/testify/require"
)

func TestWithPrefix_SetsPrefix(t *testing.T) {
	g := &Group[int]{}

	WithPrefix[int]("sf:")(g)

	require.Equal(t, "sf:", g.prefix, "expected prefix to be set by WithPrefix")
}

func TestWithLocalDeduplication_EnableCreatesLocalGroup(t *testing.T) {
	g := &Group[int]{}

	WithLocalDeduplication[int](true)(g)
	require.NotNil(t, g.localGroup, "expected localGroup to be initialized")

	// Повторное включение не должно пересоздавать группу.
	old := g.localGroup
	WithLocalDeduplication[int](true)(g)
	require.Same(t, old, g.localGroup, "expected localGroup to be reused on subsequent enable")
}

func TestWithLocalDeduplication_DisableClearsLocalGroup(t *testing.T) {
	g := &Group[int]{
		localGroup: singleflight.NewGroup[string, int](),
	}

	WithLocalDeduplication[int](false)(g)

	require.Nil(t, g.localGroup, "expected localGroup to be nil after disabling")
}

func TestWithWarmupWindow_SetsWarmupWindow(t *testing.T) {
	g := &Group[int]{}

	d := 500 * time.Millisecond
	WithWarmupWindow[int](d)(g)

	require.Equal(t, d, g.warmupWindow, "expected warmupWindow to be set")

	WithWarmupWindow[int](0)(g)
	require.Equal(t, time.Duration(0), g.warmupWindow,
		"expected warmupWindow to be 0 after disabling")
}
