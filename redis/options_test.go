package redisflight

import (
	"testing"
	"time"

	"github.com/kozhurkin/singleflight"
)

func TestWithPrefix_SetsPrefix(t *testing.T) {
	g := &Group[int]{}

	WithPrefix[int]("sf:")(g)

	if g.prefix != "sf:" {
		t.Fatalf("expected prefix %q, got %q", "sf:", g.prefix)
	}
}

func TestWithLocalDeduplication_EnableCreatesLocalGroup(t *testing.T) {
	g := &Group[int]{}

	WithLocalDeduplication[int](true)(g)
	if g.localGroup == nil {
		t.Fatalf("expected localGroup to be initialized")
	}

	// Повторное включение не должно пересоздавать группу.
	old := g.localGroup
	WithLocalDeduplication[int](true)(g)
	if g.localGroup != old {
		t.Fatalf("expected localGroup to be reused on subsequent enable")
	}
}

func TestWithLocalDeduplication_DisableClearsLocalGroup(t *testing.T) {
	g := &Group[int]{
		localGroup: singleflight.NewGroup[string, int](),
	}

	WithLocalDeduplication[int](false)(g)

	if g.localGroup != nil {
		t.Fatalf("expected localGroup to be nil after disabling, got non-nil")
	}
}

func TestWithWarmupWindow_SetsWarmupWindow(t *testing.T) {
	g := &Group[int]{}

	d := 500 * time.Millisecond
	WithWarmupWindow[int](d)(g)

	if g.warmupWindow != d {
		t.Fatalf("expected warmupWindow %v, got %v", d, g.warmupWindow)
	}

	WithWarmupWindow[int](0)(g)
	if g.warmupWindow != 0 {
		t.Fatalf("expected warmupWindow to be 0 after disabling, got %v", g.warmupWindow)
	}
}
