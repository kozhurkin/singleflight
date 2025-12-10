package redisflight

import (
	"time"

	"github.com/kozhurkin/singleflight"
)

// Option задаёт дополнительные параметры для Group.
type Option[V any] func(*Group[V])

// WithPrefix настраивает префикс для lock/result ключей (например, "sf:").
func WithPrefix[V any](prefix string) Option[V] {
	return func(g *Group[V]) {
		g.prefix = prefix
	}
}

// WithLocalDeduplication включает или отключает локальную дедупликацию
// внутри процесса с помощью singleflight.Group. При enabled == true
// создаётся внутренняя localGroup; при false — остаётся nil.
func WithLocalDeduplication[V any](enabled bool) Option[V] {
	return func(g *Group[V]) {
		if !enabled {
			g.localGroup = nil
			return
		}
		if g.localGroup == nil {
			g.localGroup = singleflight.NewGroup[string, V]()
		}
	}
}

// WithWarmupWindow настраивает окно прогрева результата:
// после истечения resultTTL ключ продолжает обслуживаться из кеша
// ещё warmupWindow, в течение которого первый запрос может асинхронно
// "прогреть" значение. При warmupWindow == 0 прогрев отключён.
func WithWarmupWindow[V any](d time.Duration) Option[V] {
	return func(g *Group[V]) {
		g.warmupWindow = d
	}
}