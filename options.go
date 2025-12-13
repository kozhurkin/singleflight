package singleflight

import (
	"time"

	"github.com/kozhurkin/singleflight/flight"
)

// Option задаёт дополнительные параметры для Group.
// Аналогично redisflight.Option, но параметризован по ключу и значению.
type Option[K comparable, V any] func(*Group[K, V])

// WithCache настраивает TTL кеша успешных результатов и ошибок.
// errorTTL == 0 означает, что ошибки не кешируются.
func WithCache[K comparable, V any](resultTTL, errorTTL time.Duration) Option[K, V] {
	return func(g *Group[K, V]) {
		g.resultTTL = resultTTL
		g.errorTTL = errorTTL
	}
}

// WithWarmupWindow настраивает окно прогрева результата.
// При warmupWindow == 0 прогрев отключён.
func WithWarmupWindow[K comparable, V any](warmupWindow time.Duration) Option[K, V] {
	return func(g *Group[K, V]) {
		g.warmupWindow = warmupWindow

		// Для прогрева нужна карта warmings. Инициализируем её лениво.
		if warmupWindow > 0 && g.warmings == nil {
			g.warmings = make(map[K]*flight.FlightFlow[V])
		}

		// При выключении прогрева можно очистить warmings,
		// чтобы не держать лишние ссылки.
		if warmupWindow == 0 {
			g.warmings = nil
		}
	}
}
