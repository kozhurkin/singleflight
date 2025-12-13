package singleflight

import "time"

// Single — обёртка для случая "один запрос", где ключ не нужен снаружи.
// Внутри использует Group c единым ключом типа struct{}.
type Single[V any] struct {
	g *Group[struct{}, V]
}

// NewSingle создаёт группу без кеша (только дедупликация одного запроса).
// Дополнительные параметры можно задать через опции, аналогичные Group:
//   - WithCache
//   - WithWarmupWindow
func NewSingle[V any](opts ...Option[struct{}, V]) *Single[V] {
	return &Single[V]{g: NewGroup(opts...)}
}

// NewSingleWithCache создаёт группу с кешированием одного запроса.
func NewSingleWithCache[V any](
	resultTTL time.Duration,
	errorTTL time.Duration,
	warmupWindow time.Duration,
) *Single[V] {
	return NewSingle(
		WithCache[struct{}, V](resultTTL, errorTTL),
		WithWarmupWindow[struct{}, V](warmupWindow),
	)
}

// Do выполняет (или переиспользует) вычисление значения для единственного ключа
// с учётом настроек кеша, скрывая key от вызывающего кода.
func (s *Single[V]) Do(fn func() (V, error)) (V, error) {
	return s.g.Do(struct{}{}, fn)
}
