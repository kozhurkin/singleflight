package singleflight

import "time"

// SingleGroup — обёртка для случая "один запрос", где ключ не нужен снаружи.
// Внутри использует Group c единым ключом типа struct{}.
type SingleGroup[V any] struct {
	g *Group[struct{}, V]
}

// NewSingleGroup создаёт группу без кеша (только дедупликация одного запроса).
func NewSingleGroup[V any]() *SingleGroup[V] {
	return &SingleGroup[V]{g: NewGroup[struct{}, V]()}
}

// NewSingleGroupWithCache создаёт группу с кешированием одного запроса.
func NewSingleGroupWithCache[V any](
	resultTTL time.Duration,
	errorTTL time.Duration,
	warmupWindow time.Duration,
) *SingleGroup[V] {
	return &SingleGroup[V]{
		g: NewGroupWithCache[struct{}, V](resultTTL, errorTTL, warmupWindow),
	}
}

// Do выполняет (или переиспользует) вычисление значения для единственного ключа
// с учётом настроек кеша, скрывая key от вызывающего кода.
func (sg *SingleGroup[V]) Do(fn func() (V, error)) (V, error) {
	return sg.g.Do(struct{}{}, fn)
}
