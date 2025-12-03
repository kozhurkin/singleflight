package flight

import "sync/atomic"

// FlightFlow — надстройка над Flight, добавляющая удобные методы композиции,
// обработку ошибок, метрики и дополнительную семантику использования результата.
// Она не меняет базовый примитив Flight, а лишь управляет его жизненным циклом.
type FlightFlow[T any] struct {
	base *Flight[T]

	hits     uint64
	started  uint64
	canceled uint64
}

// NewFlightFlow создаёт новый FlightFlow с внутренним Flight.
func NewFlightFlow[T any](fn func() (T, error)) *FlightFlow[T] {
	return &FlightFlow[T]{base: NewFlight(fn)}
}

// FromFlight оборачивает существующий Flight в FlightFlow.
// Предполагается, что этот Flight больше не используется напрямую.
func FromFlight[T any](f *Flight[T]) *FlightFlow[T] {
	return &FlightFlow[T]{base: f}
}

// Done возвращает канал завершения базового Flight.
func (ff *FlightFlow[T]) Done() <-chan struct{} {
	return ff.base.Done()
}

// Wait блокируется до завершения вычисления и возвращает результат и ошибку.
// Счётчик Hits увеличивается на 1 для каждого вызова Wait.
func (ff *FlightFlow[T]) Wait() (T, error) {
	res, err := ff.base.Wait()
	atomic.AddUint64(&ff.hits, 1)
	return res, err
}

// OnDone блокируется до завершения базового Flight,
// а затем синхронно вызывает fn с результатом и ошибкой.
func (ff *FlightFlow[T]) OnDone(fn func(res T, err error)) {
	res, err := ff.base.Wait()
	fn(res, err)
}

// Run выполняет базовый Flight синхронно ровно один раз.
// Для первого вызова возвращает true, для последующих — false.
func (ff *FlightFlow[T]) Run() bool {
	ok := ff.base.Run()
	if ok {
		atomic.StoreUint64(&ff.started, 1)
	}
	return ok
}

// RunAsync запускает базовый Flight в отдельной горутине ровно один раз.
// Для первого вызова возвращает true, для последующих — false.
func (ff *FlightFlow[T]) RunAsync() bool {
	ok := ff.base.RunAsync()
	if ok {
		atomic.StoreUint64(&ff.started, 1)
	}
	return ok
}

// Cancel пытается отменить базовый Flight до его запуска.
// Успешная отмена помечает FlightFlow как canceled.
func (ff *FlightFlow[T]) Cancel() bool {
	ok := ff.base.Cancel()
	if ok {
		atomic.StoreUint64(&ff.canceled, 1)
	}
	return ok
}

// Hits возвращает количество обращений к результату через Wait.
func (ff *FlightFlow[T]) Hits() int64 {
	return int64(atomic.LoadUint64(&ff.hits))
}

// Started возвращает true, если Run/RunAsync в этом Flow успешно запустили базовый Flight.
func (ff *FlightFlow[T]) Started() bool {
	return atomic.LoadUint64(&ff.started) == 1
}

// Canceled возвращает true, если Cancel в этом Flow успешно отменил базовый Flight до запуска.
func (ff *FlightFlow[T]) Canceled() bool {
	return atomic.LoadUint64(&ff.canceled) == 1
}

// Then создаёт новый FlightFlow[T], который будет выполнять функцию fn
// после завершения текущего FlightFlow.
// Если текущий Flow завершился с ошибкой, fn не вызывается, и новый Flow возвращает эту ошибку.
func (ff *FlightFlow[T]) Then(fn func(T) (T, error)) *FlightFlow[T] {
	return ThenAny(ff, fn)
}

// ThenAny создаёт новый FlightFlow[R] из FlightFlow[T], выполняя fn после завершения ff.
// Это свободная функция (а не метод), потому что в Go методы не могут иметь собственные параметров типа.
func ThenAny[T, R any](ff *FlightFlow[T], fn func(T) (R, error)) *FlightFlow[R] {
	return NewFlightFlow(func() (R, error) {
		ff.Run()
		res, err := ff.Wait()
		if err != nil {
			var zero R
			return zero, err
		}
		return fn(res)
	})
}

// Catch создаёт новый FlightFlow[T], который обрабатывает ошибку из текущего FlightFlow.
// Если текущий FlightFlow завершился без ошибки, результат просто прокидывается дальше.
// Если произошла ошибка, вызывается handler, который может вернуть восстановленное значение или
// другую ошибку.
func (ff *FlightFlow[T]) Catch(handler func(error) (T, error)) *FlightFlow[T] {
	return NewFlightFlow(func() (T, error) {
		ff.Run()
		res, err := ff.Wait()
		if err != nil {
			return handler(err)
		}
		return res, nil
	})
}

// Handle создаёт новый FlightFlow[T], который всегда вызывает fn с результатом и ошибкой
// исходного FlightFlow и возвращает то, что вернёт fn.
// В отличие от Then и Catch, fn получает и res и err одновременно и сам решает,
// как их интерпретировать.
func (ff *FlightFlow[T]) Handle(fn func(res T, err error) (T, error)) *FlightFlow[T] {
	return HandleAny(ff, fn)
}

// HandleAny создаёт новый FlightFlow[R] из FlightFlow[T], вызывая fn с результатом и ошибкой
// исходного FlightFlow и возвращая то, что вернёт fn.
// Это свободная функция (а не метод), потому что в Go методы не могут иметь собственные параметров типа.
func HandleAny[T, R any](ff *FlightFlow[T], fn func(res T, err error) (R, error)) *FlightFlow[R] {
	return NewFlightFlow(func() (R, error) {
		ff.Run()
		res, err := ff.Wait()
		return fn(res, err)
	})
}


