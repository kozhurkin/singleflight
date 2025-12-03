package flight

import (
	"errors"
	"sync"
	"sync/atomic"
)

// ErrCanceled возвращается, если Flight был отменён до запуска fn.
var ErrCanceled = errors.New("flight: canceled")

// Flight хранит состояние одного вычисления значения типа T.
// Позволяет дождаться результата через Wait и гарантирует единичный запуск fn.
// Это минимальный concurrency-примитив без цепочек, метрик и дополнительных политик.
type Flight[T any] struct {
	done chan struct{}
	res  T
	err  error
	fn   func() (T, error)
	once sync.Once

	started  uint64
	canceled uint64
}

// NewFlight создаёт новый Flight для выполнения функции fn.
func NewFlight[T any](fn func() (T, error)) *Flight[T] {
	return &Flight[T]{
		done: make(chan struct{}),
		fn:   fn,
	}
}

// Done возвращает канал, который закрывается по завершении fn.
func (f *Flight[T]) Done() <-chan struct{} {
	return f.done
}

// Wait блокируется до завершения fn и возвращает результат и ошибку.
func (f *Flight[T]) Wait() (T, error) {
	<-f.done
	return f.res, f.err
}

// Cancel пытается отменить Flight до запуска fn.
// Если отмена удалась (fn ещё не запускался), возвращает true и устанавливает ошибку ErrCanceled.
// Если fn уже был запущен или завершён, возвращает false и ничего не меняет.
func (f *Flight[T]) Cancel() bool {
	canceled := false

	f.once.Do(func() {
		canceled = true
		atomic.StoreUint64(&f.canceled, 1)

		var zero T
		f.res = zero
		f.err = ErrCanceled
		close(f.done)
	})

	return canceled
}

// execute выполняет функцию fn и закрывает канал done.
func (f *Flight[T]) execute() {
	f.res, f.err = f.fn()
	close(f.done)
}

// run выполняет fn ровно один раз.
// Если async == true, fn запускается в отдельной горутине, иначе выполняется синхронно.
// Для первого вызова возвращает true, для последующих — false.
func (f *Flight[T]) run(async bool) bool {
	first := false
	f.once.Do(func() {
		first = true
		atomic.StoreUint64(&f.started, 1)
		if async {
			go f.execute()
		} else {
			f.execute()
		}
	})
	return first
}

// Run выполняет fn ровно один раз синхронно (в той же горутине).
// Для первого вызова возвращает true, для последующих — false.
func (f *Flight[T]) Run() bool {
	return f.run(false)
}

// RunAsync запускает fn в отдельной горутине ровно один раз (не ждёт завершения).
// Для первого вызова возвращает true, для последующих — false.
func (f *Flight[T]) RunAsync() bool {
	return f.run(true)
}

// Started возвращает true, если выполнение fn было запущено (синхронно или асинхронно).
// Безопасно для конкурентного использования.
func (f *Flight[T]) Started() bool {
	return atomic.LoadUint64(&f.started) == 1
}

// Canceled возвращает true, если Flight был отменён до запуска fn.
// Безопасно для конкурентного использования.
func (f *Flight[T]) Canceled() bool {
	return atomic.LoadUint64(&f.canceled) == 1
}
