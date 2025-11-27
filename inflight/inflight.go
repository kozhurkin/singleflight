package inflight

import (
	"sync/atomic"
)

// InFlight хранит состояние одного вычисления значения типа T.
// Позволяет дождаться результата через Wait и гарантирует единичный запуск fn.
type InFlight[T any] struct {
	done    chan struct{}
	res     T
	err     error
	fn      func() (T, error)
	started atomic.Bool
}

// NewInFlight создаёт новый InFlight для выполнения функции fn.
func NewInFlight[T any](fn func() (T, error)) *InFlight[T] {
	return &InFlight[T]{
		done: make(chan struct{}),
		fn:   fn,
	}
}

// Done возвращает канал, который закрывается по завершении fn.
func (f *InFlight[T]) Done() <-chan struct{} {
	return f.done
}

// Wait блокируется до завершения fn и возвращает результат и ошибку.
func (f *InFlight[T]) Wait() (T, error) {
	<-f.done
	return f.res, f.err
}

// run выполняет fn ровно один раз.
// Если async == true, fn запускается в отдельной горутине, иначе выполняется синхронно.
// Для первого вызова возвращает true, для последующих — false.
func (f *InFlight[T]) run(async bool) bool {
	// только первый, кто успешно CAS-нет, запускает fn
	if !f.started.CompareAndSwap(false, true) {
		return false
	}

	if async {
		go func() {
			f.res, f.err = f.fn()
			close(f.done)
		}()
	} else {
		f.res, f.err = f.fn()
		close(f.done)
	}

	return true
}

// Run выполняет fn ровно один раз синхронно (в той же горутине).
// Для первого вызова возвращает true, для последующих — false.
func (f *InFlight[T]) Run() bool {
	return f.run(false)
}

// RunAsync запускает fn в отдельной горутине ровно один раз (не ждёт завершения).
// Для первого вызова возвращает true, для последующих — false.
func (f *InFlight[T]) RunAsync() bool {
	return f.run(true)
}
