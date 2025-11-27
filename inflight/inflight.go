package inflight

import "sync"

// InFlight хранит состояние одного вычисления значения типа T.
// Позволяет дождаться результата через Wait и гарантирует единичный запуск fn.
type InFlight[T any] struct {
	done chan struct{}
	res  T
	err  error
	fn   func() (T, error)
	once sync.Once
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

// execute выполняет функцию fn и закрывает канал done.
func (f *InFlight[T]) execute() {
	f.res, f.err = f.fn()
	close(f.done)
}

// run выполняет fn ровно один раз.
// Если async == true, fn запускается в отдельной горутине, иначе выполняется синхронно.
// Для первого вызова возвращает true, для последующих — false.
func (f *InFlight[T]) run(async bool) bool {
	first := false
	f.once.Do(func() {
		first = true
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
func (f *InFlight[T]) Run() bool {
	return f.run(false)
}

// RunAsync запускает fn в отдельной горутине ровно один раз (не ждёт завершения).
// Для первого вызова возвращает true, для последующих — false.
func (f *InFlight[T]) RunAsync() bool {
	return f.run(true)
}

// Then создаёт новый InFlight[T], который будет выполнять функцию next после завершения текущего InFlight.
// Функция next получает результат текущего выполнения и возвращает новое значение типа T и ошибку.
// Если текущий InFlight завершился с ошибкой, next не вызывается, и новый InFlight сразу возвращает эту ошибку.
// Исходный InFlight автоматически запускается синхронно для начала выполнения цепочки.
func (f *InFlight[T]) Then(next func(T) (T, error)) *InFlight[T] {
	return ThenAny(f, next)
}

// ThenAny создаёт новый InFlight[R] из InFlight[T], выполняя next после завершения f.
// Это свободная функция (а не метод), потому что в Go методы не могут иметь собственные параметров типа.
func ThenAny[T, R any](f *InFlight[T], next func(T) (R, error)) *InFlight[R] {
	return NewInFlight(func() (R, error) {
		f.Run()
		res, err := f.Wait()
		if err != nil {
			var zero R
			return zero, err
		}
		return next(res)
	})
}

// Catch создаёт новый InFlight[T], который обрабатывает ошибку из текущего InFlight.
// Если текущий InFlight завершился без ошибки, результат просто прокидывается дальше.
// Если произошла ошибка, вызывается handler, который может вернуть восстановленное значение или
// другую ошибку.
func (f *InFlight[T]) Catch(handler func(error) (T, error)) *InFlight[T] {
	return NewInFlight(func() (T, error) {
		f.Run()
		res, err := f.Wait()
		if err != nil {
			return handler(err)
		}
		return res, nil
	})
}
