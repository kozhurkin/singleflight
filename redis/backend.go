package redisflight

import (
	"context"
	"time"
)

// Backend описывает абстрактный стор для блокировок и хранения результата.
// Конкретная реализация может быть на Redis, Valkey и т.п.
type Backend interface {
	// GetResult пытается получить закодированный результат по ключу.
	//   - data: сами байты результата (валидны только если found == true и err == nil),
	//   - found: признак, найден ли результат,
	//   - err: ошибка доступа к бекенду (отсутствие ключа не считается ошибкой).
	GetResult(ctx context.Context, resultKey string) (data []byte, found bool, err error)

	// GetResultWithTTL атомарно получает закодированный результат и оставшийся TTL ключа.
	//   - data: байты результата (валидны только если found == true и err == nil),
	//   - found: признак, существует ли ключ,
	//   - ttl: оставшееся время жизни; 0 если TTL не установлен или ключ не найден.
	GetResultWithTTL(ctx context.Context, resultKey string) (data []byte, found bool, ttl time.Duration, err error)

	// SetResult сохраняет результат по ключу с TTL.
	SetResult(ctx context.Context, resultKey string, data []byte, ttl time.Duration) error

	// TryLock пытается установить блокировку по ключу lockKey со значением lockValue и TTL.
	//   - ok == true, если блок установлен нами,
	//   - ok == false, если блок уже занят кем-то другим.
	TryLock(ctx context.Context, lockKey, lockValue string, ttl time.Duration) (ok bool, err error)

	// Unlock снимает блокировку, только если она принадлежит lockValue.
	// Возвращает, была ли реально снята блокировка.
	Unlock(ctx context.Context, lockKey, lockValue string) (bool, error)

	// UnlockAndSetResult атомарно:
	//   - проверяет, что блокировка по lockKey принадлежит lockValue,
	//   - снимает её,
	//   - записывает результат в resultKey с TTL.
	// Возвращает, была ли реально произведена операция.
	UnlockAndSetResult(
		ctx context.Context,
		lockKey, resultKey, lockValue string,
		data []byte,
		ttl time.Duration,
	) (bool, error)

	// TTL возвращает оставшийся TTL произвольного ключа.
	// Если ключ не существует или у него нет TTL, возвращает 0, nil.
	TTL(ctx context.Context, key string) (time.Duration, error)
}
