## redisflight (distributed singleflight backend)

`redisflight` — это пакет для использования `singleflight`‑подобной дедупликации **между процессами** поверх Redis.

- **Общий Backend-интерфейс**: пакет `redisflight` определяет интерфейс `Backend`, описывающий операции для:
  - чтения/записи закодированного результата (`GetResult`, `SetResult`),
  - распределённых блокировок (`TryLock`, `Unlock`, `UnlockAndSetResult`),
  - опционального атомарного чтения значения вместе с TTL (`GetResultWithTTL`).
- **Генерик‑группа `Group`**: реализует дедупликацию и кеширование результатов так же, как in‑memory `singleflight.Group`, но поверх любого `Backend`.
- **Адаптеры под go-redis**: в пакете есть готовые реализации `Backend` для клиентов:
  - `github.com/redis/go-redis/v9` (`NewGoRedisV9Backend`),
  - `github.com/go-redis/redis/v8` (`NewGoRedisV8Backend`).

### Пример: распределённый singleflight поверх Redis

```go
import (
    "time"

    goredis "github.com/redis/go-redis/v9"
    redisflight "github.com/kozhurkin/singleflight/redis"
)

func example() {
    // обычный клиент go-redis v9
    rdb := goredis.NewClient(&goredis.Options{
        Addr: "127.0.0.1:6379",
    })

    // адаптер к Backend-интерфейсу
    backend := redisflight.NewGoRedisV9Backend(rdb)

    // распределённая группа: lockTTL, resultTTL, pollInterval
    g := redisflight.NewGroup[int](
        backend,
        2*time.Second,       // lockTTL
        5*time.Second,       // resultTTL
        50*time.Millisecond, // pollInterval для ожидания результата
        // redisflight.WithWarmupWindow[int](500*time.Millisecond),
        // redisflight.WithPrefix[int]("sf:"),
        // redisflight.WithLocalDeduplication[int](true),
    )

    // Все процессы/инстансы, использующие один и тот же Redis и ключ,
    // будут делить между собой выполнение fn().
    res, err := g.Do("key", func() (int, error) {
        // дорогое вычисление, например HTTP-запрос к внешнему API
        return 42, nil
    })
    _ = res
    _ = err
}
```

### Когда использовать

Используйте `redisflight.Group`, если:

- у вас несколько инстансов сервиса (k8s, ECS, bare-metal) и
- вы хотите, чтобы **дорогой запрос выполнялся ровно один раз на кластер** по данному ключу,
  а все остальные конкурирующие запросы ждали и переиспользовали результат.


