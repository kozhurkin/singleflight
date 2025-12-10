## redisflight (distributed singleflight)

Это пакет для дедупликации и кеширования запросов **между процессами** поверх Redis.

Используйте, если:

- у вас **несколько инстансов сервиса** (k8s, ECS, bare-metal) и
- вы хотите, чтобы **дорогой запрос выполнялся ровно один раз на кластер** по данному ключу,
  а все остальные конкурирующие запросы (в том числе из других инстансов сервиса) ждали и переиспользовали результат.

### Схема работы

<img src="https://raw.githubusercontent.com/kozhurkin/singleflight/main/doc/distributed-timeline.png" width="780" />

### Пример использования

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

### Аргументы и опции `NewGroup`

```go
func NewGroup[V any](
    backend Backend,
    lockTTL time.Duration,
    resultTTL time.Duration,
    pollInterval time.Duration,
    opts ...Option[V],
) *Group[V]
```

- **`backend Backend`**  
  - Конкретная реализация интерфейса `Backend` (Redis, Valkey и т.п.).  
  - Определяет, **куда** сохраняются блокировки и результаты и **каким клиентом** выполняются операции.
  - В пакете есть готовые реализации `Backend` для клиентов:
    - `github.com/redis/go-redis/v9` (`NewGoRedisV9Backend`),
    - `github.com/go-redis/redis/v8` (`NewGoRedisV8Backend`),
    - `github.com/valkey-io/valkey-glide/go/v2` (`NewValkeyGlideBackend`).

- **`lockTTL time.Duration`**  
  - TTL Redis-ключа блокировки.  
  - Ограничивает, как долго один инстанс держит блокировку: если владелец "умер",
    другие инстансы смогут перехватить вычисление после истечения `lockTTL`.  

- **`resultTTL time.Duration`**  
  - TTL Redis-ключа с закэшированным результатом.  
  - Определяет, как долго результат считается "актуальным" и будет отдаваться из кеша без повторного вычисления.  

- **`pollInterval time.Duration`**  
  - Интервал, с которым конкурирующие запросы (не получившие блокировку) опрашивают Redis в ожидании результата.  
  - Чем меньше значение, тем быстрее появится результат, но тем выше нагрузка на Redis;
    чем больше — тем реже опрос, но выше латентность для ожидающих запросов.  

- **`opts ...Option[V]`** — дополнительные опции:
  - **`WithPrefix(prefix string)`**  
    - Добавляет префикс, например `"sf:"`, ко всем Redis-ключам, которые создаёт библиотека.  
    - Удобно для разделения кластера между приложениями / окружениями и избежания коллизий ключей.

  - **`WithLocalDeduplication(enabled bool)`**  
    - При `true` включает **локальную** дедупликацию внутри процесса через `singleflight.Group`.  
    - Снижает нагрузку на Redis за счёт того, что параллельные вызовы `Do` с одного инстанса по одному ключу выполняются один раз **локально**, без походов в Redis.

  - **`WithWarmupWindow(d time.Duration)`**  
    - Включает "окно прогрева" результата.  
    - В интервале `resultTTL` результат всегда просто берётся из кеша.  
    - После этого, в течение дополнительного окна прогрева `d`:
      1) первый запрос по ключу получает **старое** значение из кеша,  
      2) параллельно асинхронно запускается пересчёт и обновление результата в Redis.  
    - При `d == 0` прогрев отключён.
