package redisflight

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// redisAddr возвращает адрес Redis для тестов.
// Можно переопределить через переменную окружения REDIS_ADDR.
func redisAddr() string {
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		return v
	}
	return "127.0.0.1:6379"
}


// TestGroup_MultiProcess_UsesSingleComputation запускает несколько процессов,
// использующих один и тот же Redis и ключ, и проверяет, что реальное вычисление fn
// произошло ровно один раз (через счётчик в Redis).
func TestGroup_MultiProcess_UsesSingleComputation(t *testing.T) {
	const workers = 8

	client := newTestRedisClientV9(t)
	ctx := context.Background()

	// Уникальный префикс для ключей этого теста
	testID := fmt.Sprintf("mp:%d", time.Now().UnixNano())
	key := "key:" + testID
	counterKey := "counter:" + testID

	// На всякий случай очищаем счётчик
	if err := client.Del(ctx, counterKey).Err(); err != nil {
		t.Fatalf("failed to delete counter key: %v", err)
	}

	var cmds []*exec.Cmd

	for i := 0; i < workers; i++ {
		// Запускаем текущий тестовый бинарник как отдельный процесс,
		// который выполнит helper-тест TestGroup_MultiProcess_Helper.
		cmd := exec.Command(os.Args[0],
			"-test.run=TestGroup_MultiProcess_Helper",
			"--",
			key,
			counterKey,
		)

		// Наследуем окружение и добавляем маркер, что это helper-процесс.
		cmd.Env = append(os.Environ(),
			"GO_WANT_HELPER_PROCESS=1",
			"REDIS_ADDR="+redisAddr(),
		)

		cmds = append(cmds, cmd)
	}

	// Стартуем все процессы
	for _, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			t.Fatalf("failed to start worker: %v", err)
		}
	}

	// Ждём всех
	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("worker exited with error: %v", err)
		}
	}

	// Проверяем, сколько раз реально вызывался fn (через Redis-счётчик).
	n, err := client.Get(ctx, counterKey).Int()
	if err != nil {
		t.Fatalf("failed to get counter: %v", err)
	}

	if n != 1 {
		t.Fatalf("expected fn to be executed exactly once across processes, got %d", n)
	}
}

// TestGroup_MultiProcess_Helper — helper-тест, который реально выполняется в отдельных процессах.
// В обычном запуске "go test ./..." он сразу же выходит.
func TestGroup_MultiProcess_Helper(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	// Формат аргументов: ... -test.run=TestGroup_MultiProcess_Helper -- key counterKey
	args := os.Args
	sep := -1
	for i, a := range args {
		if a == "--" {
			sep = i
			break
		}
	}
	if sep == -1 || len(args) < sep+3 {
		fmt.Fprintln(os.Stderr, "helper: invalid args, want: -- key counterKey")
		os.Exit(2)
	}
	key := args[sep+1]
	counterKey := args[sep+2]

	client := goredis.NewClient(&goredis.Options{
		Addr: redisAddr(),
	})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "helper: redis ping failed: %v\n", err)
		os.Exit(1)
	}

	backend := NewGoRedisV9Backend(client)

	// Создаём Group с достаточно большим resultTTL, чтобы все воркеры
	// успели попасть в кешированный результат.
	g := NewGroup[int](backend,
		2*time.Second,       // lockTTL
		5*time.Second,       // resultTTL
		50*time.Millisecond, // pollInterval
		0,                   // warmupWindow
	)

	fn := func() (int, error) {
		// Имитация долгой работы, чтобы повысить шанс пересечения по времени
		time.Sleep(200 * time.Millisecond)

		// Считаем число реальных запусков через INCR в Redis.
		n, err := client.Incr(ctx, counterKey).Result()
		if err != nil {
			return 0, err
		}
		return int(n), nil
	}

	_, err := g.Do(key, fn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: Do failed: %v\n", err)
		os.Exit(1)
	}
}


