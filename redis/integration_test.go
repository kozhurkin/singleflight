package redisflight

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// TestGroup_MultiProcess_UsesSingleComputation запускает несколько процессов,
// использующих один и тот же Redis и ключ, и проверяет, что реальное вычисление fn
// произошло ровно один раз (через Redis).
func TestGroup_MultiProcess_UsesSingleComputation(t *testing.T) {
	const (
		workers = 8

		lockTTL      = 2 * time.Second
		resultTTL    = 5 * time.Second
		pollInterval = 50 * time.Millisecond
	)

	client := newTestRedisClientV9(t)
	ctx := context.Background()

	// Уникальный префикс для ключей этого теста
	testID := fmt.Sprintf("mp:%d", time.Now().UnixNano())
	key := "key:" + testID
	timestampsKey := "timestamps:" + testID

	// На всякий случай очищаем счётчик
	if err := client.Del(ctx, timestampsKey).Err(); err != nil {
		t.Fatalf("failed to delete timestamps key: %v", err)
	}

	var cmds []*exec.Cmd

	for i := 0; i < workers; i++ {
		// Запускаем текущий тестовый бинарник как отдельный процесс,
		// который выполнит helper-тест TestGroup_MultiProcess_Helper.
		// Здесь каждый процесс делает ровно один запрос (count=1) без пауз (interval=0).
		cmd := exec.Command(os.Args[0],
			"-test.run=TestGroup_MultiProcess_Helper",
			"--",
		)

		// Наследуем окружение и добавляем маркеры/параметры для helper-процесса.
		cmd.Env = append(
			os.Environ(),
			"GO_WANT_HELPER_PROCESS=1",
			"REDIS_ADDR="+redisAddr(),
			"KEY_DO="+key,
			"KEY_HITS="+timestampsKey,
			"GROUP_LOCK_TTL="+lockTTL.String(),
			"GROUP_RESULT_TTL="+resultTTL.String(),
			"GROUP_POLL_INTERVAL="+pollInterval.String(),
			"CLIENT_COUNT=1",
			"CLIENT_INTERVAL=0s",
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

	// Проверяем, сколько раз реально вызывался fn по длине списка таймстемпов.
	timestamps, err := client.LRange(ctx, timestampsKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("failed to get timestamps: %v", err)
	}

	t.Logf("single-computation timestamps: %v", timestamps)

	if len(timestamps) != 1 {
		t.Fatalf("expected fn to be executed exactly once across processes, got %d", len(timestamps))
	}
}

// TestGroup_MultiProcess_RecomputesOnResultTTL запускает несколько процессов,
// которые в течение фиксированного времени (1 секунда) активно вызывают Do по одному
// и тому же ключу. При resultTTL=100ms мы ожидаем, что реальное вычисление fn (INCR
// счётчика в Redis) произойдёт примерно 10 раз — по одному разу на каждый интервал TTL.
func TestGroup_MultiProcess_RecomputesOnResultTTL(t *testing.T) {
	runMultiProcessRecomputesOnResultTTL(t, false)
}

// TestGroup_MultiProcess_RecomputesOnResultTTL_WithLocalDeduplication делает то же самое,
// что и TestGroup_MultiProcess_RecomputesOnResultTTL, но с включённой локальной дедупликацией
// (WithLocalDeduplication). Ожидаемый результат не должен меняться.
func TestGroup_MultiProcess_RecomputesOnResultTTL_WithLocalDeduplication(t *testing.T) {
	runMultiProcessRecomputesOnResultTTL(t, true)
}

// runMultiProcessRecomputesOnResultTTL содержит общую логику для интеграционных тестов выше.
func runMultiProcessRecomputesOnResultTTL(t *testing.T, enableLocalDedup bool) {
	const (
		lockTTL      = 50 * time.Millisecond
		resultTTL    = 200 * time.Millisecond
		pollInterval = 10 * time.Millisecond

		workers        = 4
		testDuration   = time.Second
		expectedResult = int(testDuration / resultTTL)

		interval = 50 * time.Millisecond
		count    = testDuration / interval
	)

	client := newTestRedisClientV9(t)
	ctx := context.Background()

	// Уникальный префикс для ключей этого теста
	testID := fmt.Sprintf("mp-ttl:%d", time.Now().UnixNano())
	key := "key:" + testID
	timestampsKey := "timestamps:" + testID

	// На всякий случай очищаем счётчик
	if err := client.Del(ctx, timestampsKey).Err(); err != nil {
		t.Fatalf("failed to delete timestamps key: %v", err)
	}

	var cmds []*exec.Cmd

	for i := 0; i < workers; i++ {
		// Запускаем текущий тестовый бинарник как отдельный процесс,
		// который выполнит helper-тест TestGroup_MultiProcess_Helper.
		// Здесь каждый процесс сделает count запросов с паузой interval между ними.
		cmd := exec.Command(os.Args[0],
			"-test.run=TestGroup_MultiProcess_Helper",
			"--",
		)

		// Наследуем окружение и добавляем маркеры/параметры для helper-процесса.
		env := []string{
			"GO_WANT_HELPER_PROCESS=1",
			"REDIS_ADDR=" + redisAddr(),
			"KEY_DO=" + key,
			"KEY_HITS=" + timestampsKey,
			"GROUP_LOCK_TTL=" + lockTTL.String(),
			"GROUP_RESULT_TTL=" + resultTTL.String(),
			"GROUP_POLL_INTERVAL=" + pollInterval.String(),
			"CLIENT_COUNT=" + fmt.Sprintf("%d", count),
			"CLIENT_INTERVAL=" + interval.String(),
		}
		if enableLocalDedup {
			env = append(env, "GROUP_LOCAL_DEDUP=true")
		}
		cmd.Env = append(os.Environ(), env...)
		cmds = append(cmds, cmd)
	}

	// Стартуем все процессы
	for _, cmd := range cmds {
		fmt.Println("starting worker", time.Now().Format(time.RFC3339Nano))
		if err := cmd.Start(); err != nil {
			t.Fatalf("failed to start worker: %v", err)
		}
	}

	// Ждём всех
	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("worker exited with error: %v", err)
		}
		fmt.Println("waiting for worker", time.Now().Format(time.RFC3339Nano))
	}

	// Проверяем, сколько раз реально вызывался fn по длине списка таймстемпов.
	timestamps, err := client.LRange(ctx, timestampsKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("failed to get timestamps: %v", err)
	}

	t.Logf("timestamps:\n%v+", strings.Join(timestamps, "\n"))

	if len(timestamps) != expectedResult {
		t.Fatalf("expected fn to be executed %d times across processes, got %d", expectedResult, len(timestamps))
	}
}

// TestGroup_MultiProcess_Helper — helper-тест, который реально выполняется
// в отдельных процессах. В обычном запуске "go test ./..." он сразу же выходит.
//
// Поведение задаётся через переменные окружения:
//   - KEY_DO              — ключ, по которому вызывается Group.Do;
//   - KEY_HITS            — ключ Redis-списка, куда пишутся таймстемпы реальных запусков fn;
//   - GROUP_LOCK_TTL      — TTL блокировки;
//   - GROUP_RESULT_TTL    — TTL результата;
//   - GROUP_POLL_INTERVAL — интервал опроса результата конкурентами;
//   - GROUP_LOCAL_DEDUP   — если непустой, включает локальную дедупликацию (WithLocalDeduplication);
//   - CLIENT_COUNT        — сколько раз вызвать Do;
//   - CLIENT_INTERVAL     — пауза между вызовами (Go duration, например "0s", "110ms").
func TestGroup_MultiProcess_Helper(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	// Читаем и валидируем параметры из окружения.
	mustEnv := func(name string) string {
		v := os.Getenv(name)
		if v == "" {
			fmt.Fprintf(os.Stderr, "helper: missing env %s\n", name)
			os.Exit(2)
		}
		return v
	}

	key := mustEnv("KEY_DO")
	timestampsKey := mustEnv("KEY_HITS")

	count, err := strconv.Atoi(mustEnv("CLIENT_COUNT"))
	if err != nil || count < 1 {
		fmt.Fprintf(os.Stderr, "helper: invalid CLIENT_COUNT: %v\n", err)
		os.Exit(2)
	}

	interval, err := time.ParseDuration(mustEnv("CLIENT_INTERVAL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: invalid CLIENT_INTERVAL: %v\n", err)
		os.Exit(2)
	}

	lockTTL, err := time.ParseDuration(mustEnv("GROUP_LOCK_TTL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: invalid GROUP_LOCK_TTL: %v\n", err)
		os.Exit(2)
	}
	resultTTL, err := time.ParseDuration(mustEnv("GROUP_RESULT_TTL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: invalid GROUP_RESULT_TTL: %v\n", err)
		os.Exit(2)
	}
	pollInterval, err := time.ParseDuration(mustEnv("GROUP_POLL_INTERVAL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: invalid GROUP_POLL_INTERVAL: %v\n", err)
		os.Exit(2)
	}
	enableLocalDedup := os.Getenv("GROUP_LOCAL_DEDUP") != ""

	client := goredis.NewClient(&goredis.Options{
		Addr: redisAddr(),
	})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "helper: redis ping failed: %v\n", err)
		os.Exit(1)
	}

	backend := NewGoRedisV9Backend(client)

	opts := []Option[int]{}
	if enableLocalDedup {
		opts = append(opts, WithLocalDeduplication[int](true))
	}

	g := NewGroup(backend, lockTTL, resultTTL, pollInterval, opts...)

	fn := func() (int, error) {
		// Пишем таймстемп реального запуска в список.
		now := time.Now().Format(time.RFC3339Nano)
		if err := client.RPush(ctx, timestampsKey, now).Err(); err != nil {
			return 0, err
		}
		// Значение не важно для тестов, возвращаем фиктивное.
		return 0, nil
	}

	for i := 0; i < count; i++ {
		if _, err := g.Do(key, fn); err != nil {
			fmt.Fprintf(os.Stderr, "helper: Do failed: %v\n", err)
			os.Exit(1)
		}
		if i+1 < count {
			time.Sleep(interval)
		}
	}
}
