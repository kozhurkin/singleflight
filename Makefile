MODULE := github.com/kozhurkin/singleflight

.PHONY: all test bench

all: test

## Запуск всех тестов с выводом подробной информации
test:
	go test ./... -v

## Полные бенчмарки
bench:
	go test -run=^$$ -bench=. -benchmem -count=5 ./...
