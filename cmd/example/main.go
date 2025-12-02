package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/kozhurkin/singleflight"
)

type Weather struct {
	CurrentWeather struct {
		Temperature float64 `json:"temperature"`
		Windspeed   float64 `json:"windspeed"`
	} `json:"current_weather"`
}

// простая карта "город -> координаты" для примера
var cityCoords = map[string]struct {
	Lat float64
	Lon float64
}{
	"spb":    {Lat: 59.93, Lon: 30.31},
	"moscow": {Lat: 55.75, Lon: 37.62},
	"london": {Lat: 51.51, Lon: -0.13},
}

func weatherURLForCity(city string) (string, error) {
	coords, ok := cityCoords[city]
	if !ok {
		return "", fmt.Errorf("unknown city %q", city)
	}
	return fmt.Sprintf(
		"https://api.open-meteo.com/v1/forecast?latitude=%.4f&longitude=%.4f&current_weather=true&city=%s",
		coords.Lat, coords.Lon, city,
	), nil
}

func fetchWeather(city string) (Weather, error) {
	u, err := weatherURLForCity(city)
	if err != nil {
		return Weather{}, err
	}

	start := time.Now()
	log.Printf("[WEATHER] making request for city=%s url=%s", city, u)

	var w Weather

	resp, err := http.Get(u)
	if err != nil {
		log.Printf("[WEATHER] error: %v", err)
		return w, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return w, err
	}

	log.Printf("[WEATHER] status=%d duration=%s", resp.StatusCode, time.Since(start))

	if err := json.Unmarshal(body, &w); err != nil {
		return w, fmt.Errorf("json decode error: %w", err)
	}

	log.Printf("[WEATHER] parsed temperature = %.2f°C", w.CurrentWeather.Temperature)

	return w, nil
}

func main() {
	// Добавляем миллисекунды (и микросекунды) в таймстемпы логов
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Кеш: TTL 5 секунд, ошибки не кешируем, время прогрева 2 секунды
	cache := singleflight.NewGroupWithCache[string, Weather](5*time.Second, 0, 2*time.Second)

	mux := http.NewServeMux()

	// HTTP-эндпоинт /weather?city=...
	mux.HandleFunc("/weather", func(w http.ResponseWriter, r *http.Request) {
		city := r.URL.Query().Get("city")
		if city == "" {
			http.Error(w, "missing city", http.StatusBadRequest)
			return
		}

		// Используем singleflight.Group: ключом будет город
		weather, err := cache.Do(city, func() (Weather, error) {
			return fetchWeather(city)
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("fetch error: %v", err), http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(weather); err != nil {
			log.Printf("[SERVER] encode error: %v", err)
		}
	})

	// Создаём HTTP сервер для возможности graceful shutdown
	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// WaitGroup для отслеживания всех запросов-клиентов
	var wg sync.WaitGroup

	// Клиенты, которые параллельно шлют запросы по разным городам
	go func() {
		client := &http.Client{
			Timeout: 3 * time.Second,
		}
		cities := []string{"spb", "spb", "moscow", "moscow", "london", "spb", "kiev"}

		for round := 0; round < 5; round++ {
			round := round
			for _, city := range cities {
				city := city
				wg.Add(1)
				go func() {
					defer wg.Done()
					u := "http://localhost:8080/weather?city=" + url.QueryEscape(city)
					resp, err := client.Get(u)
					if err != nil {
						log.Printf("[CLIENT] round=%d city=%s error=%v", round, city, err)
						return
					}
					defer resp.Body.Close()

					if resp.StatusCode != http.StatusOK {
						body, _ := io.ReadAll(resp.Body)
						log.Printf("[CLIENT] round=%d city=%s status=%d body=%q", round, city, resp.StatusCode, string(body))
						return
					}

					var w Weather
					if err := json.NewDecoder(resp.Body).Decode(&w); err != nil {
						log.Printf("[CLIENT] round=%d city=%s status=%d decode_error=%v", round, city, resp.StatusCode, err)
						return
					}

					log.Printf("[CLIENT] round=%d city=%s status=%d temp=%.2f°C", round, city, resp.StatusCode, w.CurrentWeather.Temperature)
				}()
				time.Sleep(100 * time.Millisecond)
			}
			time.Sleep(1 * time.Second)
		}
		wg.Wait()
		log.Println("[SERVER] all requests completed")
	}()

	log.Println("starting server on :8080")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	log.Println("[SERVER] server stopped")
}
