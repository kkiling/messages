package bench

import (
	"context"
	"encoding/json"
	"github.com/kkiling/messages/producer"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/kkiling/messages/producer/kafka_producer"
)

type TestData struct {
	Id        int64  `json:"id"`
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`
	Text      string `json:"text"`
	Valid     bool   `json:"valid"`
}

func BenchmarkProducer(b *testing.B) {
	const topic = "kafka.test.benching"
	cfg := kafka_producer.Config{
		BootstrapServers: []string{"localhost:9092", "localhost:9093"},
	}
	var prod producer.IProducer = kafka_producer.NewProducer(cfg)

	testDataChan := make(chan TestData)
	ctx := context.Background()
	wg := &sync.WaitGroup{}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case data, ok := <-testDataChan:
					if !ok {
						return
					}
					jsonData, _ := json.Marshal(data)
					msg := producer.Message{
						Key:     []byte(strconv.FormatInt(data.Id%5, 10)),
						Value:   jsonData,
						Headers: nil,
					}
					if err := prod.ProduceMsg(ctx, topic, msg); err != nil {
						panic(err)
					}
				}
			}
		}()
	}
	b.ResetTimer() // сбросим таймер, чтобы не учитывать время на подготовку
	for i := 0; i < b.N; i++ {
		testDataChan <- TestData{
			Id:        int64(i),
			Type:      "test_producer",
			Timestamp: time.Now().Unix(),
			Text: `В своём стремлении повысить качество жизни, они забывают, что новая модель 
			организационной деятельности прекрасно подходит для реализации вывода текущих активов.`,
			Valid: true,
		}
	}
	close(testDataChan)
	wg.Wait()
}
