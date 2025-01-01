package main

import (
	"sync"
	"time"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
)

func main() {

	brokers := []string{"localhost:9092"}
	topic := "gap_demo"
	consumerGroup := "gap_consumer"
	clientId := "local_gap_demo_client"

	// use context for graceful shutdown

	wgPrd := &sync.WaitGroup{}
	wgPrd.Add(1)
	go func() {
		runGap(topic, consumerGroup, clientId, brokers)
		wgPrd.Done()
	}()

	wgConsumer := sync.WaitGroup{}
	wgConsumer.Add(1)
	go func() {
		runConsumers(topic, consumerGroup, clientId, brokers)
		wgConsumer.Done()
	}()

	wgLag := sync.WaitGroup{}
	wgLag.Add(1)
	go func() {
		printLag(topic, consumerGroup, clientId, brokers)
		wgLag.Done()
	}()

	wgPrd.Wait()
	wgConsumer.Wait()
	wgLag.Wait()
}

func runGap(topic, cg, clientId string, brokers []string) {
	//topic, group string, lagRefreshInterval int, mcfg MonitorConfig, prdCfg ProducerCfg) (gap *GroupAwareSyncProducer, err error) {
	gap, err := NewGroupAwareSyncProducer(topic, cg, 1, MonitorConfig{Brokers: brokers, ClientID: clientId}, ProducerCfg{ClientId: clientId, Brokers: brokers})
	if err != nil {
		log.Fatal(err)
	}
	// send 5 msgs every second
	values := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}
	for {
		select {
		case <-time.After(time.Second):
			var msgs []*sarama.ProducerMessage
			for _, v := range values {
				msg := sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.StringEncoder(v),
				}
				msgs = append(msgs, &msg)
			}
			err = gap.SendMessages(msgs)
			if err != nil {
				log.Error(err)
			}
		}
	}

}

func printLag(topic, cg, clientId string, brokers []string) {
	m, _ := NewMonitor(MonitorConfig{Brokers: brokers, ClientID: clientId})
	for {
		select {
		case <-time.After(time.Second * 5):
			_, _, perPart, err := m.GetConsumerGroupLag(cg, topic)
			if err != nil {
				log.Error(err)
				continue
			}
			for key, value := range perPart {
				log.Infof("%d : %d", key, value)
			}
		}
	}

}

func runConsumers(topic, cg, clientId string, brokers []string) {
	// start 5 consumer, each one consumes 1 msg in i seconds from loop
	wgC := sync.WaitGroup{}
	for iter := 0; iter < 5; iter++ {
		wgC.Add(1)
		go func() {
			RunConsumer(brokers, topic, cg, clientId, iter+1)
			wgC.Done()
		}()
	}
	wgC.Wait()

}
