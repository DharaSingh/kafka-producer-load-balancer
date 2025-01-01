package main

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/pkg/errors"
)

type ProducerCfg struct {
	Brokers  []string
	ClientId string
}

/*
GroupAwareSyncProducer to use this there are following components -

	consumer side config - group and topic: to get the lag for each partition to determine best partition to push the kafka msg
	producer - a producer to send msgs to kafka using manual partitioner technique
	monitor config - to create a monitor instance to monitor lag for consumer-topic combination
	fallback to round-robin if anything goes wrong, do not block the messages
*/
type GroupAwareSyncProducer struct {
	producer sarama.SyncProducer
	cache    *LagCache
}

/*
NewGroupAwareSyncProducer configs required to monitor lag, and create a producer
*/
func NewGroupAwareSyncProducer(topic, group string, lagRefreshInterval int, mcfg MonitorConfig, prdCfg ProducerCfg) (gap *GroupAwareSyncProducer, err error) {
	if topic == "" || group == "" {
		return nil, errors.New("topic or group can't be empty")
	}
	// if not defined, default 60 seconds
	if lagRefreshInterval <= 0 {
		lagRefreshInterval = 60
	}

	gap = &GroupAwareSyncProducer{}
	gap.producer, err = initSyncProducer(prdCfg.ClientId, prdCfg.Brokers)
	if err != nil {
		return
	}

	lc, err := NewLagCache(group, topic, mcfg, time.Duration(lagRefreshInterval)*time.Second)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create lag cache for topic")
	}
	gap.cache = lc

	return gap, nil
}

func initSyncProducer(clientId string, brokers []string) (producer sarama.SyncProducer, err error) {
	config := sarama.NewConfig()
	config.ClientID = clientId
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner // has to be manual only to assign partition by us
	producer, err = sarama.NewSyncProducer(brokers, config)

	return

}

// SendMessages - send message will check the lag for given consumer group and topic and will assign the partition with least msgs
// this will be a slow produce, should be used only where processing time is more per msg in consumer side
// to keep optimal performance in all instances just use this producer
func (gap *GroupAwareSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) (err error) {
	for _, m := range msgs {
		m.Partition = gap.cache.GetPartitionWithMinLag()
	}
	return gap.producer.SendMessages(msgs)
}

// SendMessage - same as above for one msg
func (gap *GroupAwareSyncProducer) SendMessage(msg *sarama.ProducerMessage) (err error) {
	msg.Partition = gap.cache.GetPartitionWithMinLag()
	_, _, err = gap.producer.SendMessage(msg)
	return
}

func (gap *GroupAwareSyncProducer) Close() (err error) {
	err = gap.producer.Close()
	gap.cache.close <- true
	return err
}
