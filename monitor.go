package main

import (
	"github.com/IBM/sarama"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Monitor for kafka
type Monitor interface {
	GetConsumerGroupLag(consumerGroup, topic string) (int64, int64, map[int32]int64, error)
	GetNumberOfPartitions(topic string) (size int, err error)
}

// DefaultMonitor is the default implementation for the Monitor interface
type DefaultMonitor struct {
	cfg    MonitorConfig
	client sarama.Client
}

// MonitorConfig parameters
type MonitorConfig struct {
	ClientID string
	Brokers  []string
}

// NewMonitor with default monitor implementation
func NewMonitor(cfg MonitorConfig) (m *DefaultMonitor, err error) {
	m = new(DefaultMonitor)
	m.cfg = cfg
	return
}

func (m *DefaultMonitor) setupClient() (err error) {
	scfg := sarama.NewConfig()
	scfg.ClientID = m.cfg.ClientID
	m.client, err = sarama.NewClient(m.cfg.Brokers, scfg)
	err = errors.Wrap(err, "fail to start new sarama client")
	return
}

// GetConsumerGroupLag ...
func (m *DefaultMonitor) GetConsumerGroupLag(consumerGroup, topic string) (lastOffset, committed int64, perPartition map[int32]int64, err error) {
	// setupClient - based on requirement this can be one time open connection and keep it open
	// for demo purpose - just create and close
	err = m.setupClient()
	if err != nil {
		return
	}
	defer func() {
		_ = m.client.Close()
		m.client = nil
	}()

	partitions, err := m.client.Partitions(topic)
	if err != nil {
		err = errors.Wrapf(err, "fail to get partitions for topic '%s'", topic)
		return
	}

	perPartition = make(map[int32]int64, len(partitions))

	var (
		manager  sarama.OffsetManager
		pom      sarama.PartitionOffsetManager
		position int64
	)

	manager, err = sarama.NewOffsetManagerFromClient(consumerGroup, m.client)
	if err != nil {
		err = errors.Wrap(err, "error in creating offset manager")
		return
	}

	defer manager.Close()

	for _, partition := range partitions {
		log.Trace("partition", partition)

		pom, err = manager.ManagePartition(topic, partition)

		if err != nil {
			err = errors.Wrap(err, "not able to get pom")
			return
		}

		offset, _ := pom.NextOffset()
		pom.AsyncClose()

		log.Trace("offset", offset)
		if offset != -1 {
			committed += offset
		}

		position, err = m.client.GetOffset(topic, partition, sarama.OffsetNewest)
		log.Trace("position, err", position, err)
		if err != nil {
			err = errors.Wrap(err, "not able to get newest offset from kafka")
			return
		}
		if position != -1 {
			lastOffset += position
		}
		if offset != -1 && position != -1 {
			perPartition[partition] = position - offset
		}
	}

	return
}

func (m *DefaultMonitor) GetNumberOfPartitions(topic string) (size int, err error) {
	err = m.setupClient()
	if err != nil {
		return
	}
	defer func() {
		_ = m.client.Close()
		m.client = nil
	}()

	partitions, err := m.client.Partitions(topic)
	if err != nil {
		err = errors.Wrapf(err, "fail to get partitions for topic '%s'", topic)
		return
	}
	return len(partitions), nil
}
