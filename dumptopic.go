package dumptopic

import (
	"sync"

	"github.com/Shopify/sarama"
)

// GetChanelFromClient returns a channel that can be read to completion
// The channel will be closed when all the messages have been read up to the highwater mark.
func GetChanelFromClient(topic string, client sarama.Client) (<-chan *sarama.ConsumerMessage, error) {
	return do(topic, client, false)
}

// GetChannel returns a channel that can be read to completion
// The channel will be closed when all the messages have been read up to the highwater mark.
func GetChannel(brokers []string, topic string, config *sarama.Config) (<-chan *sarama.ConsumerMessage, error) {
	// Client
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}

	return do(topic, client, true)
}

func do(topic string, client sarama.Client, closeclient bool) (<-chan *sarama.ConsumerMessage, error) {

	// Consumer
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		client.Close()
		return nil, err
	}

	// Get the partitions
	partitions, err := client.Partitions(topic)
	if err != nil {
		consumer.Close()
		client.Close()
		return nil, err
	}

	var wg sync.WaitGroup

	// Loop through the partitions and start reading
	messagechan := make(chan *sarama.ConsumerMessage)
	for _, partition := range partitions {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()

			offset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				return
			}

			// Nothing to do if offset is 0
			if offset == 0 {
				return
			}

			// We want the last current offset, not "OffsrtNewest", which is the next offset
			offset = offset - 1

			partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
			if err != nil {
				return
			}
			defer partitionConsumer.Close()
			for {
				select {
				case msg := <-partitionConsumer.Messages():
					messagechan <- msg
					if msg.Offset >= partitionConsumer.HighWaterMarkOffset()-1 {
						return
					}
				}
			}

		}(partition)
	}

	go func() {
		wg.Wait()
		close(messagechan)
		consumer.Close()
		if closeclient {
			client.Close()
		}
	}()

	return messagechan, nil
}
