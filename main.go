package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"khajiit/env"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var (
	processTmpDir string
)

func main() {
	var err error

	// set up logging:
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LUTC | log.Lmicroseconds | log.Ltime)

	sarama.Logger = log.New(os.Stderr, "kafka: ", log.LUTC|log.Lmicroseconds|log.Ltime|log.Lmsgprefix)

	processTmpDir = env.GetOrSupply("KAFKA_TMP", func() string {
		return os.TempDir()
	})
	processTmpDir = filepath.Join(processTmpDir, strconv.Itoa(os.Getpid()))
	err = os.MkdirAll(processTmpDir, 0700)
	if err != nil {
		log.Fatalln(err)
		return
	}

	// process env vars for kafka config:
	var addrs []string
	var conf *sarama.Config
	addrs, conf, err = createKafkaClient()
	if err != nil {
		log.Fatalln(err)
	}

	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	// use the raw Partition value in ProducerMessage
	conf.Producer.Partitioner = sarama.NewManualPartitioner

	// create a kafka client:
	var client sarama.Client
	client, err = sarama.NewClient(addrs, conf)
	if err != nil {
		return
	}

	defer func() {
		err := client.Close()
		if err != nil {
			log.Println(err)
		}
	}()

	var prod sarama.SyncProducer
	prod, err = sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return
	}

	// read lines of JSON from stdin:
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1048576*100), 1048576*100)
	for scanner.Scan() {
		var kafkaCatMessage map[string]json.RawMessage

		lineBytes := scanner.Bytes()

		// each line is a JSON document from kafkacat with a CMP envelope inside `payload` as a string:
		kafkaCatMessage = make(map[string]json.RawMessage)
		err = json.Unmarshal(lineBytes, &kafkaCatMessage)
		if err != nil {
			log.Println(fmt.Errorf("json unmarshal: %w", err))
			continue
		}

		var headers []sarama.RecordHeader

		// headers come in as paired values in a single array. we want to transform this into a dictionary of key/value pairs
		headersRaw := make([]string, 0, 32)
		err = json.Unmarshal(kafkaCatMessage["headers"], &headersRaw)
		if err != nil {
			log.Println(fmt.Errorf("json unmarshal 'headers': %w", err))
		} else {
			// pair up keys with values:
			headers = make([]sarama.RecordHeader, 0, len(headersRaw)/2)
			for i := 0; i < len(headersRaw); i += 2 {
				headers = append(headers, sarama.RecordHeader{
					Key:   []byte(headersRaw[i]),
					Value: []byte(headersRaw[i+1]),
				})
			}
		}

		// deserialize the raw fields for topic, partition, and key:
		var topic string
		var partition int
		var key string

		err = json.Unmarshal(kafkaCatMessage["topic"], &topic)
		if err != nil {
			log.Println(fmt.Errorf("json unmarshal 'topic': %w", err))
			continue
		}

		err = json.Unmarshal(kafkaCatMessage["partition"], &partition)
		if err != nil {
			log.Println(fmt.Errorf("json unmarshal 'partition': %w", err))
			continue
		}

		err = json.Unmarshal(kafkaCatMessage["key"], &key)
		if err != nil {
			log.Println(fmt.Errorf("json unmarshal 'key': %w", err))
			continue
		}

		// payload is a []byte effectively (really a json.RawMessage):
		payload := kafkaCatMessage["payload"]

		var out_partition int32
		var out_offset int64
		out_partition, out_offset, err = prod.SendMessage(&sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.ByteEncoder(key),
			Value:     sarama.ByteEncoder(payload),
			Headers:   headers,
			Partition: int32(partition),
			Timestamp: time.Now(),
		})
		if err != nil {
			break
		}
		_, _ = out_partition, out_offset
	}

	log.Println("Initiating shutdown of producer...")
	err = prod.Close()
	if err != nil {
		log.Printf("Error shutting down producer: %v\n", err)
	}
}
