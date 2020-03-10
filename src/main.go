/*
* This scrapper relies on Covid19 timeseries data
 */
package main

import (
	"context"
	"crypto/tls"
	"encoding/csv"
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/ananth-racherla/covid-scrapper/kafka"
	"github.com/rs/zerolog/log"
)

var logger = log.With().Str("pkg", "main").Logger()

func main() {
	var (
		dataURL        string // data endpoint containing the covid19 data
		kafkaBrokerURL string // kafka endpoint to send the scapped information to
		kafkaTopic     string // Kafka topic to publish to
	)

	// Read the program params from env or setup defaults
	flag.StringVar(&dataURL, "dataEndpoint", "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv", "URI for the covid information")
	flag.StringVar(&kafkaBrokerURL, "kafkaBrokerEndpointCSV", "kafka-headless.kafka-cluster:9092", "Comma separated kafka broker list")
	flag.StringVar(&kafkaClientID, "kafka-client-id", "my-kafka-client", "Kafka client id to connect")
	flag.StringVar(&kafkaTopic, "kafka-topic", "covid19", "Kafka topic to push")

	flag.Parse()

	data, err := fetchCovidData(dataURL)
	if err != nil {
		panic(err)
	}

	kafkaProducer, err := kafka.Configure(strings.Split(kafkaBrokerURL, ","), kafkaTopic) // Initialize kafka
	if err != nil {
		logger.Error().Str("error", err.Error()).Msg("unable to configure kafka")
		return
	}
	defer kafkaProducer.Close()

	for idx, row := range data { // loop through the covid19 dataset
		logger.Debug().Msg(fmt.Sprintf("Processing record: %d %s", idx, row[0]))

		err = kafka.Push(context.Background(), nil, []byte(row[0])) // Publish each record to kafka
		if err != nil {
			logger.Error().Str("error", err.Error()).Msg("Failed to send message to kafka")
		}
	}
}

func fetchCovidData(url string) ([][]string, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: tr}
	resp, err := client.Get(url)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	reader := csv.NewReader(resp.Body)
	reader.Comma = ','
	data, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return data, nil
}
