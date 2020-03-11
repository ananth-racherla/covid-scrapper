/*
* This scrapper relies on Covid19 timeseries data
 */
package main

import (
	"context"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"flag"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ananth-racherla/covid-scrapper/kafka"

	"github.com/rs/zerolog/log"
)

var logger = log.With().Str("pkg", "main").Logger()

type record struct {
	Region     string
	Country    string
	Lat        float64
	Long       float64
	RecordedOn time.Time
	Count      int64
}

func main() {
	var (
		dataURL        string // data endpoint containing the covid19 data
		kafkaBrokerURL string // kafka endpoint to send the scapped information to
		kafkaTopic     string // Kafka topic to publish to
	)

	// Read the program params from env or setup defaults
	flag.StringVar(&dataURL, "dataEndpoint", "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv", "URI for the covid information")
	flag.StringVar(&kafkaBrokerURL, "kafkaBrokerEndpointCSV", "kafka-headless.kafka-cluster:9092", "Comma separated kafka broker list")
	flag.StringVar(&kafkaTopic, "kafka-topic", "covid19", "Kafka topic to push")

	flag.Parse()

	records, err := fetchCovidData(dataURL) // fetch data from Github
	if err != nil {
		panic(err)
	}

	// Now send each record over to kafka
	kafkaProducer, err := kafka.Configure(strings.Split(kafkaBrokerURL, ","), kafkaTopic) // Initialize kafka
	if err != nil {
		logger.Error().Str("error", err.Error()).Msg("unable to configure kafka")
		return
	}
	defer kafkaProducer.Close()

	var wg sync.WaitGroup

	for _, data := range records { // loop through the covid19 dataset
		wg.Add(1)

		go func(data record) {
			defer wg.Done()
			jsonData, err := json.Marshal(&data)
			if err != nil {
				logger.Error().Str("error", err.Error()).Msg("Failed convert message to json")
				return
			}

			err = kafka.Push(context.Background(), nil, jsonData) // Publish each record to kafka
			if err != nil {
				logger.Error().Str("error", err.Error()).Msg("Failed to send message to kafka")
			}
		}(data)
	}

	wg.Wait() // Wait for all of the go routines to finish
}

func fetchCovidData(url string) ([]record, error) {
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

	const longForm = "1/2/06"

	var results []record

	header := data[0]
	for i, row := range data { // loop through the covid19 dataset

		if i == 0 { // header
			continue
		}

		for j := 4; j < len(row); j++ {
			r := record{Region: row[0], Country: row[1]}
			r.RecordedOn, _ = time.Parse(longForm, header[j])
			r.Lat, _ = strconv.ParseFloat(row[2], 64)
			r.Long, _ = strconv.ParseFloat(row[3], 64)
			r.Count, _ = strconv.ParseInt(row[j], 10, 64)

			results = append(results, r)
		}
	}

	return results, nil
}
