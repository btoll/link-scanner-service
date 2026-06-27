package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	linkscanner "github.com/btoll/link-scanner"
	"github.com/segmentio/kafka-go"
)

func main() {
	linkscanner.SetTagName("main")
	target, err := linkscanner.ProcessURL("https://benjamintoll.com/")
	if err != nil {
		panic(err)
	}
	c := make(chan *linkscanner.ScanResults)
	var wg sync.WaitGroup
	for _, v := range target.GetAllLinks() {
		for _, link := range v {
			wg.Go(func() {
				target, err := linkscanner.ProcessURL(link)
				if err != nil {
					log.Printf("Failed to scan %s: %v", link, err)
					return
				}
				c <- &linkscanner.ScanResults{
					Target:      link,
					LinkResults: target.GetFailures(),
				}
			})
		}
	}
	go func() {
		wg.Wait()
		close(c)
	}()
	w := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9093", "localhost:9094", "localhost:9095"),
		Topic:        "link-scanner-results",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
	defer func() {
		err := w.Close()
		if err != nil {
			panic(err)
		}
	}()
	for res := range c {
		data, err := json.Marshal(res)
		if err != nil {
			log.Printf("Failed to marshal result: %v", err)
			continue
		}
		err = w.WriteMessages(context.Background(), kafka.Message{
			Value: data,
		})
		if err != nil {
			log.Printf("Failed to write messages to topic %s: %v", w.Topic, err)
			continue
		}
	}
}
