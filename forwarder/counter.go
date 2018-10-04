package forwarder

import (
	"log"
	"time"
)

//var count int64

// NewRateCounter initiates a new counter
func NewRateCounter(window time.Duration, countChannel chan bool) {

	var count int64
	tick := make(chan bool)

	// Function generates ticks at the end of the configured window
	go func() {
		for {
			time.Sleep(window)
			tick <- true
		}
	}()

	// Fucntion records the count when a tick signal is received
	go func() {
		for range tick {
			recordCount(window, &count)
		}
	}()

	for range countChannel {

		count++
	}

}

//func increment() {
//	log.Printf("Increment Count: %d", count)
//	count
//}

func recordCount(window time.Duration, count *int64) {
	log.Printf("Count: %d", *count)
	*count = 0

}
