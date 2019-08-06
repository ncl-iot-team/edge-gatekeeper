package ingest

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/spf13/viper"
)

var wg = sync.WaitGroup{}

// RunReadSensorData runs all the configured listners
func RunReadSensorData() {

	files, err := ioutil.ReadDir(viper.GetString("listeners.tsensor2.path"))

	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()

	// Rate limiter limits the number of sensor values read per second
	limit := rate.Limit(float64(viper.GetFloat64("forwarder.cloud-rabbitmq.rate")))
	limiter := rate.NewLimiter(limit, 1)
	cntx := context.Background()

	i := 0
	for _, f := range files {
		i = i + 1
		if !f.IsDir() && strings.HasSuffix(strings.ToLower(f.Name()), ".csv") {
			fmt.Println("Reading data from sensor '" + strings.TrimSuffix(f.Name(), ".csv") + "'")
			wg.Add(1)
			go processSensorData(f, 10, limiter, &cntx)
		}

	}

	fmt.Printf("Read %d files\n", i)
	wg.Wait()

	elapsed := time.Since(start)
	log.Printf("Sensor File Processing took %s", elapsed)

}

func processSensorData(fileInfo os.FileInfo, processRate int64, limiter *rate.Limiter, context *context.Context) {

	defer wg.Done()
	fileNameStr := fmt.Sprintf("%s/%s", viper.GetString("listeners.tsensor2.path"), fileInfo.Name())
	file, err := os.Open(fileNameStr)

	failOnError(err, "Failed to open the file")

	reader := bufio.NewReader(file)
	scanner := bufio.NewScanner(reader)

	for i := 0; scanner.Scan(); i++ {
		if i == 0 {
			i++
			continue
		}
		limiter.Wait(*context)
		line := scanner.Text()
		body := line
		//body = body + " "
		log.Println(body)
		failOnError(err, "Failed to publish a message")
		i++
	}

	file.Close()

}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
