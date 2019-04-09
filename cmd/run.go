// Copyright © 2018 Nipun Balan Thekkummal <nipunbalan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"sync"

	"github.com/nipunbalan/edge-gatekeeper/forwarder"
	"github.com/nipunbalan/edge-gatekeeper/monitoring"
	"github.com/spf13/viper"

	"github.com/spf13/cobra"
)

var wg = sync.WaitGroup{}

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the edge gatekeeper",
	Long:  `Run the edge gatekeeper`,
	Run: func(cmd *cobra.Command, args []string) {
		runGateKeeper()
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	runCmd.PersistentFlags().String("fast", "", "Instruct to run fast")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	runCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// Run neccessary services for the gatekeeper to run
func runGateKeeper() {

	var dataRateCmdGoChan = make(chan int64)
	var statsCmdGoChan = make(chan string)
	fmt.Println("Running..")
	fmt.Printf("version: %s\n", viper.GetString("version"))
	wg.Add(1)
	go forwarder.RunCommandListner(dataRateCmdGoChan, statsCmdGoChan)
	//	go forwarder.RunForwarder(windowSizeCmdGoChan)
	go forwarder.RunForwarderWithRateLimiter(dataRateCmdGoChan)
	go monitoring.RunMonitoringD(statsCmdGoChan)
	//	ingest.RunReadSensorData()
	wg.Wait()
}
