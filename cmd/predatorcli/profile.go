package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/odpf/predator/api/model"
	"github.com/odpf/predator/client"
	xhttp "github.com/odpf/predator/external/http"
	"github.com/odpf/predator/protocol/job"
)

//ProfileConfig config
type ProfileConfig struct {
	Host      string
	URN       string
	Filter    string
	Group     string
	Mode      string
	AuditTime string
}

func checkProfileFailed(state job.State, message string) {
	if state == job.StateFailed {
		log.Fatal(fmt.Sprintf("Profiling failed because: %s", message))
	}
}

func profile(config *ProfileConfig, cli *client.Predator) string {
	profileRequest := &model.ProfileRequest{
		URN:       config.URN,
		Filter:    config.Filter,
		Group:     config.Group,
		Mode:      job.Mode(config.Mode),
		AuditTime: config.AuditTime,
	}

	profileReport, err := cli.Profile(profileRequest)
	if err != nil {
		log.Fatal(fmt.Errorf("Profiling failed because:\n%w", err))
	}
	checkProfileFailed(profileReport.State, profileReport.Message)
	if profileReport.Message != "" {
		log.Printf(profileReport.Message)
	}

	log.Printf("Profile with ID %s is running...", profileReport.ID)

	profileResult, err := cli.GetProfile(profileReport.ID)
	if err != nil {
		log.Fatal(fmt.Errorf("Profiling failed because:\n%w", err))
	}
	checkProfileFailed(profileResult.State, profileResult.Message)
	log.Printf("Profile finished: %s", profileResult.Message)
	metrics, err := json.Marshal(profileResult.Metrics)
	if err != nil {
		log.Printf("Parse metrics error: %s", err)
	}

	log.Printf("Records profiled: %d", profileResult.TotalRecords)
	log.Printf("Profile metrics: %s", string(metrics))
	return profileResult.ID
}

//Profile to start profile
func Profile(config *ProfileConfig) {
	cli := client.New(config.Host, xhttp.NewClientWithTimeout(10*time.Minute))
	profile(config, cli)
}
