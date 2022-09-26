package cmd

import (
	"fmt"
	"github.com/odpf/predator/client"
	xhttp "github.com/odpf/predator/external/http"
	"github.com/odpf/predator/protocol"
	"log"
	"time"
)

//UploadConfig for uploading tolerance files
type UploadConfig struct {
	Host       string
	PathPrefix string
	GitURL     string
	CommitID   string
}

func Upload(config *UploadConfig) {
	cli := client.New(config.Host, xhttp.NewClientWithTimeout(10*time.Minute))

	gitInfo := &protocol.GitInfo{
		URL:        config.GitURL,
		CommitID:   config.CommitID,
		PathPrefix: config.PathPrefix,
	}

	log.Printf("git url : %s", gitInfo.URL)
	log.Printf("commit SHA :%s", gitInfo.CommitID)
	log.Printf("path prefix :%s", gitInfo.PathPrefix)
	log.Println("uploading spec")

	start := time.Now().In(time.UTC)
	report, err := cli.Upload(gitInfo)
	end := time.Now().In(time.UTC)
	duration := end.Sub(start)

	log.Printf("process duration: %f seconds \n", duration.Seconds())

	if err != nil {
		log.Fatal(fmt.Errorf("upload spec failed because :\n%w", err))
	}

	log.Printf("spec uploaded: %d, removed: %d\n", report.UploadedCount, report.RemovedCount)
}
