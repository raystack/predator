package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/odpf/predator/client"
	xhttp "github.com/odpf/predator/external/http"
	"github.com/odpf/predator/protocol/job"
)

//ProfileAudit to start profile and audit
func ProfileAudit(config *ProfileConfig) {
	cli := client.New(config.Host, xhttp.NewClientWithTimeout(10*time.Minute))

	profileID := profile(config, cli)

	auditReport, err := cli.Audit(profileID)
	if err != nil {
		log.Fatal(fmt.Errorf("Auditing failed because:\n%w", err))
	}

	log.Printf("Audit with ID %s has finished", auditReport.AuditID)
	log.Printf(auditReport.Message)

	results, err := json.Marshal(auditReport.Result)
	if err != nil {
		log.Printf("Parse audit results error: %s", err)
	}
	log.Printf("Audit results: %s", string(results))
	if auditReport.Status == job.StateCompleted.String() && !auditReport.Pass {
		log.Fatal("Audit result is not passed the tolerance.")
	}
}
