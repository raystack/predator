package main

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

var version = "0.1.0"

var (
	predator        = kingpin.New("predator", "downstream data profiler and auditor")
	profileCmd      = newCommand(predator.Command("profile", "profile only"))
	profileAuditCmd = newCommand(predator.Command("profile_audit", "profile and audit"))
	versionCmd      = predator.Command("version", "version of predator")
)

func main() {
	args, err := predator.Parse(os.Args[1:])

	switch kingpin.MustParse(args, err) {

	case versionCmd.FullCommand():
		fmt.Println(version)
	case profileCmd.cmd.FullCommand():
		config := &ProfileConfig{
			Host:      *profileCmd.server,
			URN:       *profileCmd.urn,
			Filter:    *profileCmd.filter,
			Group:     *profileCmd.group,
			Mode:      *profileCmd.mode,
			AuditTime: *profileCmd.auditTime,
		}
		Profile(config)
	case profileAuditCmd.cmd.FullCommand():
		config := &ProfileConfig{
			Host:      *profileAuditCmd.server,
			URN:       *profileAuditCmd.urn,
			Filter:    *profileAuditCmd.filter,
			Group:     *profileAuditCmd.group,
			Mode:      *profileAuditCmd.mode,
			AuditTime: *profileAuditCmd.auditTime,
		}
		ProfileAudit(config)
	default:
		log.Println("command not found")
	}
}
