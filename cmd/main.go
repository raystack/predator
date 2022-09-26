package main

import (
	"fmt"
	"log"
	"os"

	"github.com/odpf/predator/conf"
	"github.com/odpf/predator/db"
	"github.com/odpf/predator/server"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	predator   = kingpin.New("predator", "downstream data profiler and auditor")
	startCmd   = newCommandServer(predator.Command("start", "start predator service"))
	migrateCmd = newCommandServer(predator.Command("migrate", "db migrate"))
	rollback   = newCommandServer(predator.Command("rollback", "db rollback"))

	uploadCmd = newCommandUpload(predator.Command("upload", "upload spec from git repository to storage"))

	profileCmd      = newCommandProfileAudit(predator.Command("profile", "profile only"))
	profileAuditCmd = newCommandProfileAudit(predator.Command("profile_audit", "profile and audit"))

	versionCmd = predator.Command("version", "version of predator")
)

func main() {
	args, err := predator.Parse(os.Args[1:])

	switch kingpin.MustParse(args, err) {
	case startCmd.cmd.FullCommand():
		confFile := &conf.ConfigFile{
			FilePath: *startCmd.cEnv,
		}
		server.StartService(confFile, conf.BuildVersion)
	case migrateCmd.cmd.FullCommand():
		confFile := &conf.ConfigFile{
			FilePath: *migrateCmd.cEnv,
		}
		db.Migrate(confFile)
	case rollback.cmd.FullCommand():
		confFile := &conf.ConfigFile{
			FilePath: *rollback.cEnv,
		}
		db.Rollback(confFile)
	case versionCmd.FullCommand():
		fmt.Printf("%s-%s\n", conf.BuildVersion, conf.BuildCommit)
	case uploadCmd.cmd.FullCommand():
		config := &UploadConfig{
			Host:       *uploadCmd.host,
			PathPrefix: *uploadCmd.pathPrefix,
			GitURL:     *uploadCmd.gitURL,
			CommitID:   *uploadCmd.commitID,
		}
		Upload(config)
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
