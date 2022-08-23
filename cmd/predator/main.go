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

var version = "0.10.0"

var (
	predator   = kingpin.New("predator", "downstream data profiler and auditor")
	startCmd   = newCommand(predator.Command("start", "start predator service"))
	migrateCmd = newCommand(predator.Command("migrate", "db migrate"))
	rollback   = newCommand(predator.Command("rollback", "db rollback"))

	upload     = predator.Command("upload", "upload spec from git repository to storage")
	host       = upload.Flag("host", "predator server").Required().Short('h').String()
	gitURL     = upload.Flag("git-url", "url of git, the source of data quality spec").Required().Short('g').String()
	commitID   = upload.Flag("commit-id", "specific git commit hash, default value will be empty and always upload latest commit").Default("").Short('c').String()
	pathPrefix = upload.Flag("path-prefix", "path to root of predator specs directory, default will be empty").Default("").Short('p').String()

	versionCmd = predator.Command("version", "version of predator")
)

func main() {
	args, err := predator.Parse(os.Args[1:])

	switch kingpin.MustParse(args, err) {
	case startCmd.cmd.FullCommand():
		confFile := &conf.ConfigFile{
			FilePath: *startCmd.cEnv,
		}
		server.StartService(confFile, version)
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
		fmt.Println(version)
	case upload.FullCommand():
		config := &UploadConfig{
			Host:       *host,
			PathPrefix: *pathPrefix,
			GitURL:     *gitURL,
			CommitID:   *commitID,
		}
		Upload(config)
	default:
		log.Println("command not found")
	}
}
