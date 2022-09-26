package cmd

import (
	"fmt"
	"github.com/odpf/predator/conf"
	"github.com/odpf/predator/db"
	"github.com/odpf/predator/server"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"os"
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

type commandServer struct {
	cmd  *kingpin.CmdClause
	cEnv *string
}

func newCommandServer(cmdClause *kingpin.CmdClause) *commandServer {
	return &commandServer{
		cmd:  cmdClause,
		cEnv: cmdClause.Flag("env-file", "path of config file").Short('e').String(),
	}
}

type commandProfileAudit struct {
	cmd       *kingpin.CmdClause
	server    *string
	urn       *string
	filter    *string
	group     *string
	mode      *string
	auditTime *string
}

func newCommandProfileAudit(cmdClause *kingpin.CmdClause) *commandProfileAudit {
	return &commandProfileAudit{
		cmd:       cmdClause,
		server:    cmdClause.Flag("server", "predator server url").Short('s').Envar("URL").String(),
		urn:       cmdClause.Flag("urn", "table URN").Short('u').Envar("URN").String(),
		filter:    cmdClause.Flag("filter", "data filter in query statement").Default("").Short('f').Envar("FILTER").String(),
		group:     cmdClause.Flag("group", "group of profile").Default("").Short('g').Envar("GROUP").String(),
		mode:      cmdClause.Flag("mode", "mode of profiling").Default("").Short('m').Envar("MODE").String(),
		auditTime: cmdClause.Flag("audit_time", "time of profile and audit").Default("").Short('a').Envar("AUDIT_TIME").String(),
	}
}

type commandUpload struct {
	cmd        *kingpin.CmdClause
	host       *string
	pathPrefix *string
	gitURL     *string
	commitID   *string
}

func newCommandUpload(cmdClause *kingpin.CmdClause) *commandUpload {
	return &commandUpload{
		cmd:        cmdClause,
		host:       cmdClause.Flag("host", "predator server").Required().Short('h').String(),
		pathPrefix: cmdClause.Flag("path-prefix", "path to root of predator specs directory, default will be empty").Default("").Short('p').String(),
		gitURL:     cmdClause.Flag("git-url", "url of git, the source of data quality spec").Required().Short('g').String(),
		commitID:   cmdClause.Flag("commit-id", "specific git commit hash, default value will be empty and always upload latest commit").Default("").Short('c').String(),
	}
}

func Execute() {
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
