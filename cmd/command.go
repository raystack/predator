package main

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

type commandServer struct {
	cmd  *kingpin.CmdClause
	cEnv *string
}

func newCommandServer(cmdClause *kingpin.CmdClause) *commandServer {
	return &commandServer{
		cmd:  cmdClause,
		cEnv: addEnvFlag(cmdClause).String(),
	}
}

func addEnvFlag(cmdClause *kingpin.CmdClause) *kingpin.FlagClause {
	return cmdClause.Flag("env-file", "path of config file").Short('e')
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
		server:    addServerFlag(cmdClause).String(),
		urn:       addURNFlag(cmdClause).String(),
		filter:    addFilterFlag(cmdClause).String(),
		group:     addGroupFlag(cmdClause).String(),
		mode:      addModeFlag(cmdClause).String(),
		auditTime: addAuditTimeFlag(cmdClause).String(),
	}
}

func addServerFlag(cmdClause *kingpin.CmdClause) *kingpin.FlagClause {
	return cmdClause.Flag("server", "predator server url").Short('s').Envar("URL")
}

func addURNFlag(cmdClause *kingpin.CmdClause) *kingpin.FlagClause {
	return cmdClause.Flag("urn", "table URN").Short('u').Envar("URN")
}

func addFilterFlag(cmdClause *kingpin.CmdClause) *kingpin.FlagClause {
	return cmdClause.Flag("filter", "data filter in query statement").Default("").Short('f').Envar("FILTER")
}

func addGroupFlag(cmdClause *kingpin.CmdClause) *kingpin.FlagClause {
	return cmdClause.Flag("group", "group of profile").Default("").Short('g').Envar("GROUP")
}

func addModeFlag(cmdClause *kingpin.CmdClause) *kingpin.FlagClause {
	return cmdClause.Flag("mode", "mode of profiling").Default("").Short('m').Envar("MODE")
}

func addAuditTimeFlag(cmdClause *kingpin.CmdClause) *kingpin.FlagClause {
	return cmdClause.Flag("audit_time", "time of profile and audit").Default("").Short('a').Envar("AUDIT_TIME")
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
		host:       addHostFlag(cmdClause).String(),
		pathPrefix: addPathPrefixFlag(cmdClause).String(),
		gitURL:     addGitURLFlag(cmdClause).String(),
		commitID:   addCommitIDFlag(cmdClause).String(),
	}
}

func addHostFlag(cmdClause *kingpin.CmdClause) *kingpin.FlagClause {
	return cmdClause.Flag("host", "predator server").Required().Short('h')
}

func addGitURLFlag(cmdClause *kingpin.CmdClause) *kingpin.FlagClause {
	return cmdClause.Flag("git-url", "url of git, the source of data quality spec").Required().Short('g')
}

func addCommitIDFlag(cmdClause *kingpin.CmdClause) *kingpin.FlagClause {
	return cmdClause.Flag("commit-id", "specific git commit hash, default value will be empty and always upload latest commit").Default("").Short('c')
}

func addPathPrefixFlag(cmdClause *kingpin.CmdClause) *kingpin.FlagClause {
	return cmdClause.Flag("path-prefix", "path to root of predator specs directory, default will be empty").Default("").Short('p')
}
