package main

import "gopkg.in/alecthomas/kingpin.v2"

type command struct {
	cmd       *kingpin.CmdClause
	server    *string
	urn       *string
	filter    *string
	group     *string
	mode      *string
	auditTime *string
}

func newCommand(cmdClause *kingpin.CmdClause) *command {
	return &command{
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
	return cmdClause.Flag("server", "predator server url").Required().Short('s').Envar("URL")
}

func addURNFlag(cmdClause *kingpin.CmdClause) *kingpin.FlagClause {
	return cmdClause.Flag("urn", "table URN").Required().Short('u').Envar("URN")
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
