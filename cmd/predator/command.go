package main

import "gopkg.in/alecthomas/kingpin.v2"

type command struct {
	cmd  *kingpin.CmdClause
	cEnv *string
}

func newCommand(cmdClause *kingpin.CmdClause) *command {
	return &command{
		cmd:  cmdClause,
		cEnv: addEnvFlag(cmdClause).String(),
	}
}

func addEnvFlag(cmdClause *kingpin.CmdClause) *kingpin.FlagClause {
	return cmdClause.Flag("env-file", "path of config file").Short('e')
}
