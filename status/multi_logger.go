package status

import "github.com/odpf/predator/protocol"

//MultiLogger log job status in multiple StatusLogger implementation
type MultiLogger struct {
	statusLoggers []protocol.StatusLogger
}

//NewMultiLogger is constructor
func NewMultiLogger(statusLoggers []protocol.StatusLogger) *MultiLogger {
	return &MultiLogger{
		statusLoggers: statusLoggers,
	}
}

//Log to store map of profile id and bq job id to stdout
func (m *MultiLogger) Log(entry protocol.Entry, message string) error {
	for _, loggr := range m.statusLoggers {
		if err := loggr.Log(entry, message); err != nil {
			return err
		}
	}
	return nil
}
