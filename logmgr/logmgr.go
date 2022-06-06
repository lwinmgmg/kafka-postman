package logmgr

import "github.com/lwinmgmg/logger"

var lgr *logger.Logging

func init() {
	lgr = logger.NewLogging(
		logger.DEBUG,
		func() string { return "" },
		1000,
		logger.DEFAULT_PATTERN,
		logger.DEFAULT_TFORMAT,
		logger.NewConsoleWriter(),
	)
}

func GetLogger() *logger.Logging {
	return lgr
}
