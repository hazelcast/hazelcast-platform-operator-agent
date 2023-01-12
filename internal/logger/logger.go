package logger

import (
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
)

func New() (logr.Logger, error) {
	var log logr.Logger
	zapLog, err := zap.NewDevelopment()
	if err != nil {
		return log, err
	}
	log = zapr.NewLogger(zapLog)

	return log, nil
}
