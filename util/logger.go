package util

import (
	"context"
	"os"
	"sync"
	
	"github.com/sirupsen/logrus"
)

const DefaultLogLevel = logrus.InfoLevel

var once = &sync.Once{}
var logger = logrus.New()

func GetLogger(ctx context.Context) *logrus.Entry {
	once.Do(func() {
		logger.SetLevel(getLogLevel())
		logger.SetOutput(os.Stdout)
	})
	return logger.WithContext(ctx)
}

func getLogLevel() logrus.Level {
	logLevel, logLevelPresent := os.LookupEnv("LOG_LEVEL")
	if !logLevelPresent {
		return DefaultLogLevel
	}
	customLogLevel, customLogLevelErr := logrus.ParseLevel(logLevel)
	if customLogLevelErr != nil {
		return DefaultLogLevel
	}
	return customLogLevel
}
