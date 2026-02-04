package config

import (
	"reflect"
	"testing"

	"go.uber.org/zap"
)

func TestLoggerInit(t *testing.T) {
	original := GetLogger()
	for i := range 100 {
		l := GetLogger()
		t.Logf("equal to original\t%v\n", reflect.DeepEqual(original, l))
		l.Info("msg:", zap.Int("loggers generated", i))
	}
}
