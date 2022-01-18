package logger

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// getTimeEncoder returns time encoder for logger
func getTimeEncoder() zapcore.TimeEncoder {
	return zapcore.TimeEncoderOfLayout("02.01.2006 15:04:05 -07:00")
}

// getDevConfig returns config for development logger
func getDevConfig() zap.Config {
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	encoderConfig.EncodeTime = getTimeEncoder()

	config := zap.NewDevelopmentConfig()
	config.EncoderConfig = encoderConfig

	return config
}

// getProdConfig returns config for production logger
func getProdConfig() zap.Config {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.EncodeTime = getTimeEncoder()

	config := zap.NewProductionConfig()
	config.Encoding = "console"
	config.EncoderConfig = encoderConfig

	return config
}

// buildLogger builds logger by config
func buildLogger(config zap.Config) (*zap.Logger, error) {
	l, err := config.Build()
	if err != nil {
		return nil, err
	}
	return l, nil
}

// NewLogger generates new logger by logger mode
func NewLogger(loggerMode string) (*zap.Logger, error) {
	switch loggerMode {
	case LoggerModeDev: // logger for development mode
		return buildLogger(getDevConfig())
	case LoggerModeProd: // logger for production mode
		return buildLogger(getProdConfig())
	default: // if mode is wrong or missing
		return nil, fmt.Errorf("wrong logger mode: %s", loggerMode)
	}
}
