package log

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// TODO for deduped functions, if timeLogged > logTypeLimit, should we log once
// every... 100 (?) times so we don't lose track entirely?

// concurrency-safe counter
var ctr = newCounter()

const (
	flagFormat       = "log-format"
	flagLevel        = "log-level"
	flagDisableColor = "disable-log-color"
)

// InitLogging sets standard configuration values for logging.
//
// If showLogLevelSetMessage is true, will log an unleveled message
// specifying the log level post-init.
func InitLogging(showLogLevelSetMessage bool) {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	// Default to using pretty formatting
	if strings.ToLower(viper.GetString(flagFormat)) != "json" {
		disableColor := viper.GetBool(flagDisableColor)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano, NoColor: disableColor})
	}

	level, err := zerolog.ParseLevel(viper.GetString(flagLevel))
	if err != nil {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		log.Warn().Msg("Error parsing log-level, setting level to 'info'")
		return
	}
	zerolog.SetGlobalLevel(level)

	if showLogLevelSetMessage {
		log.Log().Msgf("Log level set to %v", level)
	}

}

func GetLogger() *zerolog.Logger {
	return &log.Logger
}

func SetLogger(l *zerolog.Logger) {
	log.Logger = *l
}

func GetLogLevel() string {
	return zerolog.GlobalLevel().String()
}

func SetLogLevel(l string) error {

	level, err := zerolog.ParseLevel(l)
	if err != nil {
		return err
	}

	zerolog.SetGlobalLevel(level)
	log.Info().Msg(fmt.Sprintf("log level set to %s.", l))
	return nil
}

func Errorf(format string, a ...interface{}) {
	log.Error().Msgf(format, a...)
}

func DedupedErrorf(logTypeLimit int, format string, a ...interface{}) {
	timesLogged := ctr.increment(format)

	if timesLogged < logTypeLimit {
		Errorf(format, a...)
	} else if timesLogged == logTypeLimit {
		Errorf(format, a...)
		Infof("%s logged %d times: suppressing future logs", fmt.Sprintf(format, a...), logTypeLimit)
	}
}

func Warnf(format string, a ...interface{}) {
	log.Warn().Msgf(format, a...)
}

func DedupedWarningf(logTypeLimit int, format string, a ...interface{}) {
	timesLogged := ctr.increment(format)

	if timesLogged < logTypeLimit {
		Warnf(format, a...)
	} else if timesLogged == logTypeLimit {
		Warnf(format, a...)
		Infof("%s logged %d times: suppressing future logs", fmt.Sprintf(format, a...), logTypeLimit)
	}
}

func Info(msg string) {
	log.Info().Msg(msg)
}

func Infof(format string, a ...interface{}) {
	log.Info().Msgf(format, a...)
}

func DedupedInfof(logTypeLimit int, format string, a ...interface{}) {
	timesLogged := ctr.increment(format)

	if timesLogged < logTypeLimit {
		Infof(format, a...)
	} else if timesLogged == logTypeLimit {
		Infof(format, a...)
		Infof("%s logged %d times: suppressing future logs", fmt.Sprintf(format, a...), logTypeLimit)
	}
}

func Profilef(format string, a ...interface{}) {
	log.Info().Msgf(fmt.Sprintf("[Profiler] %s", format), a...)
}

func Debug(msg string) {
	log.Debug().Msg(msg)
}

func Debugf(format string, a ...interface{}) {
	log.Debug().Msgf(format, a...)
}

func Trace(msg string) {
	log.Trace().Msg(msg)
}

func Tracef(format string, a ...interface{}) {
	log.Trace().Msgf(format, a...)
}

func Fatalf(format string, a ...interface{}) {
	log.Fatal().Msgf(format, a...)
}

func Profile(start time.Time, name string) {
	elapsed := time.Since(start)
	Profilef("%s: %s", elapsed, name)
}

func ProfileWithThreshold(start time.Time, threshold time.Duration, name string) {
	elapsed := time.Since(start)
	if elapsed > threshold {
		Profilef("%s: %s", elapsed, name)
	}
}
