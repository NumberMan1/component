package zap_logger

import "github.com/NumberMan1/log"

type Config struct {
	Name        string    `json:"name" yaml:"name"`
	Level       log.Level `json:"level" yaml:"level"`
	StdoutTyp   string    `json:"stdout_typ" yaml:"stdout-typ"`
	Stdout      bool      `json:"stdout" yaml:"stdout"`
	LogFilePath string    `json:"log_file_path" yaml:"log-file-path"`
}

func (conf Config) OutputFile() bool {
	return conf.LogFilePath != ""
}
