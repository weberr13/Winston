package server

import (
	ENV "github.com/joho/godotenv"
)

type WinstonConf struct {
	DataDir string
	Port    int
}

func NewWinstonConf() WinstonConf {
	env, err := ENV.Read()
	if nil != err {
		panic("Failed to load environment variables, check your .env file in your current working directory.")
	}
	c := WinstonConf{Port: 5001, DataDir: env["DATA_DIR"]}
	return c
}
