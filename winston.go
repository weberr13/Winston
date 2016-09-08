package main

import (
	"github.com/LogRhythm/Winston/server"
	log "github.com/cihub/seelog"
	"github.com/vrecan/death"
	SYS "syscall"
)

func main() {
	defer log.Flush()
	logger, err := log.LoggerFromConfigAsFile("seelog.xml")

	if err != nil {
		log.Warn("failed to load seelog config", err)
	}

	log.ReplaceLogger(logger)
	log.Info("starting winston")

	death := death.NewDeath(SYS.SIGINT, SYS.SIGTERM)

	w := server.NewWinston()
	w.Start()
	death.WaitForDeath(w)
	log.Info("shutdown")
}
