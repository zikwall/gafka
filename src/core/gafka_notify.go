package core

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func (gf *GafkaEmitter) Shutdown() {
	logln("Receive shutdown signal, processed...")

	gf.cancel()

	go func() {
		gf.wg.Wait()
		gf.done <- struct{}{}

		logln("Done all listeners and coordinators!")
	}()

	gf.waitAllListeners()
	os.Exit(0)
}

func (gf GafkaEmitter) WaitInternalNotify() {
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	<-sig
}

func (gf GafkaEmitter) waitAllListeners() {
	// wait done listeners
	select {
	case <-gf.done:
		log.Println("Exit")
	case <-time.After(5 * time.Second):
		log.Println("Exited after a long wait of 5 seconds")
	}
}
