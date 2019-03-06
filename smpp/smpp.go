package smpp

import (
	"log"

	smpp "github.com/fiorix/go-smpp/smpp"
)

//TX is a pointer to smpp transmitter
var TX *smpp.Transmitter

func Init() {
	tx := &smpp.Transmitter{
		Addr:   "smpp.smsc.ru:3700",
		User:   "carusel.club",
		Passwd: "carusel.club"}
	conn := tx.Bind()
	// check initial connection status
	var status smpp.ConnStatus
	if status = <-conn; status.Error() != nil {
		log.Fatalln("Unable to connect, aborting:", status.Error())
	}
	log.Println("Connection completed, status:", status.Status().String())
	// example of connection checker goroutine
	go func() {
		for c := range conn {
			log.Println("SMPP connection status:", c.Status())
		}
	}()
	TX = tx
}
