package core

import "fmt"

func logln(logs ...interface{}) {
	fmt.Println("[GAFKA]", logs)
}

// todo debugln(), errorln(), warningln()
