package lib

import "fmt"

func logln(logs ...interface{}) {
	fmt.Println("[GAFKA]", logs)
}
