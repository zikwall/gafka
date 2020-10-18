package v2

import "fmt"

func logln(logs ...interface{}) {
	fmt.Println("[GAFKA]", logs)
}
