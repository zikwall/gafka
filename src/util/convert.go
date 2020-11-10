package util

import "strconv"

func ImmutableInt(s string) int {
	n, err := strconv.Atoi(s)

	if err != nil {
		return 0
	}

	return n
}
