package util

import "testing"

func TestImmutableInt(t *testing.T) {
	t.Run("it should success covert string to int", func(t *testing.T) {
		n := ImmutableInt("10")

		if n != 10 {
			t.Fatal("Incorrect value received, expected to get 10")
		}
	})

	t.Run("it should be failt convert string to int", func(t *testing.T) {
		n := ImmutableInt("ten")

		if n != 0 {
			t.Fatal("Incorrect value received, expected to get 0")
		}

		n = ImmutableInt("")

		if n != 0 {
			t.Fatal("Incorrect value received, expected to get 0")
		}
	})
}
