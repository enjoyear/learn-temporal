package app

import (
	"context"
	"fmt"
)

func ComposeGreeting(ctx context.Context, name string) (string, error) {
	println("Start golang activity with parameter: " + name)
	greeting := fmt.Sprintf("Hello %s!", name)
	return greeting, nil
}
