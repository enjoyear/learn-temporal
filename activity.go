package app

import (
	"context"
	"fmt"
)

func ComposeGreeting(ctx context.Context, name string) (string, error) {
	println("Got activity request with name: " + name)
	greeting := fmt.Sprintf("Hello %s!", name)
	return greeting, nil
}
