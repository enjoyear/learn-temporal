package app

import (
	"context"
	"go.temporal.io/sdk/workflow"
	"log"
	"time"
)

type BulletinBoardPost struct {
	Title             string
	Body              string
	PublisherPlatform string
	NotificationType  string
}

func PostRuleMatchingTask(ctx context.Context, data BulletinBoardPost) (string, error) {
	log.Printf(">>>> PostRuleMatchingTask running for %s", data.Title)
	return "Done", nil
}
func PostRuleMatching(ctx workflow.Context, input BulletinBoardPost) (string, error) {
	log.Printf(">>>> Workflow starts")
	options := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	}

	// Apply the options.
	ctx = workflow.WithActivityOptions(ctx, options)

	var taskStatus string
	taskError := workflow.ExecuteActivity(ctx, PostRuleMatchingTask, input).Get(ctx, &taskStatus)

	if taskError != nil {
		return "", taskError
	}

	log.Printf(">>>> Workflow completes")
	return "Success", nil
}
