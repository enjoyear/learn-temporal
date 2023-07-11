package app

import (
	"context"
	"fmt"
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

func PostRuleMatchingTask(ctx context.Context, post BulletinBoardPost,
	ruleStartInclusive int64, ruleEndExclusive int64) (string, error) {
	log.Printf(">> >> Executing PostRuleMatchingTask for Post %s with rules [%d, %d)",
		post.Title, ruleStartInclusive, ruleEndExclusive)

	return fmt.Sprintf("Done for [%d, %d)", ruleStartInclusive, ruleEndExclusive), nil
}
func PostRuleMatching(ctx workflow.Context, input BulletinBoardPost,
	ruleStartInclusive int64, ruleEndExclusive int64, ruleSplitSize int64) (string, error) {

	log.Printf(">> Workflow starts")
	options := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	}

	// Apply the options.
	ctx = workflow.WithActivityOptions(ctx, options)

	for i := ruleStartInclusive; i < ruleEndExclusive; i += ruleSplitSize {
		ruleStart := i
		ruleEnd := min(ruleEndExclusive, ruleStart+ruleSplitSize)
		log.Printf(">> Submitting activity for Post %s with rules [%d, %d)",
			input.Title, ruleStart, ruleEnd)

		var taskStatus string
		taskError := workflow.ExecuteActivity(ctx, PostRuleMatchingTask, input, ruleStart, ruleEnd).Get(ctx, &taskStatus)

		log.Printf(">> Activity submission completed with %s", taskStatus)
		if taskError != nil {
			return "", taskError
		}
	}

	log.Printf(">> Workflow completes")
	return "Success", nil
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
