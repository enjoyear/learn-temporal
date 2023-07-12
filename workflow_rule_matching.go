package app

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"log"
	"math/rand"
	"time"
)

type BulletinBoardPost struct {
	Title             string
	Body              string
	PublisherPlatform string
	NotificationType  string
}

const CodeVersion = 2

func PostRuleMatchingTask(ctx context.Context, post BulletinBoardPost,
	ruleStartInclusive int64, ruleEndExclusive int64) (string, error) {

	// ToDo: send metrics for retries

	log.Printf(">> >> Executing PostRuleMatchingTask for Post %s with rules [%d, %d)",
		post.Title, ruleStartInclusive, ruleEndExclusive)

	for i := ruleStartInclusive; i < ruleEndExclusive; i += 1 {
		if randomNumber := rand.Intn(100); randomNumber < 1 {
			log.Printf(">> >> Executing rule of ID %d at %s with code version %d",
				i, time.Now().Format("15:04:05.000"), CodeVersion)
			time.Sleep(5 * time.Second)
		}
	}

	return fmt.Sprintf("Done for [%d, %d)", ruleStartInclusive, ruleEndExclusive), nil
}
func PostRuleMatching(ctx workflow.Context, input BulletinBoardPost,
	ruleStartInclusive int64, ruleEndExclusive int64, ruleSplitSize int64) (string, error) {

	log.Printf(">> Workflow starts")
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2, // will run at most twice, retrying once
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	for i := ruleStartInclusive; i < ruleEndExclusive; i += ruleSplitSize {
		ruleStart := i
		ruleEnd := min(ruleEndExclusive, ruleStart+ruleSplitSize)
		log.Printf(">> Submitting activity for Post %s with rules [%d, %d)",
			input.Title, ruleStart, ruleEnd)

		var taskStatus string
		taskError := workflow.ExecuteActivity(ctx, PostRuleMatchingTask, input, ruleStart, ruleEnd).Get(ctx, &taskStatus)

		if taskError == nil {
			log.Printf(">> Activity execution completed with status: %s\n", taskStatus)
		} else {
			log.Printf(">> Activity execution failed with error: %s\n", taskError.Error())
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
