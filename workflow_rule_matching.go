package app

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/activity"
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

const CodeVersion = 3

func PostRuleMatchingTask(ctx context.Context, post BulletinBoardPost,
	ruleStartInclusive int64, ruleEndExclusive int64) (string, error) {
	info := activity.GetInfo(ctx)
	currentAttempt := info.Attempt

	// ToDo: send metrics for retries

	log.Printf(">> >> Executing PostRuleMatchingTask for Post %s with rules [%d, %d)",
		post.Title, ruleStartInclusive, ruleEndExclusive)

	for i := ruleStartInclusive; i < ruleEndExclusive; i += 1 {
		if ctx.Err() != nil {
			// Temporal does not automatically stop the execution of an activity function even when it has reached StartToClose timeout.
			// The primary reason is that it can't forcibly stop the execution of arbitrary code.
			// The StartToClose timeout in Temporal merely indicates to the Temporal service how long the activity is allowed to execute
			// before it's considered failed due to timeout.
			log.Printf(">> >> [Attempt: %d] Executing rule of ID %d failed due to: %s",
				currentAttempt, i, ctx.Err().Error())
			return fmt.Sprintf("Failed for [%d, %d)", ruleStartInclusive, ruleEndExclusive), ctx.Err()
		}

		if randomNumber := rand.Intn(100); randomNumber < 1 {
			log.Printf(">> >> [Attempt: %d] Executing rule of ID %d with code version %d",
				currentAttempt, i, CodeVersion)
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

		log.Printf(">> Activity execution completed with status: %s, error: %s\n", taskStatus, taskError.Error())
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
