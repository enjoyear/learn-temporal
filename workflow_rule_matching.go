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

type PostRuleMatchingTaskReturn struct {
	Status            string
	RuleHighWatermark int64
}

const TaskProcessBatchSize = 10

func PostRuleMatchingTask(ctx context.Context, post BulletinBoardPost,
	ruleStartInclusive int64, ruleEndExclusive int64) (PostRuleMatchingTaskReturn, error) {
	info := activity.GetInfo(ctx)
	currentAttempt := info.Attempt

	if currentAttempt > 1 {
		// TODO: send metrics for retries
	}

	log.Printf(">> >> [Attempt: %d] Executing PostRuleMatchingTask for Post %s with rules [%d, %d)",
		currentAttempt, post.Title, ruleStartInclusive, ruleEndExclusive)

	var watermark int64
	for i := ruleStartInclusive; i < ruleEndExclusive; i += 1 {
		if i%TaskProcessBatchSize == 0 {
			watermark = i // Rule "I" hasn't been processed yet. The next retry should start from "I".

			// It's inefficient to check ctx.Err() very frequently
			if ctx.Err() != nil {
				// Temporal does not automatically stop the execution of an activity function even when it has reached StartToClose timeout.
				// The primary reason is that it can't forcibly stop the execution of arbitrary code.
				// The StartToClose timeout in Temporal merely indicates to the Temporal service how long the activity is allowed to execute
				// before it's considered failed due to timeout.
				log.Printf(">> >> [Attempt: %d] Stop executing from rule ID %d due to: %s",
					currentAttempt, i, ctx.Err().Error())
				return PostRuleMatchingTaskReturn{
					RuleHighWatermark: watermark,
					Status:            fmt.Sprintf("Failed for [%d, %d)", ruleStartInclusive, ruleEndExclusive),
				}, ctx.Err()
			}
		}

		// ToDo: implement actually processing logic here

		// simulate random failure
		if randomNumber := rand.Intn(100); randomNumber < 1 {
			log.Printf(">> >> [Attempt: %d] Executing rule of ID %d", currentAttempt, i)
			time.Sleep(5 * time.Second)
		}
	}

	return PostRuleMatchingTaskReturn{
		RuleHighWatermark: ruleEndExclusive,
		Status:            fmt.Sprintf("Done for [%d, %d)", ruleStartInclusive, ruleEndExclusive),
	}, nil
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

		var taskReturn PostRuleMatchingTaskReturn
		taskError := workflow.ExecuteActivity(ctx, PostRuleMatchingTask, input, ruleStart, ruleEnd).Get(ctx, &taskReturn)

		if taskError == nil {
			log.Printf(">> Activity execution completed with status: %v\n", taskReturn)
		} else {
			log.Printf(">> Activity execution failed with error: %s\n", taskError.Error())
			return taskReturn.Status, taskError
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
