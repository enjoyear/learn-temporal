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
	PostId            int64
	Title             string
	Body              string
	PublisherPlatform string
	NotificationType  string
}

type PostRuleMatchingActivityReturn struct {
	Key      MatchingTaskKey
	Progress MatchingTaskProgress
	Status   string
}

const TaskProcessBatchSize = 10

func SplitRulesActivity(ctx context.Context, postId int64,
	ruleStartInclusive int64, ruleEndExclusive int64, ruleSplitSize int64) ([]MatchingTaskKey, error) {
	info := activity.GetInfo(ctx)
	currentAttempt := info.Attempt
	if currentAttempt > 1 {
		// TODO: send metrics for retries
	}

	dbClient := DBClient{}

	log.Printf(">> >> [Attempt: %d] Splitting rules for the range [%d, %d) with partition size %d",
		currentAttempt, ruleStartInclusive, ruleEndExclusive, ruleSplitSize)

	tasks := make(map[MatchingTaskKey]MatchingTaskProgress)
	ranges := make([]MatchingTaskKey, 0)

	for i := ruleStartInclusive; i < ruleEndExclusive; i += ruleSplitSize {
		ruleStart := i
		ruleEnd := min(ruleEndExclusive, ruleStart+ruleSplitSize)

		taskKey := MatchingTaskKey{
			PostId:             postId,
			RuleStartInclusive: ruleStart,
			RuleEndExclusive:   ruleEnd,
		}

		now := time.Now().UTC().UnixMilli()
		tasks[taskKey] = MatchingTaskProgress{
			TaskKey:       taskKey,
			HighWatermark: ruleStart,
			CreatedAt:     now,
			UpdatedAt:     now,
		}

		ranges = append(ranges, taskKey)
	}

	dbClient.saveTasks(tasks)
	log.Printf(">> >> [Attempt: %d] Rule splits are: %v", currentAttempt, ranges)
	return ranges, nil
}

// PostRuleMatchingActivity executes a stateful Activity where the state is kept in the DB
func PostRuleMatchingActivity(ctx context.Context, post BulletinBoardPost, taskKey MatchingTaskKey,
) (PostRuleMatchingActivityReturn, error) {
	info := activity.GetInfo(ctx)
	currentAttempt := info.Attempt
	if currentAttempt > 1 {
		// TODO: send metrics for retries
	}

	dbClient := DBClient{}
	ruleEndExclusive := taskKey.RuleEndExclusive
	currentProgress := dbClient.getProgress(taskKey)
	ruleStartInclusive := currentProgress.HighWatermark

	log.Printf(">> >> [Key: %v, Attempt: %d] Executing PostRuleMatchingActivity for Post %s from %d",
		taskKey, currentAttempt, post.Title, ruleStartInclusive)

	for i := ruleStartInclusive; i < ruleEndExclusive; i += 1 {
		if i%TaskProcessBatchSize == 0 {
			// Update activity state in DB
			// Rule "I" hasn't been processed yet. If failed, the next retry should start from "I".
			// This is the high watermark we need to track for current activity's rule range
			progress := dbClient.updateProgress(taskKey, i)

			// It's inefficient to check ctx.Err() very frequently
			if ctx.Err() != nil {
				// Temporal does not automatically stop the execution of an activity function even when it has reached StartToClose timeout.
				// The primary reason is that it can't forcibly stop the execution of arbitrary code.
				// The StartToClose timeout in Temporal merely indicates to the Temporal service how long the activity is allowed to execute
				// before it's considered failed due to timeout.
				log.Printf(">> >> [Key: %v, Attempt: %d] Stopped execution from rule ID %d due to: %s",
					taskKey, currentAttempt, i, ctx.Err().Error())
				activityReturn := PostRuleMatchingActivityReturn{
					Key:      taskKey,
					Progress: progress,
					Status:   "Fail",
				}
				return activityReturn, ctx.Err()
			}
		}

		// ToDo: implement actually processing logic here

		// simulate matching work
		if randomNumber := rand.Intn(100); randomNumber < 1 {
			log.Printf(">> >> [Key: %v, Attempt: %d] Executing rule of ID %d", taskKey, currentAttempt, i)
			time.Sleep(5 * time.Second)
		}
	}

	dbClient.completeTask(taskKey)

	return PostRuleMatchingActivityReturn{
		Key:    taskKey,
		Status: "Success",
	}, nil
}

type MatchingTaskKey struct {
	PostId             int64
	RuleStartInclusive int64
	RuleEndExclusive   int64
}

func (k MatchingTaskKey) String() string {
	return fmt.Sprintf("Matching %d within [%d, %d)", k.PostId, k.RuleStartInclusive, k.RuleEndExclusive)
}
func (k MatchingTaskKey) getRangeString() string {
	return fmt.Sprintf("[%d, %d)", k.RuleStartInclusive, k.RuleEndExclusive)
}

type MatchingTaskProgress struct {
	TaskKey       MatchingTaskKey
	HighWatermark int64 // the high watermark is exclusive
	CreatedAt     int64
	UpdatedAt     int64
}

func (rrp MatchingTaskProgress) String() string {
	return fmt.Sprintf("High watermark %d for %v", rrp.HighWatermark, rrp.TaskKey)
}

// mock the rows of a Postgres table
var mockTasksTable map[MatchingTaskKey]MatchingTaskProgress

type DBClient struct {
}

// saveTasks persists new tasks to DB
func (c DBClient) saveTasks(tasks map[MatchingTaskKey]MatchingTaskProgress) {
	mockTasksTable = tasks
}

func (c DBClient) getProgress(key MatchingTaskKey) MatchingTaskProgress {
	return mockTasksTable[key]
}

func (c DBClient) updateProgress(key MatchingTaskKey, highWatermark int64) MatchingTaskProgress {
	progress := mockTasksTable[key]
	progress.HighWatermark = highWatermark
	progress.UpdatedAt = time.Now().UTC().UnixMilli()
	mockTasksTable[key] = progress
	return progress
}

func (c DBClient) completeTask(key MatchingTaskKey) {
	delete(mockTasksTable, key)
}

func PostRuleMatching(ctx workflow.Context, input BulletinBoardPost,
	ruleStartInclusive int64, ruleEndExclusive int64, ruleSplitSize int64) (string, error) {

	log.Printf(">> Workflow starts")
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2, // at most 2 runs, allowing retry for 1 time
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	var ruleSplits []MatchingTaskKey
	taskError := workflow.ExecuteActivity(ctx, SplitRulesActivity, input.PostId,
		ruleStartInclusive, ruleEndExclusive, ruleSplitSize).Get(ctx, &ruleSplits)
	if taskError != nil {
		return "SplitRulesActivity failed", taskError
	}
	log.Printf(">> Task splits are: %v", ruleSplits)

	matchingActivityFutures := make([]workflow.Future, 0)
	for _, task := range ruleSplits {
		log.Printf(">> Submit matching for Post %s with rules %s", input.Title, task.getRangeString())
		matchingActivityFutures = append(matchingActivityFutures,
			workflow.ExecuteActivity(ctx, PostRuleMatchingActivity, input, task))
	}

	var matchingActivityFailures = make(map[MatchingTaskKey]error)
	for _, future := range matchingActivityFutures {
		var taskReturn PostRuleMatchingActivityReturn
		taskError := future.Get(ctx, &taskReturn)

		if taskError == nil {
			log.Printf(">> Activity execution completed successfully: %v", taskReturn)
		} else {
			log.Printf(">> Activity execution failed: %v", taskReturn)
			matchingActivityFailures[taskReturn.Key] = taskError
		}
	}

	if len(matchingActivityFailures) > 0 {
		for key, err := range matchingActivityFailures {
			log.Printf(">> %v failed with error: %s", key, err.Error())
		}
		return "Failed", fmt.Errorf("%d rule matching activities failed", len(matchingActivityFailures))
	}

	log.Printf(">> Workflow completes. Unfinished tasks are: %v", mockTasksTable)
	return "Success", nil
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
