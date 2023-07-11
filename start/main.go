package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"

	"learn-temporal/app"
)

func main() {
	// Create the client object just once per process
	c, err := client.Dial(client.Options{})

	if err != nil {
		log.Fatalln("Unable to create Temporal client:", err)
	}

	defer c.Close()

	we := executeWorkflowRuleMatching(err, c)

	log.Printf("WorkflowID: %s RunID: %s\n", we.GetID(), we.GetRunID())

	var result string

	err = we.Get(context.Background(), &result)

	if err != nil {
		log.Fatalln("Unable to get Workflow result:", err)
	}

	log.Println(result)
}

func executeWorkflowRuleMatching(err error, c client.Client) client.WorkflowRun {
	workflowId := "DBB-post-rule-matching"

	input := app.BulletinBoardPost{
		Title:             "PostTitle",
		Body:              "events table will be partitioned",
		PublisherPlatform: "DataBulletinBoard",
		NotificationType:  "PartitionTable",
	}

	options := client.StartWorkflowOptions{
		ID:        workflowId,
		TaskQueue: app.PostRuleMatchingTaskQueueName,
	}

	log.Printf("Start rule matching for %s", input)

	we, err := c.ExecuteWorkflow(context.Background(), options, app.PostRuleMatching, input)
	if err != nil {
		log.Fatalln("Unable to start the Workflow:", err)
	}
	return we
}
func executeWorkflowMoneyTransfer(err error, c client.Client) client.WorkflowRun {
	input := app.PaymentDetails{
		SourceAccount: "85-150",
		TargetAccount: "43-812",
		Amount:        250,
		ReferenceID:   "12345",
	}

	options := client.StartWorkflowOptions{
		ID:        "pay-invoice-701",
		TaskQueue: app.MoneyTransferTaskQueueName,
	}

	log.Printf("Starting transfer from account %s to account %s for %d", input.SourceAccount, input.TargetAccount, input.Amount)

	we, err := c.ExecuteWorkflow(context.Background(), options, app.MoneyTransfer, input)
	if err != nil {
		log.Fatalln("Unable to start the Workflow:", err)
	}
	return we
}
