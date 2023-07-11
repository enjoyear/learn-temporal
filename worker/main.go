package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"learn-temporal/app"
)

func main() {

	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create Temporal client.", err)
	}
	defer c.Close()

	w := createPostRuleMatchingWorker(c)

	// Start listening to the Task Queue.
	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("unable to start Worker", err)
	}
}

func createPostRuleMatchingWorker(c client.Client) worker.Worker {
	w := worker.New(c, app.PostRuleMatchingTaskQueueName, worker.Options{})

	// This worker hosts both Workflow and Activity functions.
	w.RegisterWorkflow(app.PostRuleMatching)
	w.RegisterActivity(app.PostRuleMatchingTask)
	return w
}

func createMoneyTransferWorker(c client.Client) worker.Worker {
	w := worker.New(c, app.MoneyTransferTaskQueueName, worker.Options{})

	// This worker hosts both Workflow and Activity functions.
	w.RegisterWorkflow(app.MoneyTransfer)
	w.RegisterActivity(app.Withdraw)
	w.RegisterActivity(app.Deposit)
	w.RegisterActivity(app.Refund)
	return w
}
