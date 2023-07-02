package app

// @@@SNIPSTART learn-temporal-shared-task-queue
const MoneyTransferTaskQueueName = "TRANSFER_MONEY_TASK_QUEUE"

// @@@SNIPEND

// @@@SNIPSTART learn-temporal-transferdetails
type PaymentDetails struct {
	SourceAccount string
	TargetAccount string
	Amount        int
	ReferenceID   string
}

// @@@SNIPEND
