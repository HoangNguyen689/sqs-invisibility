package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	queueURL = "YOUR_SQS_FIFO_QUEUE_URL"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("YOUR_AWS_REGION"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := sqs.NewFromConfig(cfg)

	// Step 1: Send message 1 to SQS FIFO
	sendMessage(client, "Message 1", "msg-group-1", "msg-dedup-1")

	// Step 2: Wait 10 seconds
	time.Sleep(10 * time.Second)

	// Step 3: Send message 2 to SQS FIFO
	sendMessage(client, "Message 2", "msg-group-1", "msg-dedup-2")

	// Step 4: Wait 10 seconds
	time.Sleep(10 * time.Second)

	// Step 5: Receive SQS messages
	messages := receiveMessages(client)

	fmt.Printf("Received messages: %+v\n", messages)

	// Step 6: Set message 2 visibility to 10 seconds
	setMessageVisibility(client, messages[1], 10)

	// Step 7: Set message 1 visibility to 15 seconds
	setMessageVisibility(client, messages[0], 15)

	// Step 8: Wait 11 seconds
	time.Sleep(11 * time.Second)

	// Step 9: Receive SQS messages
	messages = receiveMessages(client)

	// Step 10: Check message available and order
	fmt.Println("Received messages after 11 seconds:")
	for _, msg := range messages {
		fmt.Printf("Message ID: %s, Body: %s\n", *msg.MessageId, *msg.Body)
	}

	// Step 11: Wait 5 seconds
	time.Sleep(5 * time.Second)

	// Step 12: Check message available and order
	messages = receiveMessages(client)
	fmt.Println("Received messages after additional 5 seconds:")
	for _, msg := range messages {
		fmt.Printf("Message ID: %s, Body: %s\n", *msg.MessageId, *msg.Body)
	}
}

func sendMessage(client *sqs.Client, body, groupID, dedupID string) {
	_, err := client.SendMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody:            aws.String(body),
		QueueUrl:               aws.String(queueURL),
		MessageGroupId:         aws.String(groupID),
		MessageDeduplicationId: aws.String(dedupID),
	})
	if err != nil {
		log.Fatalf("failed to send message, %v", err)
	}
	fmt.Printf("Sent message: %s\n", body)
}

func receiveMessages(client *sqs.Client) []types.Message {
	resp, err := client.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 2,
		WaitTimeSeconds:     10,
	})
	if err != nil {
		log.Fatalf("failed to receive messages, %v", err)
	}
	return resp.Messages
}

func setMessageVisibility(client *sqs.Client, message types.Message, visibilityTimeout int32) {
	_, err := client.ChangeMessageVisibility(context.TODO(), &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(queueURL),
		ReceiptHandle:     message.ReceiptHandle,
		VisibilityTimeout: visibilityTimeout,
	})
	if err != nil {
		log.Fatalf("failed to change message visibility, %v", err)
	}
	fmt.Printf("Changed visibility timeout for message %s to %d seconds\n", *message.MessageId, visibilityTimeout)
}
