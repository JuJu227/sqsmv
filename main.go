package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type client struct {
	session *sqs.Client
	rmi     *sqs.ReceiveMessageInput
	dest    string
}

func newClient(src string, dest string) *client {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf(err.Error())
	}

	return &client{
		session: sqs.NewFromConfig(cfg),
		rmi: &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(src),
			MaxNumberOfMessages:   10,
			WaitTimeSeconds:       0,
			MessageAttributeNames: []string{"ALL"},
		},
		dest: dest,
	}
}

//transferMessages loops, transferring a number of messages from the src to the dest at an interval.
func (c *client) transferMessages(ctx context.Context) error {
	resp, err := c.session.ReceiveMessage(ctx, c.rmi)
	if err != nil {
		log.Println(err)
		return err
	}

	lastMessageCount := len(resp.Messages)
	log.Printf("received %v messages...", len(resp.Messages))

	smsg := make([]types.SendMessageBatchRequestEntry, 0, lastMessageCount)
	dmsg := make([]types.DeleteMessageBatchRequestEntry, 0, lastMessageCount)

	//loading batch
	for _, m := range resp.Messages {
		smsg = append(smsg, types.SendMessageBatchRequestEntry{
			Id:          m.MessageId,
			MessageBody: m.Body,
		})

		dmsg = append(dmsg, types.DeleteMessageBatchRequestEntry{
			Id:            m.MessageId,
			ReceiptHandle: m.ReceiptHandle,
		})
	}

	_, err = c.session.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		Entries:  smsg,
		QueueUrl: aws.String(c.dest),
	})
	if err != nil {
		log.Println(err.Error())
		return err
	}

	_, err = c.session.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		Entries:  dmsg,
		QueueUrl: c.rmi.QueueUrl,
	})
	return err
}

func main() {
	src := flag.String("src", "", "source queue")
	dest := flag.String("dest", "", "destination queue")
	flag.Parse()

	if *src == "" || *dest == "" {
		flag.Usage()
		os.Exit(1)
	}

	log.Printf("source queue : %v", *src)
	log.Printf("destination queue : %v", *dest)

	client := newClient(*src, *dest)

	client.transferMessages(context.TODO())
	log.Println("all done")
}
