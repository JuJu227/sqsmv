package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type client struct {
	session  *sqs.Client
	bSession *s3.Client
	con      conf
}

type conf struct {
	source string
	destQ  string
	bucket string
}

func newClient(c conf) *client {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf(err.Error())
	}

	return &client{
		session:  sqs.NewFromConfig(cfg),
		bSession: s3.NewFromConfig(cfg),
		con:      c,
	}
}

func (c *client) pullMessages(ctx context.Context) ([]types.Message, error) {
	resp, err := c.session.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(c.con.source),
		MaxNumberOfMessages:   10,
		WaitTimeSeconds:       0,
		MessageAttributeNames: []string{"ALL"},
	})
	if err != nil {
		log.Println(err)
		return nil, err
	}

	log.Printf("received %v messages...", len(resp.Messages))
	return resp.Messages, nil
}

func (c *client) transferMessagesToBucket(ctx context.Context, ms []types.Message) error {
	by, err := json.Marshal(ms)
	if err != nil {
		return err
	}

	_, err = c.bSession.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.con.bucket),
		Key:    aws.String(fmt.Sprintf("%s-%s", c.con.source, time.Now().UTC().String())),
		Body:   bytes.NewReader(by),
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *client) purgeQueue(ctx context.Context, ms []types.Message) error {
	dmsg := make([]types.DeleteMessageBatchRequestEntry, 0, len(ms))
	for _, m := range ms {
		dmsg = append(dmsg, types.DeleteMessageBatchRequestEntry{
			Id:            m.MessageId,
			ReceiptHandle: m.ReceiptHandle,
		})
	}
	_, err := c.session.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		Entries:  dmsg,
		QueueUrl: aws.String(c.con.source),
	})
	return err
}

//transferMessages loops, transferring a number of messages from the src to the dest at an interval.
func (c *client) transferMessagesToQueue(ctx context.Context, ms []types.Message) error {
	lastMessageCount := len(ms)
	smsg := make([]types.SendMessageBatchRequestEntry, 0, lastMessageCount)

	//loading batch
	for _, m := range ms {
		smsg = append(smsg, types.SendMessageBatchRequestEntry{
			Id:          m.MessageId,
			MessageBody: m.Body,
		})

	}

	_, err := c.session.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		Entries:  smsg,
		QueueUrl: aws.String(c.con.destQ),
	})
	return err
}

func main() {
	var wg sync.WaitGroup

	src := flag.String("src", "", "source queue")
	dest := flag.String("dest", "", "destination queue")
	bucket := flag.String("bucket", "", "destination bucket")

	flag.Parse()

	if *src == "" || (*dest == "" && *bucket == "") {
		flag.Usage()
		os.Exit(1)
	}

	log.Printf("source queue : %s", *src)
	log.Printf("destination queue : %s", *dest)
	log.Printf("destination bucket : %s", *bucket)

	client := newClient(conf{
		source: *src,
		destQ:  *dest,
		bucket: *bucket,
	})

	ms, err := client.pullMessages(context.TODO())
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	if *dest != "" {
		wg.Add(1)
		go func() {
			log.Println("transfer Message to queue")
			defer wg.Done()
			err = client.transferMessagesToQueue(context.TODO(), ms)
			if err != nil {
				log.Println(err)
				os.Exit(1)
			}
		}()
	}

	if *bucket != "" {
		wg.Add(1)
		go func() {
			log.Println("transfer Message to bucket")
			defer wg.Done()
			err = client.transferMessagesToBucket(context.TODO(), ms)
			if err != nil {
				log.Println(err)
				os.Exit(1)
			}
		}()
	}

	wg.Wait()

	err = client.purgeQueue(context.TODO(), ms)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	log.Println("all done")
}
