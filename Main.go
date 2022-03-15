package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/xh-dev-go/xhUtils/flagUtils"
	"github.com/xh-dev-go/xhUtils/flagUtils/flagString"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"log"
	"time"
)

func insertData(mongoConnectionStr, dbName, dataCollectionName, outboxCollectionName, data string) error {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoConnectionStr))
	if err != nil {
		panic(err)
	}
	defer client.Disconnect(ctx)

	session, err := client.StartSession()
	if err != nil {
		panic(err)
	}
	defer session.EndSession(context.Background())

	db := client.Database(dbName)
	dataCollection := db.Collection(dataCollectionName)
	outboxCollection := db.Collection(outboxCollectionName)

	wc := writeconcern.New(writeconcern.WMajority())
	rc := readconcern.Snapshot()
	txnOpts := options.Transaction().SetWriteConcern(wc).SetReadConcern(rc)

	_, err = session.WithTransaction(context.Background(), func(sessionContext mongo.SessionContext) (interface{}, error) {
		dateNow := time.Now().Format("20060102150405")
		_, err = dataCollection.InsertOne(sessionContext, bson.M{"datetime": dateNow, "message": data})

		if err != nil {
			return nil, err
		}
		result, err := outboxCollection.InsertOne(sessionContext, bson.M{"datetime": dateNow})
		if err != nil {
			return nil, err
		}

		fmt.Println("Committed insertion")
		return result, nil
	}, txnOpts)

	if err != nil {
		fmt.Println("Rollback insertion")
		return err
	}
	return nil
}

func main() {

	urlParam := flagString.New("amqp-url", "The connection string of amqp").BindCmd()
	queueNameParam := flagString.New("queue-name", "The name of queue").BindCmd()
	mongUrlParam := flagString.New("mongo-url", "The connection string of mongodb").BindCmd()
	mongoDBParam := flagString.New("mongo-db", "The name of mongo db").BindCmd()
	dataCollectionParam := flagString.New("data-collection", "The name of collection to store the message").BindCmd()
	outboxCollectionParam := flagString.New("outbox-collection", "The name of collection to store the outbox message").BindCmd()
	versionParam := flagUtils.Version().BindCmd()
	flag.Parse()

	println("====")
	println(urlParam.Value())
	println(queueNameParam.Value())
	println(versionParam.Value())
	println(mongUrlParam.Value())
	println(mongoDBParam.Value())
	println(dataCollectionParam.Value())
	println(outboxCollectionParam.Value())

	conn, err := amqp.Dial(urlParam.Value())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	forever := make(chan bool)

	msgs, err := ch.Consume(
		queueNameParam.Value(), // queue
		"",                     // consumer
		false,                  // auto-ack
		false,                  // exclusive
		false,                  // no-local
		false,                  // no-wait
		nil,                    // args
	)

	go func() {
		for d := range msgs {
			fmt.Println("Process message: " + d.MessageId)
			err = insertData(mongUrlParam.Value(), mongoDBParam.Value(), dataCollectionParam.Value(), outboxCollectionParam.Value(), string(d.Body))
			if err != nil {
				log.Fatal(err, "Fail to process message")
				err := ch.Reject(d.DeliveryTag, true)
				if err != nil {
					fmt.Println("Fail act")
					return
				}
			} else {
				fmt.Println("[DONE]Process message: " + d.MessageId)
				err := ch.Ack(d.DeliveryTag, true)
				if err != nil {
					fmt.Println("Fail reject")
					return
				}
			}

		}
	}()

	<-forever
}
