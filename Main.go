package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/xh-dev-go/xhUtils/flagUtils"
	"github.com/xh-dev-go/xhUtils/flagUtils/flagBool"
	"github.com/xh-dev-go/xhUtils/flagUtils/flagString"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"log"
	"os"
	"strings"
	"time"
)

type Outbox struct {
	Datetime string `bson:"datetime"`
}

func findAllOutboxItems(client *mongo.Client, dbName, collection string) []string {
	session, err := client.StartSession()
	if err != nil {
		panic(err)
	}
	defer session.EndSession(context.Background())

	wc := writeconcern.New(writeconcern.WMajority())
	rc := readconcern.Snapshot()
	txnOpts := options.Transaction().SetWriteConcern(wc).SetReadConcern(rc)
	var arr []string
	_, err = session.WithTransaction(context.Background(), func(sessionContext mongo.SessionContext) (interface{}, error) {
		db := client.Database(dbName)
		cur, err := db.Collection(collection).Find(context.Background(), bson.D{})
		if err != nil {
			return nil, err
		}
		var record Outbox
		for cur.Next(context.Background()) {
			err = cur.Decode(&record)
			if err != nil {
				return nil, nil
			}
			arr = append(arr, record.Datetime)
		}
		return arr, nil
	}, txnOpts)
	return arr
}

func sendOutboxMsg(
	client *mongo.Client,
	outboxKey, dbName, outboxCollectionName string,
	amqpUrl string,
	exchange, key string,
) error {
	conn, err := amqp.Dial(amqpUrl)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	session, err := client.StartSession()
	if err != nil {
		panic(err)
	}
	defer session.EndSession(context.Background())

	db := client.Database(dbName)
	outboxCollection := db.Collection(outboxCollectionName)

	err = ch.Publish(exchange, key, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(outboxKey),
	})
	if err != nil {
		return err
	}

	wc := writeconcern.New(writeconcern.WMajority())
	rc := readconcern.Snapshot()
	txnOpts := options.Transaction().SetWriteConcern(wc).SetReadConcern(rc)
	_, err = session.WithTransaction(context.Background(), func(sessionContext mongo.SessionContext) (interface{}, error) {
		var err error
		res, err := outboxCollection.DeleteOne(sessionContext, bson.M{"datetime": outboxKey})
		if err != nil {
			return nil, err
		} else if res.DeletedCount > 1 {
			return nil, errors.New(fmt.Sprintf("delete count not match: %d", res.DeletedCount))
		} else {
			return nil, nil
		}
	}, txnOpts)

	if err != nil {
		return err
	}
	return nil
}

func insertData(
	client *mongo.Client, dbName, dataCollectionName, outboxCollectionName, data string,
	newMsg chan string,
	splitting bool,
) error {
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

	dateNow := time.Now().Format("20060102150405")
	_, err = session.WithTransaction(context.Background(), func(sessionContext mongo.SessionContext) (interface{}, error) {

		var err error
		if splitting {
			var arrMsg []string
			for _, m := range strings.Split(data, "\n") {
				arrMsg = append(arrMsg, strings.TrimRight(m, "\r"))
			}

			_, err = dataCollection.InsertOne(sessionContext, bson.M{"datetime": dateNow, "messages": arrMsg})
		} else {
			_, err = dataCollection.InsertOne(sessionContext, bson.M{"datetime": dateNow, "message": data})
		}

		if err != nil {
			return nil, err
		}
		result, err := outboxCollection.InsertOne(sessionContext, bson.M{"datetime": dateNow})
		if err != nil {
			return nil, err
		}
		return result, nil
	}, txnOpts)

	if err != nil {
		return err
	}
	newMsg <- dateNow

	return nil
}

const VERSION = "1.0.0-snapshot"

func main() {

	urlParam := flagString.New("amqp-url", "The connection string of amqp").BindCmd()
	queueNameParam := flagString.New("queue-name", "The name of queue").BindCmd()
	mongUrlParam := flagString.New("mongo-url", "The connection string of mongodb").BindCmd()
	mongoDBParam := flagString.New("mongo-db", "The name of mongo db").BindCmd()
	dataCollectionParam := flagString.New("data-collection", "The name of collection to store the message").BindCmd()
	outboxCollectionParam := flagString.New("outbox-collection", "The name of collection to store the outbox message").BindCmd()
	splittingParam := flagBool.New("splitting", "splitting message by new line").BindCmd()
	versionParam := flagUtils.Version().BindCmd()
	flag.Parse()
	if len(os.Args) == 1 {
		flag.PrintDefaults()
		os.Exit(0)
	}
	if versionParam.Value() {
		fmt.Println("1.0.1")
		os.Exit(0)
	}

	println("====")
	println(urlParam.Value())
	println(queueNameParam.Value())
	println(versionParam.Value())
	println(mongUrlParam.Value())
	println(mongoDBParam.Value())
	println(dataCollectionParam.Value())
	println(outboxCollectionParam.Value())
	println(splittingParam.Value())
	exchangeName := fmt.Sprintf("e-%v-ob", queueNameParam.Value())
	println(exchangeName)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongUrlParam.Value()))
	if err != nil {
		panic(err)
	}
	defer client.Disconnect(ctx)

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

	chNewMsg := make(chan string)
	ticker := time.NewTicker(time.Minute)
	sendOutboxMsg(client, "1234", mongUrlParam.Value(), outboxCollectionParam.Value(), urlParam.Value(), exchangeName, "")

	go func() {
		for {
			select {
			case outboxKey := <-chNewMsg:
				fmt.Printf("delete outbox %v\n", outboxKey)
				err := sendOutboxMsg(client, outboxKey, mongoDBParam.Value(), outboxCollectionParam.Value(), urlParam.Value(), exchangeName, "")
				if err != nil {
					fmt.Println(err.Error())
				}
			case <-ticker.C:
				for _, outboxKey := range findAllOutboxItems(client, mongoDBParam.Value(), "outbox") {
					fmt.Println("Batch delete")
					err := sendOutboxMsg(client, outboxKey, mongoDBParam.Value(), outboxCollectionParam.Value(), urlParam.Value(), exchangeName, "")
					if err != nil {
						fmt.Printf("delete outbox %v\n", outboxKey)
						fmt.Println(err.Error())
					}
				}

			}
		}
	}()

	go func() {
		for d := range msgs {
			fmt.Println("Process message: " + d.MessageId)
			err = insertData(
				client,
				mongoDBParam.Value(),
				dataCollectionParam.Value(),
				outboxCollectionParam.Value(),
				string(d.Body),
				chNewMsg,
				splittingParam.Value(),
			)
			if err != nil {
				log.Fatal(err, "Fail to process message")
				err := ch.Reject(d.DeliveryTag, false)
				if err != nil {
					fmt.Println(err.Error())
					fmt.Println("Fail act")
					return
				} else {
					fmt.Println("success rollback")
				}
			} else {
				fmt.Println("[DONE]Process message: " + d.MessageId)
				err := ch.Ack(d.DeliveryTag, false)
				if err != nil {
					fmt.Println("Fail reject")
					fmt.Println(err.Error())
					return
				} else {
					fmt.Println("Success in ack")
				}
			}

		}
	}()

	<-forever
}
