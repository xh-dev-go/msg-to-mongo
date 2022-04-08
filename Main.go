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
	amqpConn, amqpCh := getAMQPConn(amqpUrl)
	defer amqpCh.Close()
	defer amqpConn.Close()
	session, err := client.StartSession()
	if err != nil {
		panic(err)
	}
	defer session.EndSession(context.Background())

	db := client.Database(dbName)
	outboxCollection := db.Collection(outboxCollectionName)

	err = amqpCh.Publish(exchange, key, false, false, amqp.Publishing{
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

const VERSION = "1.0.2"

var resetChannel = true

func getAMQPConn(urlParam string) (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial(urlParam)

	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	return conn, ch
}

func getMongoClient(mongoUrlParam string) (*mongo.Client, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoUrlParam))
	if err != nil {
		return nil, err
	}
	return client, nil
}

func main() {
	fmt.Println("Version: " + VERSION)
	urlParam := flagString.New("amqp-url", "The connection string of amqp").BindCmd()
	queueNameParam := flagString.New("queue-name", "The name of queue").BindCmd()
	mongoUrlParam := flagString.New("mongo-url", "The connection string of mongodb").BindCmd()
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
	println(mongoUrlParam.Value())
	println(mongoDBParam.Value())
	println(dataCollectionParam.Value())
	println(outboxCollectionParam.Value())
	println(splittingParam.Value())
	exchangeName := fmt.Sprintf("e-%v-ob", queueNameParam.Value())
	println(exchangeName)

	forever := make(chan bool)

	chNewMsg := make(chan string)
	ticker := time.NewTicker(time.Minute)

	go func() {
		defer func() {
			log.Println("Exit outbox processing")
		}()
		for {
			select {
			case outboxKey := <-chNewMsg:
				log.Printf("delete outbox %v\n", outboxKey)
				client, err := getMongoClient(mongoUrlParam.Value())
				if err != nil {
					log.Println("Error creating client")
					log.Println(err.Error())
					time.Sleep(time.Second)
					continue
				}
				err = sendOutboxMsg(client, outboxKey, mongoDBParam.Value(), outboxCollectionParam.Value(), urlParam.Value(), exchangeName, "")
				if err != nil {
					log.Printf("Fail delete outbox: %v\n", outboxKey)
					log.Println(err.Error())
				}
			case <-ticker.C:
				log.Println("delete outbox batch")
				client, err := getMongoClient(mongoUrlParam.Value())
				if err != nil {
					log.Println("Error creating client")
					log.Println(err.Error())
					time.Sleep(time.Second)
					continue
				}
				for _, outboxKey := range findAllOutboxItems(client, mongoDBParam.Value(), "outbox") {
					err := sendOutboxMsg(client, outboxKey, mongoDBParam.Value(), outboxCollectionParam.Value(), urlParam.Value(), exchangeName, "")
					if err != nil {
						log.Printf("delete outbox %v\n", outboxKey)
						log.Println(err.Error())
					}
				}

			}
		}
	}()

	go func() {
		defer func() {
			log.Println("Exit amqp process")
		}()
		for {
			log.Println("Reset connection")
			_, ch := getAMQPConn(urlParam.Value())
			client, err := getMongoClient(mongoUrlParam.Value())
			msgs, err := ch.Consume(
				queueNameParam.Value(), // queue
				"",                     // consumer
				false,                  // auto-ack
				false,                  // exclusive
				false,                  // no-local
				false,                  // no-wait
				nil,                    // args
			)
			for d := range msgs {
				log.Println("Process message: " + d.MessageId)
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
					log.Println("Fail to process message")
					log.Println(err.Error())
					err := ch.Reject(d.DeliveryTag, false)
					if err != nil {
						log.Println("Fail ack")
						log.Println(err.Error())
						break
					} else {
						log.Println("success rollback")
					}
				} else {
					fmt.Println("[DONE]Process message: " + d.MessageId)
					err := ch.Ack(d.DeliveryTag, false)
					if err != nil {
						log.Println("Fail reject")
						log.Println(err.Error())
						break
					} else {
						log.Println("Success in ack")
					}
				}
			}
		}
	}()

	<-forever
}
