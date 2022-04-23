package main

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

type MongoWrapper struct {
	url    string
	cleint *mongo.Client
}

func (wrapper *MongoWrapper) setUrl(url string) {
	wrapper.url = url
}

func (wrapper *MongoWrapper) getConnection() (*mongo.Client, error) {
	createConnection := func() (*mongo.Client, error) {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		log.Println("Create new connection")
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(wrapper.url))
		if err != nil {
			return nil, err
		}
		wrapper.cleint = client
		return wrapper.cleint, nil
	}

	if wrapper.cleint == nil {
		return createConnection()
	} else {
		return wrapper.cleint, nil
	}
}
