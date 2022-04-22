package main

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
)

type MQWrapper struct {
	url  string
	conn *amqp.Connection
}

func (wrapper *MQWrapper) setUrl(url string) {
	wrapper.url = url
}

func (wrapper *MQWrapper) getConn() error {
	if wrapper.conn == nil {
		log.Println("Create new connect")
		conn, err := amqp.Dial(wrapper.url)
		if err != nil {
			log.Println("Fail to create new connection")
			return err
		}
		wrapper.conn = conn
	} else if wrapper.conn.IsClosed() {
		log.Println("Create new connect due to closed conn")
		conn, err := amqp.Dial(wrapper.url)
		if err != nil {
			log.Println("Fail to create new connection")
			return err
		}
		wrapper.conn = conn
	}
	return nil
}
func (wrapper *MQWrapper) ready() error {
	if wrapper.conn == nil {
		log.Println("No connection set")
		return errors.New("no connection set")
	} else if wrapper.conn.IsClosed() {
		return errors.New("connection is closed")
	}
	return nil
}

func (wrapper *MQWrapper) getChannel() (*amqp.Channel, error) {
	if err := wrapper.ready(); err != nil {
		return nil, err
	} else {
		return wrapper.conn.Channel()
	}
}
