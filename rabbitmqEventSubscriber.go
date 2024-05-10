package eventhandler

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitmqEventSubscriber, can subscribe for events given from an RabbitmqEventHandler
type RabbitmqEventSubscriber struct {

	// Extends a base event subscriber class, giving general event subscriber functionalities.
	// The function subscribe, must be implemented as AbstractEventSubscriber does not do so.
	*AbstractEventSubscriber

	stopSubscribingToEventTypeCannel map[SubscriptionKey]chan bool
}

func (r *RabbitmqEventSubscriber) subscribe(eventType EventType, eventHandlerAddress string, eventHandlerPort int, eventHandlerSystemName string, subscriptionKey SubscriptionKey, metaData map[string]string, receiver Receiver) error {
	url := fmt.Sprintf("%s:%d/", eventHandlerAddress, eventHandlerPort)
	dialAddrr := fmt.Sprintf("amqp://guest:guest@%s", url)
	conn, err := amqp.Dial(dialAddrr)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		metaData[EXCHANGE], // name
		"fanout",           // type
		true,               // durable
		false,              // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		q.Name,             // queue name
		"",                 // routing key
		metaData[EXCHANGE], // exchange
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			receiver.ReceiveEvent(d.Body)

		}
	}()

	r.stopSubscribingToEventTypeCannel[subscriptionKey] = make(chan bool)
	<-r.stopSubscribingToEventTypeCannel[subscriptionKey]
	delete(r.stopSubscribingToEventTypeCannel, subscriptionKey)
	return nil

}

/*
Overrides the super class's Unsubscribe method, and stops all connections before calling the super class's Unsubscribe.
*/
func (r *RabbitmqEventSubscriber) Unsubscribe(eventType EventType) error {
	for subscriptionKey, currentEventType := range r.getSubscriptions() {
		if currentEventType == eventType {
			r.stopSubscribingToEventTypeCannel[subscriptionKey] <- true

		}
	}
	err := r.AbstractEventSubscriber.Unsubscribe(eventType)
	if err != nil {
		return err
	}
	return nil
}
