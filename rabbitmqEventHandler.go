package eventhandler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const EXCHANGE = "exchange"

const RABBITMQ_3_12_12_EVENT_HANDLER EventHandlerImplementationType = "rabbitmq-3.12.12"

// RabbitMQ event handler, publishes messages via AMQP
type RabbitmqEventHandler struct {

	// Extends a base event handler class, giving general event handler functionalities.
	// The functions PublishEvent, getInterfaces, and getMetadata, must be implemented as AbstractEventHandler does not do so.
	*AbstractEventHandler
}

func (r *RabbitmqEventHandler) PublishEvent(event Event, eventService EventService) error {
	err := r.emit(event, r.getMetadata(eventService)[EXCHANGE])
	return err
}

func (r *RabbitmqEventHandler) getInterfaces() []string {
	return []string{
		"AMQP-INSECURE-JSON", // RabbitMQ uses AMQP, and this implementation uses an unsecure version.
	}
}

func (r *RabbitmqEventHandler) getMetadata(eventService EventService) map[string]string {
	return map[string]string{
		EXCHANGE: fmt.Sprintf("%s-%s", eventService.GetEventType(), eventService.GetEventServiceId().String()), // This implementation needs a specific exchange to route the messages, thus this is given in the metadata field, so consumers can connect.
	}
}

func (r *RabbitmqEventHandler) emit(event Event, exchange string) error {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%d/", r.eventHandlerAddress, r.eventHandlerPort))
	if err != nil {
		log.Printf("%s: %s", "Failed to connect to RabbitMQ", err)
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("%s: %s", "Failed to open a channel", err)
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchange, // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		log.Printf("%s: %s", "Failed to declare an exchange", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data, err := json.Marshal(event.GetEventData())
	if err != nil {
		log.Printf("%s: %s", "Failed to marshal the event", err)
		return err
	}
	err = ch.PublishWithContext(ctx,
		exchange, // exchange
		"",       // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data,
		},
	)

	if err != nil {
		log.Printf("%s: %s", "Failed to publish a message", err)
		return err
	}

	log.Printf("sent %s", event.GetEventType())
	return nil
}
