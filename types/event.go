package types

type EventHandlerImplementationType string

type EventType string

/*
Events are sent to the even handler system, to be published to all subscribers.
Events are written on basis on what is to be sent, they require a method that defines the eventType and the data to be sent.

The data must conform to json (map[string]interface{})
*/
type Event interface {
	GetEventType() EventType
	GetEventData() map[string]interface{}
}
