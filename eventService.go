package eventhandler

import (
	"github.com/MrDweller/service-registry-connection/models"
	"github.com/google/uuid"
)

type EventService interface {
	GetEventServiceId() uuid.UUID
	GetEventType() EventType
	PublishEvent(event Event) error
	UnregisterEventService() error
	getServiceDefinition() models.ServiceDefinition
}

type EventServiceImplementation struct {
	serviceId    uuid.UUID
	eventType    EventType
	eventHandler EventHandler
}

func (e *EventServiceImplementation) GetEventServiceId() uuid.UUID {
	return e.serviceId
}

func (e *EventServiceImplementation) GetEventType() EventType {
	return e.eventType
}

func (e *EventServiceImplementation) PublishEvent(event Event) error {
	return e.eventHandler.PublishEvent(event, e)
}

func (e *EventServiceImplementation) getServiceDefinition() models.ServiceDefinition {
	return models.ServiceDefinition{
		ServiceDefinition: string(e.GetEventType()),
		ServiceUri:        "",
	}
}

func (e *EventServiceImplementation) UnregisterEventService() error {
	return e.eventHandler.UnregisterEventService(e)
}
