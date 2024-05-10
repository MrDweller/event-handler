package eventhandler

import (
	"errors"
	"fmt"
	"log"

	"github.com/MrDweller/event-handler/types"
	"github.com/MrDweller/service-registry-connection/models"
	serviceregistry "github.com/MrDweller/service-registry-connection/service-registry"
	"github.com/google/uuid"
)

/*
Event handler, is a system that can Publish events to an implemented messaging broker.
*/
type EventHandler interface {
	// Interfaces that the event handler implementation uses.
	getInterfaces() []string
	// Metadata that the event handler implementation needs, in order to function.
	getMetadata(eventService EventService) map[string]string

	// Sends a given event to the messaging broker, the routing of the messages are defined by the used event handler implementation and the given event service
	PublishEvent(event types.Event, eventService EventService) error

	registerEventPublisherSystem() error

	// Removes the event handler system and all it's services from the service registry,
	// Should be called on shutdown of the system, or when the event handler is no longer to be used.
	UnregisterEventPublisherSystem() error

	RegisterEventService(eventType types.EventType) (EventService, error)
	UnregisterEventService(eventService EventService) error

	getSystemDefinition() models.SystemDefinition
}

/*
Creates a new event handler system of the given implementation eventHandlerImplementation.
The event handler will automatically on creation be registered to the service registry.
*/
func EventPublisherFactory(
	eventHandlerImplementation types.EventHandlerImplementationType,
	eventHandlerAddress string,
	eventHandlerPort int,
	eventHandlerDomainAddress string,
	eventHandlerDomainPort int,
	systemName string,
	serviceRegistryAddress string,
	serviceRegistryPort int,
	serviceRegistryImplementation serviceregistry.ServiceRegistryImplementationType,
	certFilePath string,
	keyFilePath string,
	truststoreFilePath string,
) (EventHandler, error) {
	serviceRegistryConnection, err := serviceregistry.NewConnection(
		serviceregistry.ServiceRegistry{
			Address: serviceRegistryAddress,
			Port:    serviceRegistryPort,
		},
		serviceRegistryImplementation,
		models.CertificateInfo{
			CertFilePath: certFilePath,
			KeyFilePath:  keyFilePath,
			Truststore:   truststoreFilePath,
		},
	)
	if err != nil {
		return nil, err
	}

	var eventHandler EventHandler
	eventHandlerBase := AbstractEventHandler{
		eventHandlerAddress:       eventHandlerAddress,
		eventHandlerPort:          eventHandlerPort,
		eventHandlerDomainAddress: eventHandlerDomainAddress,
		eventHandlerDomainPort:    eventHandlerDomainPort,
		systemName:                systemName,
		serviceRegistryConnection: serviceRegistryConnection,
	}

	switch eventHandlerImplementation {
	case types.RABBITMQ_3_12_12_EVENT_HANDLER:
		eventHandler = &RabbitmqEventHandler{
			AbstractEventHandler: &eventHandlerBase,
		}

	default:
		errorString := fmt.Sprintf("there is no %s event handler implementation", eventHandlerImplementation)
		return nil, errors.New(errorString)
	}

	eventHandlerBase.EventHandler = eventHandler
	err = eventHandler.registerEventPublisherSystem()
	if err != nil {
		log.Println(err)
	}
	return eventHandler, nil

}

/*
Base class for event handlers, implementing all service registry functionalities.
The abstract class does not implement PublishEvent, getInterfaces, and getMetadata, they must be implemented by a sub class.
*/
type AbstractEventHandler struct {
	EventHandler
	eventHandlerAddress       string
	eventHandlerPort          int
	eventHandlerDomainAddress string
	eventHandlerDomainPort    int
	systemName                string
	serviceRegistryConnection serviceregistry.ServiceRegistryConnection
}

func (e *AbstractEventHandler) registerEventPublisherSystem() error {
	_, err := e.serviceRegistryConnection.RegisterSystem(e.getSystemDefinition())
	if err != nil {
		return err
	}
	return nil
}

func (e *AbstractEventHandler) UnregisterEventPublisherSystem() error {
	return e.serviceRegistryConnection.UnRegisterSystem(e.getSystemDefinition())
}

func (e *AbstractEventHandler) RegisterEventService(eventType types.EventType) (EventService, error) {
	eventService := &EventServiceImplementation{
		serviceId:    uuid.New(),
		eventType:    eventType,
		eventHandler: e,
	}
	_, err := e.serviceRegistryConnection.RegisterService(
		eventService.getServiceDefinition(),
		e.getInterfaces(),
		e.getMetadata(eventService),
		e.getSystemDefinition(),
	)
	if err != nil {
		return nil, fmt.Errorf("error during registration of event handler service to the serviceregistry: %s", err)
	}

	return eventService, nil
}

func (e *AbstractEventHandler) UnregisterEventService(eventService EventService) error {
	return e.serviceRegistryConnection.UnRegisterService(
		eventService.getServiceDefinition(),
		e.getSystemDefinition(),
	)
}

func (e *AbstractEventHandler) getSystemDefinition() models.SystemDefinition {
	return models.SystemDefinition{
		Address:    e.eventHandlerDomainAddress,
		Port:       e.eventHandlerDomainPort,
		SystemName: e.systemName,
	}
}
