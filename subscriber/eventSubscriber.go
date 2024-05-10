package eventsubscriber

import (
	"errors"
	"fmt"
	"log"

	"github.com/MrDweller/event-handler/types"
	orchestratormodels "github.com/MrDweller/orchestrator-connection/models"
	"github.com/MrDweller/orchestrator-connection/orchestrator"
	"github.com/MrDweller/service-registry-connection/models"
	serviceregistry "github.com/MrDweller/service-registry-connection/service-registry"
)

/*
Event subscriber, is a system that can subscribe for events from a certain messaging broker.
*/
type EventSubscriber interface {
	subscribe(eventType types.EventType, eventHandlerAddress string, eventHandlerPort int, eventHandlerSystemName string, subscriptionKey SubscriptionKey, metaData map[string]string, receiver Receiver) error

	registerEventSubscriberSystem() error
	UnregisterEventSubscriberSystem() error

	Subscribe(eventType types.EventType, receiver Receiver) error
	Unsubscribe(eventType types.EventType) error

	getSystemDefinition() models.SystemDefinition
	getSubscriptions() map[SubscriptionKey]types.EventType
}

/*
Creates a new event subscriber system, that can subscribe for events from event handlers that uses the eventHandlerImplementation.
The event subscriber will automatically on creation be registered to the service registry as a system.
*/
func EventSubscriberFactory(
	eventHandlerImplementation types.EventHandlerImplementationType,
	systemDomainAddress string,
	systemDomainPort int,
	systemName string,
	serviceRegistryAddress string,
	serviceRegistryPort int,
	serviceRegistryImplementation serviceregistry.ServiceRegistryImplementationType,
	certFilePath string,
	keyFilePath string,
	truststoreFilePath string,
) (EventSubscriber, error) {
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

	serviceQueryResult, err := serviceRegistryConnection.Query(models.ServiceDefinition{
		ServiceDefinition: "orchestration-service",
	})
	if err != nil {
		return nil, err
	}
	if len(serviceQueryResult.ServiceQueryData) <= 0 {
		return nil, fmt.Errorf("found no orchestration service")
	}
	serviceQueryData := serviceQueryResult.ServiceQueryData[0]
	orchestrationConnection, err := orchestrator.NewConnection(
		orchestrator.Orchestrator{
			Address: serviceQueryData.Provider.Address,
			Port:    serviceQueryData.Provider.Port,
		},
		orchestrator.ORCHESTRATION_ARROWHEAD_4_6_1,
		orchestratormodels.CertificateInfo{
			CertFilePath: certFilePath,
			KeyFilePath:  keyFilePath,
			Truststore:   truststoreFilePath,
		},
	)
	if err != nil {
		return nil, err
	}

	var eventSubscriber EventSubscriber
	eventSubscriberBase := AbstractEventSubscriber{
		domainAddress:             systemDomainAddress,
		domainPort:                systemDomainPort,
		systemName:                systemName,
		serviceRegistryConnection: serviceRegistryConnection,
		orchestrationConnection:   orchestrationConnection,

		subscriptions: map[SubscriptionKey]types.EventType{},
	}

	switch eventHandlerImplementation {
	case types.RABBITMQ_3_12_12_EVENT_HANDLER:
		eventSubscriber = &RabbitmqEventSubscriber{
			AbstractEventSubscriber:          &eventSubscriberBase,
			stopSubscribingToEventTypeCannel: map[SubscriptionKey]chan bool{},
		}

	default:
		errorString := fmt.Sprintf("there is no %s event handler implementation", eventHandlerImplementation)
		return nil, errors.New(errorString)
	}

	eventSubscriberBase.EventSubscriber = eventSubscriber
	err = eventSubscriber.registerEventSubscriberSystem()
	if err != nil {
		log.Println(err)
	}
	return eventSubscriber, nil

}

type SubscriptionKey string

/*
Base class for event subscribers, implementing all service registry functionalities.
The abstract class does not implement the subscribe method and this must be implemented by a sub class.
*/
type AbstractEventSubscriber struct {
	EventSubscriber
	domainAddress             string
	domainPort                int
	systemName                string
	serviceRegistryConnection serviceregistry.ServiceRegistryConnection
	orchestrationConnection   orchestrator.OrchestratorConnection

	subscriptions map[SubscriptionKey]types.EventType
}

func (e *AbstractEventSubscriber) registerEventSubscriberSystem() error {
	_, err := e.serviceRegistryConnection.RegisterSystem(e.getSystemDefinition())
	if err != nil {
		return err
	}
	return nil
}

func (e *AbstractEventSubscriber) UnregisterEventSubscriberSystem() error {
	return e.serviceRegistryConnection.UnRegisterSystem(e.getSystemDefinition())
}

func (e *AbstractEventSubscriber) Subscribe(eventType types.EventType, receiver Receiver) error {
	systemDefinition := e.getSystemDefinition()
	orchestrationResponse, err := e.orchestrationConnection.Orchestration(
		string(eventType),
		[]string{
			"AMQP-INSECURE-JSON",
		},
		orchestratormodels.SystemDefinition{
			Address:    systemDefinition.Address,
			Port:       systemDefinition.Port,
			SystemName: systemDefinition.SystemName,
		},
		orchestratormodels.AdditionalParametersArrowhead_4_6_1{
			OrchestrationFlags: map[string]bool{
				"overrideStore": true,
			},
		},
	)
	if err != nil {
		return err
	}

	if len(orchestrationResponse.Response) <= 0 {
		return fmt.Errorf("found no %s event providers", eventType)
	}

	providers := orchestrationResponse.Response

	for _, provider := range providers {
		log.Printf("subscribing to %s events from %s at %s:%d\n", eventType, provider.Provider.SystemName, provider.Provider.Address, provider.Provider.Port)

		go func(eventType types.EventType, eventHandlerAddress string, eventHandlerPort int, eventHandlerSystemName string, metaData map[string]string) {
			subscriptionKey, err := e.NewSubscription(eventType, eventHandlerAddress, eventHandlerPort, eventHandlerSystemName)
			if err != nil {
				log.Printf("error during subscription: %s\n", err)
				return
			}

			err = e.subscribe(eventType, eventHandlerAddress, eventHandlerPort, eventHandlerSystemName, *subscriptionKey, metaData, receiver)
			if err != nil {
				log.Printf("error during subscription: %s\n", err)
				return
			}

		}(eventType, provider.Provider.Address, provider.Provider.Port, provider.Provider.SystemName, provider.Metadata)

	}
	return nil
}

func (e *AbstractEventSubscriber) Unsubscribe(eventType types.EventType) error {
	for subscriptionKey, currentEventType := range e.subscriptions {
		if currentEventType == eventType {
			e.RemoveSubscription(subscriptionKey)
		}
	}
	return nil
}

func (e *AbstractEventSubscriber) getSystemDefinition() models.SystemDefinition {
	return models.SystemDefinition{
		Address:    e.domainAddress,
		Port:       e.domainPort,
		SystemName: e.systemName,
	}
}

/*
Saves a notion of the subscription, so that the same connection is subscribed to multiple times.
A subscription is identified by eventType, eventHandlerAddress, eventHandlerPort, and eventHandlerSystemName.
A subscriptionKey is created and saved from those attributes.
If a new subscription is found to alredy exist an error is given.
*/
func (e *AbstractEventSubscriber) NewSubscription(eventType types.EventType, eventHandlerAddress string, eventHandlerPort int, eventHandlerSystemName string) (*SubscriptionKey, error) {
	subscriptionKey := e.NewSubscriptionKey(eventType, eventHandlerAddress, eventHandlerPort, eventHandlerSystemName)
	_, ok := e.subscriptions[subscriptionKey]
	if ok {
		return nil, fmt.Errorf("already subscribed to %s events from system %s at %s:%d", string(eventType), eventHandlerSystemName, eventHandlerAddress, eventHandlerPort)
	}

	e.subscriptions[subscriptionKey] = eventType
	return &subscriptionKey, nil
}

func (e *AbstractEventSubscriber) RemoveSubscription(subscriptionKey SubscriptionKey) {
	_, ok := e.subscriptions[subscriptionKey]
	if !ok {
		return
	}

	delete(e.subscriptions, subscriptionKey)
}

// The subscriptionKey identifies an unique subscription, it is identified by eventType, eventHandlerAddress, eventHandlerPort, and eventHandlerSystemName.
func (e *AbstractEventSubscriber) NewSubscriptionKey(eventType types.EventType, eventHandlerAddress string, eventHandlerPort int, eventHandlerSystemName string) SubscriptionKey {
	return SubscriptionKey(fmt.Sprintf("%s-%s-%d-%s", eventType, eventHandlerAddress, eventHandlerPort, eventHandlerSystemName))
}

func (e *AbstractEventSubscriber) getSubscriptions() map[SubscriptionKey]types.EventType {
	return e.subscriptions
}
