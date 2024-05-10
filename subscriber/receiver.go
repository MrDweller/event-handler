package eventsubscriber

/*
A Receiver provides a method to receive an event, this is used in the EventSubscriber systems when subscribing.
Thus, a specific implementation of what to do when receiving an event can be written, and injected to the subscriber.
*/
type Receiver interface {

	// Called when a subscriber gets an event (if given to the subscribe method).
	ReceiveEvent(event []byte)
}
