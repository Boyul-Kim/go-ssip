package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Event struct {
	Topic   string
	Payload any
	Time    time.Time
}

// TODO make it concurrency safe with mutex
type EventStorage struct {
	mu      sync.RWMutex
	storage []Event
}

type Subscriber struct {
	ID      string
	Topics  map[string]bool
	Channel chan Event
}

type DeadLetter struct {
	Event        Event
	SubscriberID string
	Reason       string
}

type EventBus struct {
	subscribers     map[string]*Subscriber
	topics          map[string]map[string]bool
	mu              sync.RWMutex
	bufferSize      int
	deadLetterQueue chan DeadLetter
	eventStorage    *EventStorage
}

func (eventStorage *EventStorage) StoreEvent(event Event) error {
	//TODO better error handling needed
	eventStorage.mu.Lock()
	defer eventStorage.mu.Unlock()
	eventStorage.storage = append(eventStorage.storage, event)
	return nil
}

func NewEventBus(bufferSize int) *EventBus {
	return &EventBus{
		subscribers:     make(map[string]*Subscriber),
		topics:          make(map[string]map[string]bool),
		bufferSize:      bufferSize,
		deadLetterQueue: make(chan DeadLetter, bufferSize),
		eventStorage:    &EventStorage{},
	}
}

func (bus *EventBus) Subscribe(id string, topics []string) (*Subscriber, error) {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	if _, exists := bus.subscribers[id]; exists {
		return nil, fmt.Errorf("subscriber %s exists", id)
	}

	subscriber := &Subscriber{
		ID:      id,
		Topics:  make(map[string]bool),
		Channel: make(chan Event, bus.bufferSize),
	}

	for _, topic := range topics {
		subscriber.Topics[topic] = true

		if _, exists := bus.topics[topic]; !exists {
			bus.topics[topic] = make(map[string]bool)
		}

		bus.topics[topic][id] = true
	}

	bus.subscribers[id] = subscriber
	return subscriber, nil
}

func (bus *EventBus) Unsubscribe(id string) error {
	bus.mu.Lock()
	defer bus.mu.Unlock()

	subscriber, exists := bus.subscribers[id]
	if !exists {
		return fmt.Errorf("subscriber %s does not exist", id)
	}

	for topic := range subscriber.Topics {
		if topicSubs, exists := bus.topics[topic]; exists {
			delete(topicSubs, id)
			if len(topicSubs) == 0 {
				delete(bus.topics, topic)
			}
		}
	}

	close(subscriber.Channel)
	delete(bus.subscribers, id)
	return nil
}

func (bus *EventBus) Publish(topic string, payload any) {
	event := Event{
		Topic:   topic,
		Payload: payload,
		Time:    time.Now(),
	}

	// Store event before publishing
	err := bus.eventStorage.StoreEvent(event)
	if err != nil {
		return
	}

	// Continue with normal publishing
	bus.mu.RLock()
	defer bus.mu.RUnlock()

	topicSubs, exists := bus.topics[topic]
	if !exists {
		return
	}

	for subID := range topicSubs {
		subscriber := bus.subscribers[subID]
		select {
		case subscriber.Channel <- event:
			// Event was sent successfully
		default:
			// Send to dead letter queue non blocking
			// Withtout this select the write will be suspended if the buffer is full due to the nature of goroutines waiting until the send is finished
			select {
			// This is linked to a consumer so that the queue stays open in theory
			case bus.deadLetterQueue <- DeadLetter{
				Event:        event,
				SubscriberID: subID,
				Reason:       "Buffer full",
			}:
			default:
				//need to do proper error handling here
				fmt.Println("DLQ full")
			}
		}
	}
}

func handleEvents(ctx context.Context, subscriber *Subscriber) {
	for {
		select {
		case event, ok := <-subscriber.Channel:
			if !ok {
				fmt.Printf("Subscriber %s channel closed", subscriber.ID)
				return
			}
			fmt.Printf("Subscriber %s received: %s - %v\n",
				subscriber.ID, event.Topic, event.Payload)
		case <-ctx.Done():
			fmt.Printf("Subscriber %s context done\n", subscriber.ID)
			return
		}
	}
}

// dead letter consumer
func (bus *EventBus) ProcessDeadLetterEvents(ctx context.Context) {
	for {
		select {
		case dl := <-bus.deadLetterQueue:
			fmt.Println("Dead letter queue processed", dl)
		case <-ctx.Done():
			return
		}
	}
}

func main() {
	bus := NewEventBus(10)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	// Create subscribers
	phoneApp, _ := bus.Subscribe("phone_app", []string{"security", "temperature"})
	thermostat, _ := bus.Subscribe("thermostat", []string{"temperature"})
	homeHub, _ := bus.Subscribe("home_hub", []string{"security", "temperature", "energy"})

	// Simple test for handling events

	for _, sub := range []*Subscriber{phoneApp, thermostat, homeHub} {
		wg.Add(1)
		go func(sub *Subscriber) {
			defer wg.Done()
			go handleEvents(ctx, sub)
		}(sub)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		bus.ProcessDeadLetterEvents(ctx)
	}()

	// Publish events
	bus.Publish("temperature", "Living room: 21.5°C")
	bus.Publish("security", "Front door opened")
	bus.Publish("energy", "Solar output: 3.2 kW")

	// Block until shutdown signal
	<-ctx.Done()
	fmt.Println("Shutdown signal received")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("Clean shutdown")
	case <-time.After(10 * time.Second):
		fmt.Println("Shutdown timeout — forcing exit")
	}
}
