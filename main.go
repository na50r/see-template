package main

// Based on https://gist.github.com/ismasan/3fb75381cd2deb6bfa9c
// Original used one global channel to broadcast events to all clients
// This version broadcasts to a selection of clients

// Copyright (c) 2017 Ismael Celis

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

type Subscription struct {
	topic         string
	client        string
	clientChannel chan []byte
}

type ChannelSet struct {
	channels map[chan []byte]bool
}

func NewChannelSet() *ChannelSet {
	return &ChannelSet{
		channels: make(map[chan []byte]bool),
	}
}

func (cs *ChannelSet) Add(ch chan []byte) {
	cs.channels[ch] = true
}

func (cs *ChannelSet) Remove(ch chan []byte) {
	delete(cs.channels, ch)
}

func (cs *ChannelSet) Broadcast(msg []byte) {
	for ch := range cs.channels {
		ch <- msg
	}
}

type Broker struct {
	// Events are pushed to a specitic channel in this map
	Topics map[string]*ChannelSet

	// New client connections are pushed to this channel
	newClients chan Subscription

	// Closed client connections are pushed to this channel
	closingClients chan Subscription

	// Client connections registry
	clientChannels map[chan []byte]bool

	// Client ids
	clientIds map[string]chan []byte
}

func NewServer() (broker *Broker) {
	// Instantiate a broker
	broker = &Broker{
		Topics:         make(map[string]*ChannelSet),
		newClients:     make(chan Subscription),
		closingClients: make(chan Subscription),
		clientChannels: make(map[chan []byte]bool),
		clientIds:      make(map[string]chan []byte),
	}

	// Set it running - listening and broadcasting events
	go broker.listen()
	return
}

func (broker *Broker) listen() {
	for {
		select {
		case s := <-broker.newClients:
			// A new client has connected.
			// Register their message channel
			broker.clientChannels[s.clientChannel] = true

			// Add their channel to the topic
			if _, ok := broker.Topics[s.topic]; !ok {
				broker.Topics[s.topic] = NewChannelSet()
			}
			broker.Topics[s.topic].Add(s.clientChannel)
			log.Printf("Client added. %d registered clients", len(broker.clientChannels))
			log.Printf("Client added to topic %s. %d registered clients", s.topic, len(broker.Topics[s.topic].channels))
		case s := <-broker.closingClients:

			// A client has dettached and we want to
			// stop sending them messages.
			delete(broker.clientChannels, s.clientChannel)

			// Remove their channel from the topic
			broker.Topics[s.topic].Remove(s.clientChannel)

			// Delete the topic if no channel is registered
			if len(broker.Topics[s.topic].channels) == 0 {
				delete(broker.Topics, s.topic)
			}
			log.Printf("Removed client. %d registered clients", len(broker.clientChannels))
			log.Printf("Removed client from topic %s. %d registered clients", s.topic, len(broker.Topics[s.topic].channels))
		}
	}
}

type Message struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

type Transaction struct {
	Sender    int     `json:"sender"`
	Recipient int     `json:"recipient"`
	Amount    float64 `json:"amount"`
}

func getClientAndTopic(r *http.Request) (string, string) {
	vars := mux.Vars(r)
	client := vars["client"]
	topic := vars["topic"]
	return client, topic
}

func (broker *Broker) Stream(w http.ResponseWriter, r *http.Request) {
	// Check if the ResponseWriter supports flushing.
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	// Signal the broker that we have a new connection
	client, topic := getClientAndTopic(r)
	if topic == "" {
		topic = "default"
	}
	messageChan := broker.clientIds[client]
	if messageChan == nil {
		messageChan = make(chan []byte)
		broker.clientIds[client] = messageChan
	}
	s := Subscription{topic: topic, client: client, clientChannel: messageChan}
	// Very confusing but the messageChannel is passed into newClients
	// This means that broker.listen will broadcast event to this channel if Notifier received an event
	broker.newClients <- s

	// Remove this client from the map of connected clients
	// when this handler exits.
	defer func() {
		broker.closingClients <- s
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	for {
		select {
		// Listen to connection close and un-register messageChan
		case <-r.Context().Done():
			// remove this client from the map of connected clients
			broker.closingClients <- s
			return

		// Listen for incoming messages from messageChan
		case msg := <-messageChan:
			// Write to the ResponseWriter
			// Server Sent Events compatible
			fmt.Fprintf(w, "data: %s\n\n", msg)
			// Flush the data immediatly instead of buffering it for later.
			flusher.Flush()
		}
	}
}

func (broker *Broker) BroadcastTopic(topic string, eventType string, payload interface{}) {
	if _, ok := broker.Topics[topic]; !ok {
		log.Printf("Topic %s does not exist", topic)
		return
	}
	msg := Message{
		Type: eventType,
		Data: payload,
	}
	j, _ := json.Marshal(msg)
	broker.Topics[topic].Broadcast(j)
}

// Make an endpoint on your server that has access to the broker
func (broker *Broker) Transfer(w http.ResponseWriter, r *http.Request) {
	// Parse the request body
	var tx Transaction
	err := json.NewDecoder(r.Body).Decode(&tx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Send the message to the broker via Notifier channel
	broker.BroadcastTopic("transaction", "transaction", tx)
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Transaction sent\n"))
}

func (broker *Broker) SendTopic(w http.ResponseWriter, r *http.Request) {
	var msg Message
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	broker.BroadcastTopic("transaction", msg.Type, msg.Data)
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Message sent\n"))
}

func main() {
	broker := NewServer()
	r := mux.NewRouter()
	r.HandleFunc("/stream/{client}", broker.Stream).Methods("GET")
	r.HandleFunc("/stream/{client}/{topic}", broker.Stream).Methods("GET")
	r.HandleFunc("/send/{topic}", broker.SendTopic).Methods("POST")
	r.HandleFunc("/transfer", broker.Transfer).Methods("POST")
	log.Println("Listening on localhost:3000")
	err := http.ListenAndServe("localhost:3000", r)
	if err != nil {
		log.Fatal(err)
	}
}

// To test the server, run the following commands in separate terminals:
// Start listening to the stream
//     $ curl http://localhost:<port>/stream/transaction
// Send a transaction
//     $ curl -X POST -H "Content-Type: application/json" -d '{"sender": 1, "recipient": 2, "amount": 100"}' http://localhost:<port>/transfer
// Send a message to a topic
//     $ curl -X POST -H "Content-Type: application/json" -d '{"type": "message", "data": "Hello World"}' http://localhost:<port>/stream/topic1
