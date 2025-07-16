package main

// Original based on: https://github.com/plutov/packagemain/tree/master/30-sse
// YouTube video: https://www.youtube.com/watch?v=nvijc5J-JAQ

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)


func main() {
	b := NewBroker()
	http.HandleFunc("/events", b.sseHandler)
	http.HandleFunc("/publish", b.PublishEndpoint)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("unable to start server: %s", err.Error())
	}
}

type Broker struct {
	cnt            int
	clientChannels map[int]chan []byte
}


type Message struct {
	Data interface{} `json:"data"`
}

func NewBroker() *Broker {
	return &Broker{
		clientChannels: make(map[int]chan []byte),
		cnt:            0,
	}
}

func (b *Broker) createChannel() int {
	b.cnt++
	b.clientChannels[b.cnt] = make(chan []byte)
	return b.cnt
}

func (b *Broker) sseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	channelID := b.createChannel()
	channel := b.clientChannels[channelID]
	fmt.Printf("client connected (id=%d)\n", channelID)

	defer func() {
		delete(b.clientChannels, channelID)
		b.cnt--
	}()

	clientGone := r.Context().Done()

	rc := http.NewResponseController(w)

	for {
		select {
		case <-clientGone:
			fmt.Printf("client has disconnected (id=%d)\n", channelID)
			return
		case data := <-channel:
			if _, err := fmt.Fprintf(w, "event:msg\ndata:%s\n\n", data); err != nil {
				log.Printf("unable to write: %s", err.Error())
				return
			}
			rc.Flush()
		}
	}
}

func (b *Broker) Publish(msg Message) {
	data, err := json.Marshal(msg.Data)
	if err != nil {
		log.Printf("unable to marshal: %s", err.Error())
		return
	}
	// Publish to all channels
	// NOTE: Not concurrent
	for _, channel := range b.clientChannels {
		channel <- data
	}
}

func (b *Broker) PublishEndpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var m Message
	err := json.NewDecoder(r.Body).Decode(&m)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	b.Publish(m)
	w.Write([]byte("Msg sent\n"))
}
