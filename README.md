# Based on: https://thoughtbot.com/blog/writing-a-server-sent-events-server-in-go

# How the original version worked (High Level)
* The original version ran `broker.listen` as a goroutine in the background.
* It listened to a 'global channel' called `Notifier`
```go
type Broker struct {
	Notifier chan []byte
	newClients chan chan []byte
	closingClients chan chan []byte
	clients map[chan []byte]bool
}
```
* `Notifier` will receive an event, typically when a call to an endpoint is made:
```go 
broker.Notifier <- []byte(msg) //msg can be a string, struct, whatever
```

* And the goroutine will forward this event to all clients
```go
func (broker *Broker) listen() {
    ...
		case event := <-broker.Notifier:

			// We got a new event from the outside!
			// Send event to all connected clients
			for clientMessageChan, _ := range broker.clients {
				clientMessageChan <- event
			}
		}
    ...
```
* In the stream endpoint, the client creates their channel but also continues listening to it.
```go
    //Create channel here
	messageChan := make(chan []byte)
	broker.newClients <- messageChan

    ...
	for {

		// Write to the ResponseWriter
		// Server Sent Events compatible
		fmt.Fprintf(rw, "data: %s\n\n", <-messageChan)

		// Flush the data immediatly instead of buffering it for later.
		flusher.Flush()
	}
```
* `messageChan` is added to `broker.newClients`
* All clients in `broker.newClients` are added to `broker.clients`
* The goroutine sends the event to all clients in `broker.clients`
* `messageChan 'changes' state and the stream endpoint print the event placed by the goroutine.


# How my version works
I want to broadcast to a group of channels but still utilize this setup. The main difference is that I do not need a "global Notifier" channel but instead just group the client channels based on the topics a client subscribed to. 
* Each client has one channel
* A client can subscribe to multiple topics
* A topic is a set of channels
* A client subscribed to a topic will receive events broadcasted to that topic in their channel
