package csxwebsocket

import "github.com/sirupsen/logrus"

// Hub maintains the set of active clients and broadcasts messages to the
// clients.
type Hub struct {
	// Registered clients.
	clients map[string]*Client

	// Inbound messages from the clients.
	Broadcast chan *Message

	// Send channel for personal client messages
	Send chan *Message

	// Register requests from the clients.
	register chan *Client

	// Unregister requests from clients.
	Unregister chan *Client
}

// NewHub constructor for create new hub
func NewHub() *Hub {
	return &Hub{
		Broadcast:  make(chan *Message),
		register:   make(chan *Client),
		Unregister: make(chan *Client),
		clients:    make(map[string]*Client),
		Send:       make(chan *Message),
	}
}

func (h *Hub) GetHubClients() map[string]*Client {
	return h.clients
}

// Run prepare hub channels
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			logrus.Debug("register websocket client: ", client.ID)
			h.clients[client.ID] = client
		case client := <-h.Unregister:
			logrus.Debug("unregister websocket client: ", client.ID)
			if _, ok := h.clients[client.ID]; ok {
				delete(h.clients, client.ID)
				close(client.send)
			}
		case message := <-h.Send:
			for i := 0; i < len(message.Clients); i++ {
				clientID := message.Clients[i]
				client := h.clients[clientID]
				if client == nil {
					continue
				}
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, clientID)
				}
			}
		case message := <-h.Broadcast:
			for clientID, client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, clientID)
				}
			}
		}
	}
}
