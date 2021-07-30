package csxwebsocket

import (
	"sync"

	"github.com/sirupsen/logrus"
)

var clientsMutex sync.RWMutex

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

// registerClient register new client
func (h *Hub) registerClient(client *Client) {
	clientsMutex.Lock()
	h.clients[client.ID] = client
	clientsMutex.Unlock()
	logrus.Debug("register websocket client: ", client.ID)
}

// unregisterClient unregister client and force close connection
func (h *Hub) unregisterClient(client *Client, useLock bool) {
	if useLock {
		clientsMutex.Lock()
	}
	if _, ok := h.clients[client.ID]; ok {
		delete(h.clients, client.ID)
		close(client.send)
	}
	if useLock {
		clientsMutex.Unlock()
	}
	logrus.Debug("unregister websocket client: ", client.ID)
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

// GetClientByID get client by ID
func (h *Hub) GetClientByID(clientID string) (*Client, bool) {
	clientsMutex.RLock()
	client, ok := h.clients[clientID]
	clientsMutex.RUnlock()
	return client, ok
}

// RangeClients iterate all clients in hub and run callback
func (h *Hub) RangeClients(cb func(client *Client)) {
	clientsMutex.Lock()
	for _, client := range h.clients {
		cb(client)
	}
	clientsMutex.Unlock()
}

// SendBroadcast send broadcast with prepare message callback
func (h *Hub) SendBroadcast(cb func(*Client) (*Message, bool)) {
	clientsMutex.Lock()
	for _, client := range h.clients {
		message, ok := cb(client)
		if !ok {
			continue
		}
		select {
		case client.send <- message:
		default:
			h.unregisterClient(client, false)
		}
	}
	clientsMutex.Unlock()
}

// Run prepare hub channels
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.registerClient(client)
		case client := <-h.Unregister:
			h.unregisterClient(client, true)
		case message := <-h.Send:
			for i := 0; i < len(message.Clients); i++ {
				clientID := message.Clients[i]
				client, ok := h.GetClientByID(clientID)
				if !ok {
					continue
				}
				select {
				case client.send <- message:
				default:
					h.unregisterClient(client, true)
				}
			}
		case message := <-h.Broadcast:
			clientsMutex.Lock()
			for _, client := range h.clients {
				select {
				case client.send <- message:
				default:
					h.unregisterClient(client, false)
				}
			}
			clientsMutex.Unlock()
		}
	}
}
