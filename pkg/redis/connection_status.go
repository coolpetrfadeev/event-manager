package redis

import "sync"

const (
	ConnectionStatusReady  = "ready"
	ConnectionStatusClosed = "closed"
)

type ConnectionStatus struct {
	status string
	mutex  sync.Mutex
}

func (c *ConnectionStatus) SetReadyStatus() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.status = ConnectionStatusReady
}

func (c *ConnectionStatus) SetClosedStatus() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.status = ConnectionStatusClosed
}

func (c *ConnectionStatus) IsReady() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.status == ConnectionStatusReady {
		return true
	}
	return false
}
