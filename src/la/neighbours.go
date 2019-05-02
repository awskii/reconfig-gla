package lattice

import (
	"fmt"
	"net"
	"sync"
)

// Neighbours interface needed to separate Lattice model from Broadcast
// mechanism which Lattice can employ.
type Neighbours interface {
	N() uint64                          // amount of active neighbours
	Bcast(msg Message) map[uint64]error // map where k:v is account_id:transmission_error if occured
	Send(receiverPID uint64, msg Message) error
}

// Neigh is a struct to separate all networking and
// communication stuff from process structure
type Neigh struct {
	hosts map[uint64]net.Conn
	// TODO(awskii): Quorums [][]
}

// Bcast does broadcast on all known hosts and collects delivery error.
// Any error during connection writing treated as delivery error.
//
// TODO(awskii): seems like RPC is suits to problem far more than simple TCP keep-alive.
func (n *Neigh) Bcast(msg []byte) map[uint64]error {
	hostsReceivedMutex := new(sync.Mutex)
	hostsReceived := make(map[uint64]error)

	for k, v := range n.hosts {
		go func(host uint64, c net.Conn, mu *sync.Mutex) {
			b, err := c.Write(msg)
			if b != len(msg) {
				err = fmt.Errorf("host=0x%x received bytes=%d msg_bytes=%d", host, b, len(msg))
			}
			mu.Lock()
			hostsReceived[host] = err
			mu.Unlock()
		}(k, v, hostsReceivedMutex)
	}
	return hostsReceived
}

func (n *Neigh) Send(hostID uint64, msg []byte) error {
	host, ok := n.hosts[hostID]
	if !ok {
		return fmt.Errorf("host with id=%d not in active host list", hostID)
	}
	b, err := host.Write(msg)
	if err != nil {
		return err
	}
	if b != len(msg) {
		return fmt.Errorf("host sent bytes mismatch: msg_size=%d write_size=%d", len(msg), b)
	}
	return nil
}

// N returns amount of currently active servers
func (n *Neigh) N() uint64 {
	return uint64(len(n.hosts))
}
