package la

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"go.uber.org/zap"
)

// neighbours interface needed to separate Agreement model from Broadcast
// mechanism which Agreement can employ.
type Neighbours interface {
	N() uint64 // amount of active neighbours
	Bcast(ctx context.Context, msg Message) chan Message
	Send(receiverPID uint64, msg Message) error
	RecvInput(host uint64) chan Message
	Recv(from uint64) chan Message
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

// Local is a Neighbours impl for use message passing at one host
type Local struct {
	mu    sync.RWMutex
	hosts map[uint64]duplex
	log   *zap.Logger
}

func NewNeighboursLocal(log *zap.Logger) *Local {
	return &Local{
		hosts: make(map[uint64]duplex),
		log:   log,
	}
}

type duplex struct {
	in  chan Message
	out chan Message
}

func (l *Local) AddNew(hostID uint64) {
	l.mu.Lock()
	l.hosts[hostID] = duplex{in: make(chan Message, 128), out: make(chan Message, 128)}
	l.mu.Unlock()
}

func (l *Local) N() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return uint64(len(l.hosts))
}

func (l *Local) Send(hid uint64, msg Message) error {
	l.mu.RLock()
	ch := l.hosts[hid]
	l.mu.RUnlock()

	if ch.in == nil || ch.out == nil {
		return errors.New("route to host is malformed")
	}
	ch.in <- msg
	return nil
}

// If by broadcasting message will be loopbacked to sender person, he also might be
// voting as all another processes. But how to vote?
func (l *Local) Bcast(ctx context.Context, msg Message) chan Message {
	sink := make(chan Message)
	var wg sync.WaitGroup

	l.mu.RLock()
	for _, ch := range l.hosts {
		wg.Add(1)

		go func(ch *duplex, wg *sync.WaitGroup) {
			defer wg.Done()

			ch.in <-msg
			resp := <- ch.out
			sink<-resp
		}(&ch, &wg)
		ch.in <- msg

		// wg.Add(1)
		// // routine waits response from process. Any response from process copied
		// // to sink from which Sender can read responses. Waiting can be interrupted by context
		// go func(sink chan<- Message, out <-chan Message, ctx context.Context, wg *sync.WaitGroup) {
		// 	defer wg.Done()
		// 	select {
		// 	case m := <-out:
		// 		sink <- m
		// 	case <-ctx.Done():
		// 		return
		// 	}
		// }(sink, ch.out, ctx, &wg)
	}
	l.mu.RUnlock()
	l.log.Debug("finished message broadcasting")
	go func(sink chan Message, wg *sync.WaitGroup) {
		wg.Done()
		close(sink)
	}(sink, &wg)
	//
	//
	// // routine needed for proper closing of a sink. It closes if all hosts did respond OR if
	// // ctx has been cancelled, whatever happens first.
	// go func(sink chan<- Message, ctx context.Context, wg *sync.WaitGroup) {
	// 	ch := make(chan struct{}, 1)
	// 	go func() {
	// 		wg.Wait()
	// 		ch <- struct{}{}
	// 	}()
	//
	// 	select {
	// 	case <-ctx.Done():
	// 		break
	// 	case <-ch:
	// 		break
	// 	}
	// 	close(sink)
	// }(sink, ctx, &wg)

	return sink
}

// ofc only owner process should receive from channel. But in case of Local impl
func (l *Local) RecvInput(host uint64) chan Message {
	var c chan Message
	l.mu.RLock()
	c = l.hosts[host].in
	l.mu.RUnlock()
	return c
}

func (l *Local) Recv(from uint64) chan Message{
	l.mu.RLock()
c:= 	l.hosts[from].out
	l.mu.RUnlock()
return c
}
