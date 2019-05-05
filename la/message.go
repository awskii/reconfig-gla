package la

import (
	"bytes"
	"encoding/gob"
)

type msgType int

const (
	respondType msgType = 1 << iota
	proposalType
)

// Message is an interface for anything which could be
// passed through network among processes
type Message interface {
	// TODO(awskii): should be extended with crypto signatures
	Bytes() []byte
	Type() msgType
}

type response struct {
	Ack   bool   // false in case NACK // could be extended to uint32 to pass error code in case NACK
	PID   uint64 // responder process ID
	SeqNo uint64 // proposalSeqNo
	Value []Element
}

func (r *response) Bytes() []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(r); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (r *response) Type() msgType {
	return respondType
}

type proposal struct {
	SenderPID uint64    // proposer PID
	SeqNo     uint64    // tx seq_no for sender account
	Value     []Element // proposed value
}

func (p *proposal) Bytes() []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(p); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (p *proposal) Type() msgType {
	return proposalType
}

func (p *proposal) Refine(v []Element) {


}
