package la

import (
	"bytes"
	"encoding/gob"
	"net"
)

type responseMessage struct {
	Ack   bool   // false in case NACK // could be extended to uint32 to pass error code in case NACK
	PID   uint64 // responder process ID
	SeqNo uint64 // proposalSeqNo
	Value Element
}

func (r *responseMessage) Bytes() []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(r); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (r *responseMessage) RespondToAddr() net.Addr {
	return nil
}

type proposal struct {
	SenderPID uint64   // proposer PID
	SeqNo     uint64   // tx seq_no for sender account
	Value     Element  // proposed value
	ProcAddr  net.Addr // proposer address
}

func (p *proposal) Bytes() []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(p); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (p *proposal) RespondToAddr() net.Addr {
	return p.ProcAddr
}

type DiffType int

const (
	txDiff DiffType = 1 << iota
)

type Diff interface {
	Type() DiffType
}

type Proposal struct {
	ProposerID       uint64
	ack, nack, seqNo uint64

	Value Diff
}

func (p *Proposal) Bytes() []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(p); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

