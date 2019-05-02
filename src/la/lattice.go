package lattice

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
)

var (
	ErrUnknownAccountID = errors.New("unknown account id")
	// ErrNonMonotonicSeqID means that received message from some process
	// with proposalSeqNo greater than our and diff > 1
	ErrNonMonotonicSeqID = errors.New("non-monotonic sequence id")
	// ErrNextNotSuperset means that received message from some process
	// with value which is not a superset of our current value
	// which violates Validity property
	ErrNextNotSuperset = errors.New("received element should be superset of previous one")
	// ErrMergeNegativeBalance means that merge result does not
	// holds non-negative balance restriction (Validity property)
	ErrMergeNegativeBalance = errors.New("trying to merge element which leads to balance(a) < 0")
)

const (
	// StatusAcceptor means that process can only handle
	// requests from network, but can not propose new values
	StatusAcceptor uint32 = 0
	// StatusProposer means that process proposing new value now
	// and can't handle Decide calls from other processes
	StatusProposer uint32 = 1
)

// Lattice should be initalized in every working process
type Lattice struct {
	accounts map[uint64]*Element // by account ID store their ordered by SeqID transactions.
	// auxiliary values for GLA
	activeValue   *Element
	acceptedValue *Element
	proposedValue *Element
	proposalSeqNo uint64
	outputValue   *Element
	ack, nack     uint64

	version uint64
	// currently owned account. Further will be added
	// account migration among processes, but this variable
	// will exists anyway to hold 'One process can holds many
	// accounts, but only one processing at a time'
	ownedAccount uint64
}

// Element reprsents one lattice element
type Element struct {
	accountID uint64 // defines owner of a set T
	T         []Tx   // Holds incoming and outgoing tx's
	// Could be extended with Replecated Data Types:
	//  - u,   ownership function(?) implementation
	//  - Pa,  currently available processes
	//  - Prm, currently deleted processes
}

// Tx represents transaction
type Tx struct {
	Sender   uint64 // sender account ID
	Receiver uint64 // receiver account ID
	Amount   uint64 // tx amount
	SeqID    uint64 // tx sequence number for Sender account
}

// Message is an interface for anything which could be
// passed through network among processes
type Message interface {
	Bytes() []byte
	// TODO(awskii): should be extended with crypto signatures
}

// Process holds all needed data and states
// to provide communication with another processes
// and reach generalized lattice agreement
type Process struct {
	id         uint64     // UUID, process identifier (PID)
	status     uint32     // current process status {Proposer, Acceptor}
	lattice    *Lattice   // Lattice structure for, implementation of a GLA
	neighbours Neighbours // map of available servers to P2P communication with
	// CurrentQuorumID int -- should be added later. All quorum stuff should be
	// moved into separated module about Network: P2P state, known active host list,
	// list of quorums in currently active configuration and other.
}

// Merge used to merge element e with sub-semi-lattice of an
// There are some restictions on operation:
//  - after merging sender account balance should be >= 0
//  - element e should be a superset of a lattice element at position pos
//  - ???
//
// Merge can return
//   ErrNonMonotonicSeqID    -- we missed some messages, should we do something here?
//   ErrNextNotSuperset      -- their miss some messages, should be refined
//   ErrUnknownAccountID     -- unknown account id
//   ErrMergeNegativeBalance -- negative account balance after merge
func (l *Lattice) Merge(accountID uint64, next Element) (version uint64, err error) {
	v, ok := l.accounts[accountID] // RACE
	if !ok { // unknown account
		return l.version, ErrUnknownAccountID
	}

	join, err := merge(accountID, v, &next)
	if err != nil {
		return l.version, err
	}
	l.accounts[accountID] = join // RACE
	return l.version, nil
}

type Proposable interface {
	Proposer() uint64 // PID
	SeqNo() uint64    // sequence number of proposition
	Kind() int        // kind of proposal (pay, ...)
}

func (l *Lattice) makeProposal(senderPID uint64, tx *Tx) (*proposal, error) {
	l.proposedValue = l.accounts[tx.Sender]
	l.proposedValue.T = append(l.proposedValue.T, *tx)
	l.nextRound()

	return &proposal{
		SenderPID: senderPID,
		SeqNo:     l.proposalSeqNo,
		Value:     *l.proposedValue,
	}, nil
}

func (l *Lattice) nextRound() {
	atomic.AddUint64(&l.proposalSeqNo, 1)
	l.ack, l.nack = 0, 0
}

// Propose used for proposing value of p.lattice to all another
// processed ProposedValue MUST be verified before proposing.
func (p *Process) Propose(ownedAccountID uint64, tx *Tx) error {
	// TODO: seems that that check in wrong place in mine algo
	if atomic.LoadUint64(&p.lattice.proposalSeqNo) != 0 {
		return errors.New("currently proposing, parallel proposition is restricted")
	}
	if !atomic.CompareAndSwapUint32(&p.status, StatusAcceptor, StatusProposer) {
		return errors.New("currently proposing, parallel proposition is restricted")
	}
	defer atomic.StoreUint32(&p.status, StatusProposer)

	// TODO(awskii): process should own that account and quorum should approve it
	prop, err := p.lattice.makeProposal(p.id, tx)
	if err != nil {
		return err
	}
	// thats only delivery reports, not ACK/NACK messages
	deliveryReport := p.neighbours.Bcast(prop)

	return err
}

// simple func to easily check if some 'predicate' holds on proposal
func (p *Process) predicate(accountID uint64, proposedValue *Element, N, ack, nack uint64) bool {
	if nack > 0 && nack+ack >= quorum(N, ack, nack) && atomic.LoadUint32(&p.status) == StatusProposer {
		return false
	}
	if ack < quorum(N, ack, nack) && atomic.LoadUint32(&p.status) != StatusProposer {
		return false
	}
	return true
}

// Decide is an Accept func from original GLA
func (p *Process) Decide(proposerID, proposalSeqNo uint64, proposedValue *Element) bool {
	if atomic.LoadUint32(&p.status) != StatusAcceptor {
		panic("Decide call with non-acceptor process status")
		// return false
	}
	if proposalSeqNo != p.proposalSeqNo {
		panic(fmt.Errorf(
			"deciding on proposal_seq_no=%d, proccess current proposal_seq_no=%d",
			proposalSeqNo, p.proposalSeqNo))
		// return false
	}

	// TODO need to check if we processing currently proposed value
	msg := &responseMessage{
		PID:   p.id,
		SeqNo: proposalSeqNo,
		Value: *proposedValue,
	}

	if err := validate(p.ownedAccount, p.acceptedValue, proposedValue); err != nil {
		p.acceptedValue, err = merge(p.ownedAccount, p.acceptedValue, p.proposedValue)
		msg.Value = *p.acceptedValue
		if err := p.neighbours.Send(proposerID, msg); err != nil {
			panic(err)
		}
		return false
	}
	p.acceptedValue = p.proposedValue
	msg.Ack = true
	err := p.neighbours.Send(proposerID, msg)
	if err != nil {
		panic(err)
	}
	return true
}

// ------------------------- Helper functions -----------------------------
// dist checks distance between next and previous element.
// There are four cases:
//  - 0 -- ordering violation, panics
//  - 1 -- next element is right after previous, ok
//  - >1 - ordering violation, we didn't receive previous messages yet, ~ok
//  - <0 - next element is behind us, so we need to refine it, ok
func dist(prev, next *Element) (int, error) {
	var err error
	if len(prev.T) < len(next.T) {
		// next element should be superset or equal to previous
		// pass further to evaluate distance, but we should return
		// right after distance zero check
		err = ErrNextNotSuperset
	}

	latestPrev, latestNext := prev.T[len(prev.T)-1], next.T[len(next.T)-1]
	d := latestNext.SeqID - latestPrev.SeqID
	if d == 0 {
		panic(fmt.Errorf("ordering violation at seq_no=%d", latestNext.SeqID))
	}
	if err != nil {
		return int(d), err
	}
	if d > 1 {
		// panic(fmt.Errorf("ordering violation: our seq_no=%d, their seq_no=%d",
		// 	latestPrev.SeqID, latestNext.SeqID))
		return int(d), ErrNonMonotonicSeqID
	}
	if d < 0 {
		return int(d), ErrNextNotSuperset
	}
	return int(d), nil
}

// validate checks:
//  - distance between next and previous element
//  - if next.T is a superset of prev.T
//  - that merge(prev, next) result has positive balance
func validate(accoountID uint64, prev, next *Element) error {
	dist, err := dist(prev, next)
	if err != nil {
		return err
	}

	var (
		i       int
		balance uint64
	)

	// it's not safe to rely on sequence ID only, a way to fast check
	// two elements intersection (faster that O(n)) is needed
	for ; i < len(prev.T); i++ {
		p, n := prev.T[i], next.T[i]
		if err = compare(&p, &n); err != nil {
			return err
		}

		switch accoountID {
		case p.Sender:
			balance -= p.Amount
		case p.Receiver:
			balance += p.Amount
		}
	}

	// now we know that at step maxSeqNo(prev) we have some balance.
	// So let's merge two elements since maxSeqNo(prev) and finish balance validation
	if dist > 1 {
		panic("dist >1, but trying to merge for validation")
	}

	n := next.T[i+1]
	switch accoountID {
	case n.Sender:
		balance -= n.Amount
	case n.Receiver:
		balance += n.Amount
	}

	if balance < 0 {
		return ErrMergeNegativeBalance
	}
	return nil
}

func compare(a, b *Tx) error {
	if a.Sender != b.Sender {
		return fmt.Errorf("their tx seq_no=%d: invalid Sender", b.SeqID)
	}
	if a.Receiver != b.Receiver {
		return fmt.Errorf("their tx seq_no=%d: invalid Receiver", b.SeqID)
	}
	if a.Amount != b.Amount {
		return fmt.Errorf("their tx seq_no=%d: invalid Amount", b.SeqID)
	}
	if a.SeqID != b.SeqID {
		return fmt.Errorf("their tx seq_no=%d: invalid SeqID", b.SeqID)
	}
	return nil
}

// refine watches if two elements has distance more than one sequence number
// and if it is, returns subset of elements missed by next Element
func refine(prev, next *Element) (*Element, error) {
	dist, err := dist(prev, next)
	if err != nil {
		return nil, err
	}
	if dist > 1 {
		// received element is older than ours on 'dist' steps, then we missed
		// (or did not receive yet) and process messages from that steps, so we
		// need to wait. In that case, we have nothing to refine next element with.

		// Could be an error, but not sure.
		return nil, ErrNonMonotonicSeqID
	}

	diff := &Element{
		T: make([]Tx, dist),
	}

	for i, j := 0, next.T[len(next.T)-1].SeqID; j < uint64(len(prev.T)); i, j = i+1, j+1 {
		diff.T[i] = prev.T[j]
	}
	return diff, nil
}

// simple func to easily change quorum conditions
func quorum(N, ack, nack uint64) uint64 {
	return uint64(math.Ceil(float64(N) + 1.0/2.0))
}

// merge checks if prev and next 'mergable' -- result MUST holds validity
// and partial order properties. If so, merge them
func merge(accountID uint64, prev, next *Element) (*Element, error) {
	err := validate(accountID, prev, next)
	if err != nil {
		return nil, err
	}

	// distance next-prev = 1 since it's valid, so next is safe.
	return &Element{
		T: append(prev.T, next.T[len(next.T)-1]),
	}, nil
}
