package la

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.uber.org/zap"
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
	// and can't handle Accept calls from other processes
	StatusProposer uint32 = 1
)

// Lattice should be initalized in every working process
type Lattice struct {
	mu       sync.RWMutex
	accounts map[uint64]*Element // by account ID store their ordered by SeqID transactions.
	// auxiliary values for GLA
	activeValue   *Element
	acceptedValue *Element
	proposedValue *Element
	proposalSeqNo uint64
	outputValue   *Element
	ack, nack     uint64
	log           *zap.Logger

	version uint64
	// currently owned account. Further will be added
	// account migration among processes, but this variable
	// will exists anyway to hold 'One process can holds many
	// accounts, but only one processing at a time'
	ownedAccount uint64
}

func NewLattice(owner uint64, log *zap.Logger) *Lattice {
	return &Lattice{
		accounts:     make(map[uint64]*Element),
		ownedAccount: owner,
		log:          log,
	}
}

func (l *Lattice) AddAccount(aid uint64) {
	// l.log.Debug("add account", zap.Uint64("account_id", aid))
	l.mu.Lock()
	l.accounts[aid] = &Element{
		accountID: aid,
		T:         []Tx{{Sender: 0, Receiver: aid, Amount: 20, SeqID: 1}},
	}
	l.mu.Unlock()
}

// Accept func from original GLA
func (l *Lattice) Accept(proposerID, myPID uint64, proposal *proposal, netw Neighbours) (bool, error) {
	// if proposal.SeqNo != l.proposalSeqNo {
	// 	return false, fmt.Errorf(
	// 		"deciding on proposal_seq_no=%d, proccess current proposal_seq_no=%d",
	// 		proposal.SeqNo, l.proposalSeqNo)
	// }

	// TODO need to check if we processing currently proposed value
	msg := &responseMessage{
		PID:   myPID,
		SeqNo: proposal.SeqNo,
		Value: proposal.Value,
	}

	l.mu.RLock()
	l.acceptedValue = l.accounts[proposal.Value.accountID]
	l.mu.RUnlock()
	l.log.Debug("validating proposal", zap.Uint64("proposal_id", proposal.SeqNo))

	if err := validate(l.ownedAccount, l.acceptedValue, &proposal.Value); err != nil {
		av, e := refine(l.acceptedValue, &proposal.Value)
		if e != nil {
			/* can't merge accepted and proposed value
			When we can't do it?
			*/
			msg.Value = *l.acceptedValue
			// netw.Bcast(context.Background(), msg)
			if er := netw.Send(proposerID, msg); er != nil {
				l.log.Debug("send error", zap.Error(er))
			}
			return false, errors.Errorf("refine_err=%v", e)
		}
		l.acceptedValue = av
		msg.Value = *l.acceptedValue
		// netw.Bcast(context.Background(), msg)
		if er := netw.Send(proposerID, msg); er != nil {
			l.log.Debug("send error", zap.Error(er))
		}
		return false, errors.Wrap(err, "validation failure")
	}
	msg.Ack = true

	var err error
	l.acceptedValue, err = merge(proposal.Value.accountID, l.acceptedValue, &proposal.Value)
	if err != nil {
		msg.Ack = false
	}
	l.accounts[proposal.Value.accountID] = l.acceptedValue

	// netw.Bcast(context.Background(), msg)
	if er := netw.Send(proposerID, msg); er != nil {
		l.log.Debug("send error", zap.Error(er))
	}
	return msg.Ack, nil
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
	l.mu.Lock()
	defer l.mu.Unlock()
	v, ok := l.accounts[accountID] // RACE
	if !ok {                       // unknown account
		return l.version, ErrUnknownAccountID
	}

	joinMaster, err := merge(accountID, v, &next)
	if err != nil {
		return l.version, err
	}

	tx := next.T[len(next.T)-1]
	newElem := l.accounts[tx.Receiver]
	seqNo := newElem.T[len(newElem.T)-1].SeqID
	tx.SeqID = seqNo + 1
	newElem.T = append(newElem.T, tx)

	// tx.SeqID = .T[len(l.accounts[tx.Receiver].T)-1].SeqID + 1
	// joinSlave, err := merge(tx.Receiver, l.accounts[tx.Receiver], &tx)
	// if err != nil {
	// 	return l.version, err
	// }
	l.accounts[accountID] = joinMaster // RACE
	l.accounts[tx.Receiver] = newElem
	return l.version, nil
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
	proposing  chan struct{}
	log        *zap.Logger
	// CurrentQuorumID int -- should be added later. All quorum stuff should be
	// moved into separated module about Network: P2P state, known active host list,
	// list of quorums in currently active configuration and other.
}

func NewProcess(id uint64, l *Lattice, n Neighbours, log *zap.Logger) *Process {
	log = log.With(zap.Uint64("pid", id), zap.Uint64("owned_account_id", l.ownedAccount))
	log.Info("process initiated")

	return &Process{
		id:         id,
		lattice:    l,
		neighbours: n,
		proposing:  make(chan struct{}),
		log:        log,
	}
}

func (p *Process) Accept(proposerID uint64, proposal *proposal) bool {
	log := p.log.With(
		zap.Uint64("proposer_id", proposal.SenderPID),
		zap.Uint64("prop_seq_no", proposal.SeqNo),
	)
	if atomic.LoadUint32(&p.status) != StatusAcceptor {
		// log.Panic("Accept call with non-acceptor process status")
		return false
	}
	ok, err := p.lattice.Accept(proposerID, p.id, proposal, p.neighbours)
	if err != nil {
		log.Debug("decision: nack", zap.Error(err))
		return ok
	}
	log.Debug("decision: ack", zap.Error(err))
	return ok
}

// Propose used for proposing value of p.lattice to all another
// processed ProposedValue MUST be verified before proposing.
func (p *Process) Propose(ownedAccountID uint64, tx *Tx) error {
	// TODO: seems that that check in wrong place in mine algo
	if atomic.LoadUint64(&p.lattice.proposalSeqNo) != 0 {
		return errors.New("currently proposing, parallel proposition is restricted")
	}
	p.log.Debug("check local_proposal_seq_no OK")
	if !atomic.CompareAndSwapUint32(&p.status, StatusAcceptor, StatusProposer) {
		return errors.New("currently proposing, parallel proposition is restricted")
	}

	p.log.Debug("set process_status=proposer OK")
	p.proposing <- struct{}{} // notify that process starting his proposition

	defer func() {
		atomic.StoreUint32(&p.status, StatusProposer)
		p.log.Debug("set process_status=acceptor OK")
	}()
	defer func() { p.proposing <- struct{}{} }() // notify that proposing is finished

	// TODO(awskii): process should own that account and quorum should approve it
	// TODO(awskii): when proposal become more useful, move broadcast into lattice Propose function
	prop, err := p.lattice.makeProposal(p.id, tx)
	if err != nil {
		return err
	}
	p.log.Debug("tx wrapped as proposal OK")
	p.log.Sugar().Debugf("proposed_value %+v", prop.Value.T)

	log := p.log.With(zap.Uint64("proposal_seq_no", prop.SeqNo))

	resp := p.neighbours.Bcast(context.Background(), prop)
	for r := range resp {
		rm, ok := r.(*responseMessage)
		if !ok || rm.SeqNo != prop.SeqNo {
			continue
		}

		if rm.Ack {
			// we could count ack/nack in proposal to get more elegant solution
			p.lattice.ack++
		} else {
			p.lattice.nack++
			pv, err := refine(p.lattice.proposedValue, &rm.Value)
			if err != nil {
				log.Error("can not update proposed value", zap.Error(err))
				continue
			}
			p.lattice.proposedValue = pv
		}
		log.Debug("gossip", zap.Uint64("ack", p.lattice.ack),
			zap.Uint64("nack", p.lattice.nack))

		// condition could turns into true before we process all response
		if p.predicate(ownedAccountID, p.neighbours.N(), p.lattice.ack, p.lattice.nack) {
			log.Debug("merge proposal")
			// true here means quorum accepted that change, apply it to our version
			p.lattice.Merge(ownedAccountID, prop.Value)
		}
	}
	if p.predicate(ownedAccountID, p.neighbours.N(), p.lattice.ack, p.lattice.nack) {
		log.Debug("merge proposal")
		// true here means quorum accepted that change, apply it to our version
		_, err = p.lattice.Merge(ownedAccountID, prop.Value)
		log.Error("merging error", zap.Error(err))
	}
	log.Debug("proposal has been rejected")
	return err
}

// simple func to easily check if some 'predicate' holds on proposal
func (p *Process) predicate(accountID uint64, N, ack, nack uint64) bool {
	p.log.Info("predicate check",
		zap.Uint64("N", N),
		zap.Uint64("ack", ack),
		zap.Uint64("nack", nack),
		zap.Uint64("quorum", quorum(N, ack, nack)),
		zap.Bool("status_proposer", atomic.LoadUint32(&p.status) == StatusProposer),
	)

	if nack > 0 && nack+ack >= quorum(N, ack, nack) && atomic.LoadUint32(&p.status) == StatusProposer {
		return false
	}
	if ack < quorum(N, ack, nack) && atomic.LoadUint32(&p.status) == StatusProposer {
		return false
	}
	return true
}

func (p *Process) Operate() {
	// var ack = make([][]uint64, 8)
	// if m.SenderPID >= uint64(len(ack)){
	// 	ack = append(ack, make([]uint64, 0))
	// }
	// if m.SeqNo >= uint64(len(ack[m.SenderPID])) {
	// 	ack[m.SenderPID] = append(ack[m.SenderPID],  0)
	// }
	// if m.Ack {
	// 	ack[m.PID][m.SeqNo]++
	// }
	for {
		select {
		case <-p.proposing:
			<-p.proposing // waiting till proposing finished
		case msg := <-p.neighbours.RecvInput(p.id):
			switch msg.(type) {
			case *proposal:
				m, _ := msg.(*proposal)
				p.Accept(m.SenderPID, m)
			case *responseMessage:
				m, _ := msg.(*responseMessage)
				p.log.Debug("received response", zap.Bool("ack", m.Ack), zap.Uint64("seq_no", m.SeqNo))
			default:
				p.log.Debug("received msg of unknown type")
			}
		}
	}
}

func (p *Process) LastSeq(ownedAccount uint64) uint64 {
	p.lattice.mu.RLock()
	v, ok := p.lattice.accounts[ownedAccount]
	if !ok {
		p.log.Panic("account didn't initialize his mem", zap.Uint64("owned_account", ownedAccount))
	}
	p.lattice.mu.RUnlock()
	return v.T[len(v.T)-1].SeqID
}

func (p *Process) Balance(ownedAccount uint64) uint64 {
	p.lattice.mu.RLock()
	v := p.lattice.accounts[ownedAccount]
	p.lattice.mu.RUnlock()
	var s uint64
	for _, tx := range v.T {
		switch ownedAccount {
		case tx.Sender:
			s -= tx.Amount
		case tx.Receiver:
			s += tx.Amount
		}
	}
	return s
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
	if len(prev.T) > len(next.T) {
		// next element should be superset or equal to previous
		// pass further to evaluate distance, but we should return
		// right after distance zero check
		err = ErrNextNotSuperset
	}

	latestPrev, latestNext := prev.T[len(prev.T)-1], next.T[len(next.T)-1]
	fmt.Printf("lprev=%+v lnext=%+v\n", latestPrev, latestNext)
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

	n := next.T[i]
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
	return uint64(math.Floor((float64(N) + 1.0) / 2.0))
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
