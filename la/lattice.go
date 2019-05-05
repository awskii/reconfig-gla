package la

import (
	"fmt"
	"math"
	"sync"

	"github.com/pkg/errors"
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
	// holds non-negative validate restriction (Validity property)
	ErrMergeNegativeBalance = errors.New("trying to merge element which leads to balance(a) < 0")
	ErrNegativeBalance      = errors.New("negative balance")
)

const (
	// StatusAcceptor means that process can only handle
	// requests from network, but can not propose new values
	StatusAcceptor uint32 = 0
	// StatusProposer means that process proposing new value now
	// and can't handle Accept calls from other processes
	StatusProposer uint32 = 1
)

// Element reprsents one lattice element
type Element struct {
	accountID uint64 // defines owner of a set T
	T         []Tx   // Holds incoming and outgoing tx's
	mu        sync.Mutex
	// Could be extended with Replecated Data Types:
	//  - u,   ownership function(?) implementation
	//  - Pa,  currently available processes
	//  - Prm, currently deleted processes
}

// validate fails if there are monotonicity violation
// it' s a caller responsiveness to make it thread safe.
func (e *Element) validate() (balance uint64, err error) {
	var (
		s uint64
		p uint64
	)

	fmt.Printf("validate v=%+v\n", e.T)
	for i := 0; i < len(e.T); i++ {
		if i == 0 {
			p = e.T[i].SeqNo
		}
		if e.T[i].SeqNo != p {
			fmt.Printf("prev=%d cur=%d v=%v\n", p, e.T[i].SeqNo, e.T[i])
			return 0, errors.Errorf("monotonicity violation at %d!%+v", e.accountID, e.T[i])
		}
		switch e.accountID {
		case e.T[i].Receiver:
			balance += e.T[i].Amount
		case e.T[i].Sender:
			s += e.T[i].Amount
		}
		p++
	}
	if s > balance {
		fmt.Printf("%p %+v\n", e, e)
		return 0, ErrNegativeBalance
	}
	return balance - s, nil
}

func (e *Element) head() *Tx {
	e.mu.Lock()
	defer e.mu.Unlock()
	return &e.T[len(e.T)-1]
}

// mergeTxx returns diff if exists, nil otherwise (and set merged version as current)
func (e *Element) mergeTxx(tx []Tx) (diff []Tx, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if diff = e.diff(tx); diff != nil {
		return diff, errors.Errorf("can not merge: conflict tx %d!%+v", e.accountID, diff)
	}

	tmp := &Element{
		T:         append(e.T, tx...),
		accountID: e.accountID,
	}
	if _, err := tmp.validate(); err != nil {
		return nil, errors.Wrap(err, "can not merge - validation error")
	}

	fmt.Printf("old=%v new=%v\n", e.T, tmp.T)
	e.T = tmp.T
	return nil, nil
}

func (e *Element) diff(their []Tx) []Tx {
	res := make([]Tx, 0, 4)
	prev := uint64(0)
	for _, t := range their {
		if t.SeqNo >= uint64(len(e.T)) {
			continue
		}

		if t.SeqNo-prev != 1 { // monotonicity check
			res = append(res, e.T[prev+1])
			continue
		}

		m := e.T[t.SeqNo]
		if m.Sender != t.Sender || m.Receiver != t.Receiver || m.Amount != t.Amount {
			res = append(res, m)
		}
		prev = t.SeqNo
	}
	if len(res) == 0 {
		return nil
	}
	return res
}

// Tx represents transaction
type Tx struct {
	Sender   uint64 // sender account ID
	Receiver uint64 // receiver account ID
	Amount   uint64 // tx amount
	SeqNo    uint64 // tx sequence number for Sender account
}

type PartlyOrderedSet struct {
	mu    sync.RWMutex
	s     map[uint64]*Element
	epoch uint64
}

func NewPartlyOrderedSet() *PartlyOrderedSet {
	return &PartlyOrderedSet{
		s: make(map[uint64]*Element),
	}
}

func (ps *PartlyOrderedSet) add(accountID uint64) {
	ps.mu.Lock()
	ps.s[accountID] = &Element{accountID: accountID, T: make([]Tx, 0)}
	ps.mu.Unlock()
}

// write adds new account to the lattice
func (ps *PartlyOrderedSet) write(accountID uint64, el Element) {
	ps.mu.Lock()
	ps.s[accountID].mu.Lock()
	ps.s[accountID].T = append(ps.s[accountID].T, el.T...)
	ps.s[accountID].mu.Unlock()
	ps.mu.Unlock()
}

func (ps *PartlyOrderedSet) copy() *PartlyOrderedSet {
	ps.mu.Lock()
	cp := &PartlyOrderedSet{
		s:     make(map[uint64]*Element),
		epoch: ps.epoch,
	}
	for k, v := range ps.s {
		cp.s[k] = &Element{accountID: v.accountID, T: v.T}
	}
	ps.mu.Unlock()
	return cp
}

// ps knows nothing about it's owner, it's a shared knowledge.
// Payment returns verified proposal based on current POS state,
// ready to broadcast to another units that value.
func (ps *PartlyOrderedSet) payment(from, to, amount uint64) ([]Element, error) {
	// check if 'from' account has enough validate
	if ps.balance(from) < amount {
		return nil, ErrMergeNegativeBalance // pffth
	}

	// POS should write two new items to itself to finish transfer
	// and it should be atomic
	sn := ps.latestSeqNo(from, to)
	els := make([]Element, len(sn))
	for i := 0; i < len(els); i++ {
		els[i] = Element{
			T: []Tx{{Sender: from, Receiver: to, Amount: amount, SeqNo: sn[i] + 1}},
		}
	}
	els[0].accountID = from
	els[1].accountID = to

	return els[:2], nil
}

func wrapTx(tx []Tx) []Element {
	e := make(map[uint64]Element, 0)
	for i := 0; i < len(tx); i++ {
		v, ok := e[tx[i].Sender]
		if !ok {
			v = Element{accountID: tx[i].Sender, T: make([]Tx, 0)}
		}
		v.T = append(v.T, tx[i])
	}
	s := make([]Element, len(e))
	i := 0
	for _, el := range e {
		s[i] = el
		i++
	}
	return s
}

func (ps *PartlyOrderedSet) merge(set []Element) ([]Element, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	df := make([]Tx, 0)
	aux := make([]*Element, len(set))
	for i := 0; i < len(set); i++ {
		// copy current values for accounts in 'set'
		v := ps.s[set[i].accountID]
		aux[i] = &(*v)

		diff, err := aux[i].mergeTxx(set[i].T)
		if err != nil {
			fmt.Println(err)
			df = append(df, diff...)
		}
	}
	if len(df) != 0 {
		return wrapTx(df), errors.New("merge: failed due to conflicts")
	}

	// validation finished, need to merge to ours
	for _, el := range aux {
		ps.s[el.accountID].T = el.T
		/*		ps.s[el.accountID].mu.Lock()
		ps.s[el.accountID].T = append(ps.s[el.accountID].T, el.T...)
		ps.s[el.accountID].mu.Unlock()*/
	}
	return nil, nil
}

func (ps *PartlyOrderedSet) balance(accountID uint64) uint64 {
	a := ps.read(accountID)
	if accountID != a.accountID {
		return 0 // malformed key link
	}
	b, err := a.validate()
	if err != nil {
		// this error should not occur in operating PartlyOrderedSet
		// bc we validating all elements during the merge operation
		panic(err)
	}
	return b
}

// returns list of latest sequence numbers for every account in a
func (ps *PartlyOrderedSet) latestSeqNo(a ...uint64) []uint64 {
	v := make([]uint64, len(a))
	ps.mu.RLock()
	for i, account := range a {
		s := ps.s[account]
		v[i] = (*s.head()).SeqNo
	}
	ps.mu.RUnlock()
	return v
}

func (ps *PartlyOrderedSet) read(accountID uint64) Element {
	ps.mu.RLock()
	v := ps.s[accountID]
	ps.mu.RUnlock()
	return *v
}

// diff returns ps vision on transaction set, if it differs from provided 'their' version
// func (ps *PartlyOrderedSet) diff(their *Element) Element {
// 	if their == nil {
// 		return nil
// 	}
// 	ps.mu.RLock()
// 	for i := 0; i < len(their) ; i++ {
// 		if their[i].SeqNo > uint64(len(their) -1) {
// 			ps.s[their[i].Sender] = ps.
// 		}
//
// 	}
//
//
// }

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
	d := latestNext.SeqNo - latestPrev.SeqNo
	if d == 0 {
		panic(fmt.Errorf("ordering violation at seq_no=%d", latestNext.SeqNo))
	}
	if err != nil {
		return int(d), err
	}
	if d > 1 {
		// panic(fmt.Errorf("ordering violation: our seq_no=%d, their seq_no=%d",
		// 	latestPrev.SeqNo, latestNext.SeqNo))
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
//  - that merge(prev, next) result has positive validate
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
		return fmt.Errorf("their tx seq_no=%d: invalid Sender", b.SeqNo)
	}
	if a.Receiver != b.Receiver {
		return fmt.Errorf("their tx seq_no=%d: invalid Receiver", b.SeqNo)
	}
	if a.Amount != b.Amount {
		return fmt.Errorf("their tx seq_no=%d: invalid Amount", b.SeqNo)
	}
	if a.SeqNo != b.SeqNo {
		return fmt.Errorf("their tx seq_no=%d: invalid SeqNo", b.SeqNo)
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

	for i, j := 0, next.T[len(next.T)-1].SeqNo; j < uint64(len(prev.T)); i, j = i+1, j+1 {
		diff.T[i] = prev.T[j]
	}
	return diff, nil
}

// simple func to easily change quorum conditions
func quorum(N uint64) uint64 {
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
