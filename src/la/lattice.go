package lattice

import (
	"errors"
	"fmt"
)

var (
	ErrUnknownAccountID     = errors.New("unknown account id")
	ErrNonMonotonicSeqID    = errors.New("non-monotonic sequence id")
	ErrNextNotSuperset      = errors.New("received element should be superset of previous one")
	ErrMissedMessages       = errors.New("some missed messages are missed or not delivered yet")
	ErrMergeNegativeBalance = errors.New("trying to merge element which leads to balance(a) < 0")
)

// Lattice should be initalized in every working process
type Lattice struct {
	accounts map[uint64]*Element // by account ID store their ordered by SeqID transactions.
	version  uint64
}

// Element reprsents one lattice element
type Element struct {
	T []Tx // Holds incoming and outgoing tx's
	// Could be extended with Replecated Data Types:
	//  - u,   ownership function(?) implementation
	//  - Pa,  currently available processes
	//  - Prm, currently deleted processes
}

// Tx represents transaction
type Tx struct {
	Sender   uint64 // sender account ID
	Reciever uint64 // reciever account ID
	Amount   uint64 // tx amount
	SeqID    uint64 // tx sequence number
}

// Merge used to merge element e with sub-semi-lattice of an
// There are some restictions on operation:
//  - after merging sender account balance should be >= 0
//  - element e should be a superset of a lattice element at position pos
//  - ???
//
// Merge can return
//   ErrMissedMessages       -- we missed some messagesshould we do something here?
//   ErrNextNotSuperset      -- their miss some messages, should be refined
//   ErrUnknownAccountID     -- errors.New("unknown account id")
//   ErrMergeNegativeBalance -- errors.New("trying to merge element which leads to balance(a) < 0")
func (l *Lattice) Merge(accountID uint64, next Element) (version uint64, err error) {
	v, ok := l.accounts[accountID] // RACE
	if !ok {                       // unknown account
		return l.version, ErrUnknownAccountID
	}

	join, err := merge(accountID, v, &next)
	if err != nil {
		return l.version, err
	}
	l.accounts[accountID] = join // RACE
	return l.version, nil
}

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
		return int(d), ErrMissedMessages
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
		case p.Reciever:
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
	case n.Reciever:
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
	if a.Reciever != b.Reciever {
		return fmt.Errorf("their tx seq_no=%d: invalid Receiver", b.SeqID)
	}
	if a.Amount != b.Amount {
		return fmt.Errorf("their tx seq_no=%d: invalid Amount", b.SeqID)
	}
	if a.SeqID != b.SeqID {
		return fmt.Errorf("their tx seq_no=%d: invalid Amount", b.SeqID)
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
		return nil, ErrMissedMessages
	}

	diff := &Element{
		T: make([]Tx, dist),
	}

	for i, j := 0, next.T[len(next.T)-1].SeqID; j < uint64(len(prev.T)); i, j = i+1, j+1 {
		diff.T[i] = prev.T[j]
	}
	return diff, nil
}
