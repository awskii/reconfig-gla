package la

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Predicate func(N, ack, nack uint64) bool

// Agreement should be initalized in every working process
type Agreement struct {
	// currently owned account. Further will be added
	// account migration among processes, but this variable
	// will exists anyway to hold 'One process can holds many
	// accounts, but only one processing at a time'
	ownedAccount uint64
	v            *PartlyOrderedSet

	// auxiliary values for GLA
	activeValue   *PartlyOrderedSet
	acceptedValue *PartlyOrderedSet
	outputValue   *PartlyOrderedSet

	curProposal *proposal

	proposedValue *PartlyOrderedSet
	proposalSeqNo uint64
	ack, nack     uint64
	log           *zap.Logger
}

func NewAgreement(owner uint64, log *zap.Logger) *Agreement {
	return &Agreement{
		v:   NewPartlyOrderedSet(),
		log: log.With(zap.Uint64("pid", owner)),

		ownedAccount: owner,
	}
}

func (ag *Agreement) AddAccount(aid uint64) {
	ag.v.add(aid)
	ag.v.write(aid, Element{
		accountID: aid, T: []Tx{{Sender: 0, Receiver: aid, Amount: 20, SeqNo: 1}}})
}

// Accept func from original GLA
func (ag *Agreement) Accept(proposerID, myPID uint64, proposal *proposal, netw Neighbours) (bool, error) {
	cp := atomic.LoadUint64(&ag.proposalSeqNo)
	if proposal.SeqNo != cp {
		return false, fmt.Errorf("current proposal seq_no=%d provided seq_no=%d", cp, proposal.SeqNo)
	}

	// TODO need to check if we processing currently proposed value
	msg := &response{
		PID:   myPID,
		SeqNo: proposal.SeqNo,
		Value: proposal.Value,
	}

	ag.log.Debug("validating proposal",
		zap.Uint64("proposal_id", proposal.SeqNo),
		zap.Uint64("pid", ag.ownedAccount))

	// TODO where the fuck is check on proposer == account_owner?
	// maybe we really need extra aux values like accepted etc??
	diff, err := ag.v.merge(proposal.Value)
	if err != nil {
		ag.log.Sugar().Debugf("can not accept err=%v diff=%+v", err, diff)
		msg.Value = diff
		ag.log.Debug("sending NACK", zap.Uint64("proposal_id", proposal.SeqNo))

		netw.Respond(myPID, msg)
		return false, err
	}
	msg.Ack = true
	ag.log.Debug("sending ACK", zap.Uint64("proposal_id", proposal.SeqNo))
	netw.Respond(myPID, msg)
	return msg.Ack, nil
}

func (ag *Agreement) Propose(myPID uint64, predicate Predicate, netw Neighbours, resp chan response) error {
	// TODO: seems that that check in wrong place in mine algo
	if atomic.LoadUint64(&ag.proposalSeqNo) != 0 {
		return errors.New("currently proposing, parallel proposition is restricted")
	}

	ag.proposedValue = ag.v.copy()
	_, err := ag.proposedValue.merge(ag.curProposal.Value)
	if err != nil {
		return errors.Wrap(err, "propose")
	}

rePropose:
	N := netw.N()
	ctx, cancelBcast := context.WithCancel(context.Background())
	log := ag.log.With(zap.Uint64("proposal_seq_no", ag.curProposal.SeqNo))

	respp := netw.Bcast(ctx, ag.curProposal)
	for r := range respp {
		rm, ok := r.(*response)
		if !ok || rm.SeqNo != ag.curProposal.SeqNo {
			continue
		}
		if rm.SeqNo != ag.curProposal.SeqNo {
			continue
		}

		if rm.Ack {
			// we could count ack/nack in proposal to get more elegant solution
			ag.ack++
		} else {
			ag.nack++
			// what if conflict occured here???
			_, err := ag.proposedValue.merge(rm.Value)
			if err != nil {
				log.Error("can not update proposed value", zap.Error(err))
			}
			// TODO FILL PROPOSAL VALUE
			cancelBcast()
			ag.nextRound()
			goto rePropose
		}
		ag.log.Debug("gossip", zap.Uint64("ack", ag.ack), zap.Uint64("nack", ag.nack))

		if predicate(N, ag.ack, ag.nack) {
			break
		}
	}
	if !predicate(N, ag.ack, ag.nack) {
		log.Debug("proposal has been rejected")
		ag.proposedValue.s[1].T[0].Amount = 100000
		return nil
	}
	log.Debug("merge proposal")
	d, err := ag.v.merge(ag.curProposal.Value)
	if err != nil {
		log.Sugar().Errorf("merging error=%v diff=%+v", err, d)
	}
	return err
}

func (ag *Agreement) Merge(accountID uint64, elset []Element) ([]Element, error) {
	return ag.v.merge(elset)
}

// bad interface. need to rewrite it to get rid of opportunity to interrupt proposition duriong
// time window since exit from makeProposal till starting Propose
func (ag *Agreement) makeProposal(senderPID, a, b, x uint64) (*proposal, error) {
	ag.curProposal = &proposal{
		SenderPID: senderPID,
		SeqNo:     ag.proposalSeqNo,
	}

	value, err := ag.v.payment(a, b, x)
	if err != nil {
		return nil, err
	}
	ag.curProposal.Value = value
	return ag.curProposal, nil
}

func (ag *Agreement) nextRound() {
	atomic.AddUint64(&ag.proposalSeqNo, 1)
	atomic.AddUint64(&ag.curProposal.SeqNo, 1)
	ag.ack, ag.nack = 0, 0
}
