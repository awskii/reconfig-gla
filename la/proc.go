package la

import (
	"sync/atomic"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Process holds all needed data and states
// to provide communication with another processes
// and reach generalized lattice agreement
type Process struct {
	id         uint64     // UUID, process identifier (PID)
	status     uint32     // current process status {Proposer, Acceptor}
	lattice    *Agreement // Agreement structure for, implementation of a GLA
	neighbours Neighbours // map of available servers to P2P communication with
	proposing  chan struct{}
	log        *zap.Logger
	bcastresp chan response
	// CurrentQuorumID int -- should be added later. All quorum stuff should be
	// moved into separated module about Network: P2P state, known active host list,
	// list of quorums in currently active configuration and other.
}

func NewProcess(id uint64, l *Agreement, n Neighbours, log *zap.Logger) *Process {
	log = log.With(zap.Uint64("pid", id))// , zap.Uint64("owned_account_id", l.ownedAccount))
	log.Info("process initiated")

	return &Process{
		id:         id,
		lattice:    l,
		neighbours: n,
		proposing:  make(chan struct{}),
		bcastresp: make(chan response),
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
func (p *Process) Propose(ownedAccountID, a, b, x uint64) error {
	if !atomic.CompareAndSwapUint32(&p.status, StatusAcceptor, StatusProposer) {
		return errors.New("currently proposing")
	}
	p.log.Debug("set process_status=proposer OK")
	p.proposing <- struct{}{} // notify that process starting his proposition

	defer func() {
		atomic.StoreUint32(&p.status, StatusAcceptor)
		p.log.Debug("set process_status=acceptor OK")
		p.proposing <- struct{}{}
	}()

	// TODO(awskii): process should own that account and quorum should approve it
	// TODO(awskii): when proposal become more useful, move broadcast into lattice Propose function
	prop, err := p.lattice.makeProposal(p.id, a,b, x)
	if err != nil {
		return err
	}
	p.log.Sugar().Debugf("proposal %+v", prop)
	return p.lattice.Propose(p.id, p.predicate, p.neighbours, p.bcastresp)
}

// simple func to easily check if some 'predicate' holds on proposal
func (p *Process) predicate(N, ack, nack uint64) bool {
	p.log.Debug("predicate check",
		zap.Uint64("N", N),
		zap.Uint64("ack", ack),
		zap.Uint64("nack", nack),
		zap.Uint64("quorum", quorum(N)),
		zap.Bool("status_proposer", atomic.LoadUint32(&p.status) == StatusProposer),
	)

	if nack > 0 && nack+ack >= quorum(N) && atomic.LoadUint32(&p.status) == StatusProposer {
		return false
	}
	if ack < quorum(N) && atomic.LoadUint32(&p.status) == StatusProposer {
		return false
	}
	return true
}

func (p *Process) Operate() {
	for {
		select {
		case <-p.proposing:
			<-p.proposing // waiting till proposing finished
		case msg := <-p.neighbours.RecvInput(p.id):
			switch msg.(type) {
			case *proposal:
				m, _ := msg.(*proposal)
				if p.id == m.SenderPID {
					continue
				}
				p.Accept(m.SenderPID, m)
			case *response:
				m, _ := msg.(*response)
				p.log.Debug("received response", zap.Bool("ack", m.Ack), zap.Uint64("seq_no", m.SeqNo))
				p.bcastresp <- *m
			default:
				p.log.Debug("received msg of unknown type")
			}
		}
	}
}

func (p *Process) Balance(ownedAccount uint64) uint64 {
	return p.lattice.v.balance(ownedAccount)
}

