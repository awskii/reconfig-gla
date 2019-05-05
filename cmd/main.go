package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/awskii/vo/la"
	"go.uber.org/zap"
)

func main() {
	proc := make([]la.Process, 5)

	log, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	defer log.Sync()

	netw := la.NewNeighboursLocal(log.With(zap.String("module", "net")))

	for i := 1; i < len(proc)+1; i++ {
		i64 := uint64(i)
		l := la.NewAgreement(i64, log.With(zap.String("module", "lattice")))

		for j := 1; j < len(proc)+1; j++ {
			l.AddAccount(uint64(j))
		}

		proc[i-1] = *la.NewProcess(i64, l, netw, log.With(zap.Skip()))
		netw.AddNew(i64)
	}
	log.Sugar().Infof("%d processes are initialized", len(proc))

	for i := 0; i < len(proc); i++ {
		go proc[i].Operate()
	}

	// - дополнить алгоритм GLA

	rn := rand.New(rand.NewSource(time.Now().UnixNano()))
	ur := func() uint64 {
		return uint64(rn.Intn(len(proc))) + 1
	}

	round := 0
	x := uint64(1)
	for range time.Tick(time.Second * 5) {
		round++
		l := log.With(zap.Int("round", round))

		// print balances
		var balances []zap.Field
		for i := 1; i < len(proc)+1; i++ {
			b := make([]uint64, len(proc))
			for j := 0; j < len(proc); j++ {
				b[j] = proc[i-1].Balance(uint64(j + 1))
			}
			balances = append(balances, zap.Uint64s(fmt.Sprintf("proc/%d", i), b))
		}
		l.Info("balances", balances...)

		a, b := ur(), ur()
		if a == b {
			b = ur()
			if a == b {
				l.Info("skip round", zap.Uint64("pid", a))
				continue
			}
		}

		if err := proc[a-1].Propose(a, a, b, x); err != nil {
			l.Sugar().Errorf("proposal rejected receiver=%d round=%d err=%v", a, round, err)
			continue
		}
		l.Info("system progress step")
	}
}
