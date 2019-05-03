package main

import (
	"github.com/awskii/vo/la"
	"go.uber.org/zap"
)

func main() {
	proc := make([]la.Process, 5)
	netw := la.NewNeighboursLocal()

	log, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	defer log.Sync()

	for i := 1; i < len(proc)+1; i++ {
		i64 := uint64(i)
		l := la.NewLattice(i64)
		for j:= 1; j < len(proc)+1; j++ {
			l.AddAccount(uint64(j))
		}

		proc[i] = *la.NewProcess(i64, l, netw, log.With(zap.Skip()))
		netw.AddNew(i64)
	}
	log.Sugar().Infof("initialized %d processes, ready for operate\n", len(proc), len(proc))

	// каждую секунду создается перевод с рандомного А на рандомный Б 1 единицы
	// следим за счетчиками на аккаунтах и seq_id
	// - присвоить мин баланс всем аккам
	// - придумать, как толкать процесс пропозить что-то
	// - дополнить алгоритм GLA
}
