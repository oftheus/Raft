package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	peerMorto  int32
	termoAtual int
	votouEm    int
	logs       []LogEntrada
	idLider    int
	estado     int

	timerEleicao    *time.Timer
	heartbeatTicker *time.Ticker
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotTerm  int
	SnapshotIndex int

	SnapshotValid bool
	Snapshot      []byte
}

type LogEntrada struct {
	Term int
	Data interface{}
}

func (rf *Raft) GetState() (term int, ehLider bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.termoAtual
	ehLider = rf.estado == 2
	return term, ehLider
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
}

type AppendArgsEntradas struct {
	Term             int
	IdLider          int
	IndexLogAnterior int
	TermoLogAnterior int
	Entradas         []LogEntrada
	IndexCommitLider int
}

type AppendReplyEntradas struct {
	Term    int
	Sucesso bool
}

func (rf *Raft) enviarAppendEntradas() {
	rf.mu.Lock()

	args := AppendArgsEntradas{
		Term:     rf.termoAtual,
		IdLider:  rf.me,
		Entradas: rf.logs,
	}
	rf.mu.Unlock()
	for peer := 0; peer < len(rf.peers); peer++ {
		reply := AppendReplyEntradas{}
		go func(peerId int) {
			if peerId == rf.me {
				return
			}
			rf.peers[peerId].Call("Raft.AppendEntradas", &args, &reply)
		}(peer)
	}
}

type RequestVoteArgs struct {
	Term           int
	IdCandidato    int
	IndexUltimoLog int
	TermoUltimoLog int
}

type RequestVoteReply struct {
	Term          int
	VotoConcedido bool
}

func (rf *Raft) AppendEntradas(args *AppendArgsEntradas, reply *AppendReplyEntradas) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.termoAtual
	if args.Term < rf.termoAtual {
		reply.Sucesso = false
	} else {
		reply.Sucesso = true
		rf.ReiniciarTimerEleicao()
		rf.estado = 1
		rf.termoAtual = args.Term
		rf.idLider = args.IdLider
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.termoAtual
	reply.VotoConcedido = false

	if args.Term < rf.termoAtual {
		return
	}

	if args.Term > rf.termoAtual {
		rf.estado = 1
		rf.votouEm = -1
		rf.idLider = -1
		rf.termoAtual = args.Term
	}

	if rf.votouEm < 0 || rf.votouEm == args.IdCandidato {
		lastLogTerm := 0
		if len(rf.logs) > 0 {
			lastLogTerm = rf.logs[len(rf.logs)-1].Term
		}

		if args.TermoUltimoLog > lastLogTerm {
			reply.VotoConcedido = true
			rf.votouEm = args.IdCandidato
			return
		}

		if args.TermoUltimoLog == lastLogTerm && len(rf.logs) <= args.IndexUltimoLog {
			reply.VotoConcedido = true
			rf.votouEm = args.IdCandidato
			return
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	ehLider := true
	return index, term, ehLider
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.peerMorto, 1)
	DPrintf("Servidor %v está encerrado", rf.me)
	rf.timerEleicao.Stop()
}

func (rf *Raft) encerrado() bool {
	z := atomic.LoadInt32(&rf.peerMorto)
	return z == 1
}

func (rf *Raft) ReiniciarTimerEleicao() {
	if rf.timerEleicao.Stop() {
		DPrintf("timer Eleição foi parado para o servidor %v", rf.me)
		select {
		case <-rf.timerEleicao.C:
			DPrintf("O canal de tempo do timer Eleição para o servidor %v está drenado.", rf.me)
		default:
			DPrintf("O canal de tempo do timer Eleição para o servidor %v está vazio.", rf.me)
		}
	}

	rf.timerEleicao.Reset(time.Duration(300+rand.Int31n(150)) * time.Millisecond)
}

func (rf *Raft) ticker() {
	for !rf.encerrado() {
		select {
		case <-rf.timerEleicao.C:
			DPrintf("O timer de eleição do servidor %v disparou", rf.me)

			rf.mu.Lock()
			rf.ReiniciarTimerEleicao()
			if rf.estado == 2 {
				rf.mu.Unlock()
				break
			}

			DPrintf("O servidor %v está iniciando uma eleição", rf.me)

			rf.estado = 1
			rf.termoAtual++
			rf.votouEm = rf.me
			votoRecebido := 1
			votoConcedido := 1
			canalDeResultadoDoVoto := make(chan bool)
			lastLogTerm := 0

			if len(rf.logs) > 0 {
				lastLogTerm = rf.logs[len(rf.logs)-1].Term
			}

			votoArgs := RequestVoteArgs{
				Term:           rf.termoAtual,
				IdCandidato:    rf.me,
				IndexUltimoLog: len(rf.logs),
				TermoUltimoLog: lastLogTerm,
			}
			rf.mu.Unlock()
			for peer := 0; peer < len(rf.peers); peer++ {
				if peer == rf.me {
					continue
				}
				go func(peerId int) {
					votoReply := RequestVoteReply{}
					ok := rf.sendRequestVote(peerId, &votoArgs, &votoReply)
					if ok {
						canalDeResultadoDoVoto <- votoReply.VotoConcedido
					} else {
						canalDeResultadoDoVoto <- false
					}
				}(peer)
			}

			for {
				resultado := <-canalDeResultadoDoVoto
				votoRecebido++
				if resultado {
					votoConcedido++
				}
				if votoConcedido > len(rf.peers)/2 {
					break
				}
				if votoRecebido >= len(rf.peers) {
					break
				}
			}

			rf.mu.Lock()
			if rf.estado != 1 {
				DPrintf("Servidor %v não é mais um candidato", rf.me)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if votoConcedido > len(rf.peers)/2 {
				rf.mu.Lock()
				rf.estado = 2
				rf.ReiniciarTimerEleicao()
				rf.mu.Unlock()
				DPrintf("Servidor %v ganhou a eleição para o termo %v com %v votos.", rf.me, rf.termoAtual, votoConcedido)
				rf.enviarAppendEntradas()
			}

		case <-rf.heartbeatTicker.C:
			rf.mu.Lock()
			DPrintf("Servidor %v no estado %v acionou o heartbeat ticker", rf.me, rf.estado)
			estado := rf.estado
			rf.mu.Unlock()
			if estado == 2 {
				DPrintf("Líder %v heartbeatTicker tick", rf.me)
				rf.enviarAppendEntradas()
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.estado = 0
	rf.votouEm = -1
	rf.termoAtual = 1
	rf.timerEleicao = time.NewTimer(time.Duration(300+rand.Int31n(150)) * time.Millisecond)
	rf.heartbeatTicker = time.NewTicker(100 * time.Millisecond)

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
