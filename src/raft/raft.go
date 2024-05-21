package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"trabalhoRaft/labrpc"
)

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	/*'peerMorto', para indicar se o nó (peer) está "morto".
	Isso pode ser usado para finalizar operações em andamento de maneira segura
	 ou ignorar o nó em processos de consenso, de modo a promover uma implementação
	 segura de paralelismo e gerenciamento de estado. */
	peerMorto int32

	/* 'termoAtual', para indicar o termo atual do nó, com o intuito de realizar
	comparações e atualizações durante eleições e replicação de logs. */
	termoAtual int

	/* 'heartbeatTicker', com o objetivo de enviar regularmente mensagens de heartbeat
	do líder para os seguidores, garantindo que o líder permanece como líder e para
	manter os seguidores informados e evitar eleições desnecessárias  */
	heartbeatTicker *time.Ticker

	/* 'idLider' com o intuito de indicar quem é o líder no momento, o que é bastante
	útil para direcionar requisições de clientes e para que os seguidores saibam
	para quem enviar entradas de log.  */
	idLider int

	/* 'logs' para manter as entradas de log que foram replicadas entre os servidores.
	Cada entrada de log representa uma operação a ser aplicada ao estado do sistema. */
	logs []LogEntrada

	/*  'timerEleicao' que é um timer para iniciar uma nova eleição quando um nó
	seguidor não recebe heartbeat de um líder dentro de um intervalo de tempo específico.*/
	timerEleicao *time.Timer

	/* 'votouEm' para indicar o ID do candidato para quem este nó votou na eleição atual.
	Isso é crucial para evitar votos duplicados e para contabilizar votos corretamente.  */
	votouEm int

	//'estado, mostrar se é seguidor(0); candidato (1); líder (2)
	estado int
}

/*
	'logEntrada', utilizada para representar uma única entrada no registro de

log de um nó em um sistema distribuído, contendo informações sobre o termo
ao qual pertence e os dados associados a essa entrada.
*/
type LogEntrada struct {
	Term int
	Data interface{}
}

/*
para encerrar uma instância do servidor
utiliza uma operação atômica para definir o valor do campo peerMorto como 1
chama o método Stop no objeto rf.timerEleicaoleicao, que é um timer usado para controlar o tempo de eleição
*/
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.peerMorto, 1)
	rf.timerEleicao.Stop()
}

/*
para verifica se o servidor Raft foi encerrado
usa uma operação atômica para ler o valor do campo peerMorto
retorna true se o valor de peerMorto for 1, indicando que o servidor foi encerrado, e false caso contrário
*/
func (rf *Raft) encerrado() bool {
	z := atomic.LoadInt32(&rf.peerMorto)
	return z == 1
}

/*
implementa um loop que é responsável por monitorar o estado do servidor
e reagir a eventos de temporização para o controle de eleições
*/
func (rf *Raft) ticker() {
	for !rf.encerrado() {
		select {
		case <-rf.timerEleicao.C:
			rf.mu.Lock()
			rf.ReiniciarTimerEleicao()
			if rf.estado == 2 {
				rf.mu.Unlock()
				break
			}
			rf.estado = 1
			rf.termoAtual++
			rf.votouEm = rf.me
			votoRecebido := 1
			votoConcedido := 1
			canalDeResultadoDoVoto := make(chan bool)

			ultimoLogTermo := 0

			if len(rf.logs) > 0 {
				ultimoLogTermo = rf.logs[len(rf.logs)-1].Term
			}

			votoArgs := RequestVoteArgs{
				Term:           rf.termoAtual,
				IdCandidato:    rf.me,
				IndexUltimoLog: len(rf.logs),
				TermoUltimoLog: ultimoLogTermo,
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
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if votoConcedido > len(rf.peers)/2 {
				rf.mu.Lock()
				rf.estado = 2
				rf.ReiniciarTimerEleicao()
				rf.mu.Unlock()
				rf.enviarAppendEntradas()
			}

		case <-rf.heartbeatTicker.C:
			rf.mu.Lock()
			estado := rf.estado
			rf.mu.Unlock()
			if estado == 2 {
				rf.enviarAppendEntradas()
			}
		}
	}
}

/*
A função RequestVote implementa a lógica para processar uma solicitação de voto recebida por um nó.
Primeiro, ela adquire uma trava para garantir que apenas um goroutine a execute por vez.
Em seguida, ela define inicialmente que o voto não foi concedido.
Em seguida, ela verifica se o termo da solicitação é menor que o termo atual do nó;
se for, a função simplesmente retorna.
Se o termo da solicitação for maior do que o termo atual, o nó atualiza seu estado para "seguidor",
reseta o voto concedido e o ID do líder, e atualiza seu termo atual para o termo da solicitação.
Em seguida, o nó verifica se ainda não votou ou se já votou no candidato da solicitação.
Se sim, verifica se o termo do último log no candidato é maior que o seu próprio último termo de log,
ou se ambos os termos de log são iguais, mas o índice do último log do candidato é igual ou maior que
o seu próprio índice de último log. Se uma dessas condições for atendida, o nó concede o voto ao
candidato e atualiza seu voto para o candidato.
*/
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
		ultimoLogTermo := 0
		if len(rf.logs) > 0 {
			ultimoLogTermo = rf.logs[len(rf.logs)-1].Term
		}

		if args.TermoUltimoLog > ultimoLogTermo {
			reply.VotoConcedido = true
			rf.votouEm = args.IdCandidato
			return
		}

		if args.TermoUltimoLog == ultimoLogTermo && len(rf.logs) <= args.IndexUltimoLog {
			reply.VotoConcedido = true
			rf.votouEm = args.IdCandidato
			return
		}
	}
}

/*
É responsável por enviar as entradas de log para os peers.
Primeiro, ela constrói os argumentos a serem enviados para os peers, incluindo o termo atual,
o id do líder e as entradas de log. Em seguida, ela itera sobre todos os peers e, para cada um deles,
cria uma goroutine anônima que chama o método "AppendEntradas" no respectivo peer utilizando RPC,
enviando os argumentos construídos anteriormente.
*/
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

/*
Implementa a lógica de recebimento e processamento de msgs de entrada.
É responsável por atualizar o estado do nó de acordo com os argumentos fornecidos.
Inicialmente, ela verifica se o termo recebido na msg é menor do que o termo atual do nó
  - se for, indica que a operação falhou.
  - se n for, atualiza o estado do nó para indicar que está seguindo um líder válido,
    reinicia o temporizador de eleição e atualiza as informações do termo atual e do líder.
*/
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

/*
	retorna duas variáveis:

- term, que representa o termo atual do servidor
- ehLider, um booleano que indica se o servidor é o líder atual ou não.
*/
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

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotTerm  int
	SnapshotIndex int

	SnapshotValid bool
	Snapshot      []byte
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

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
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
