package DIMEX

import (
	PP2PLink "SD/PP2PLink"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// ------------------------------------------------------------------------------------
// ------- principais tipos
// ------------------------------------------------------------------------------------

type State int // enumeracao dos estados possiveis de um processo
const (
	noMX State = iota
	wantMX
	inMX
)

type dmxReq int // enumeracao dos estados possiveis de um processo
const (
	ENTER dmxReq = iota
	EXIT
	SNAPSHOT
)

type dmxResp struct { // mensagem do módulo DIMEX infrmando que pode acessar - pode ser somente um sinal (vazio)
	// mensagem para aplicacao indicando que pode prosseguir
}

type DIMEX_Module struct {
	Req       chan dmxReq  // canal para receber pedidos da aplicacao (REQ e EXIT)
	Ind       chan dmxResp // canal para informar aplicacao que pode acessar
	addresses []string     // endereco de todos, na mesma ordem
	id        int          // identificador do processo - é o indice no array de enderecos acima
	st        State        // estado deste processo na exclusao mutua distribuida
	waiting   []bool       // processos aguardando tem flag true
	lcl       int          // relogio logico local
	reqTs     int          // timestamp local da ultima requisicao deste processo
	nbrResps  int
	dbg       bool
	Pp2plink  *PP2PLink.PP2PLink // acesso a comunicacao enviar por PP2PLinq.Req  e receber por PP2PLinq.Ind

	mutex           sync.Mutex // sincronia de acesso as váriaveis
	idSnapShot      int64
	snapshots       map[int64]*SnapshotState // Map de snapshots com ID do snapshot como chave
	activeSnapshot  bool                     // Flag indicando se o snapshot está ativo
	markersReceived map[int]bool             // Map para rastrear marcadores recebidos
}

type SnapshotState struct {
	ProcessState State
	Lcl          int
	ReqTs        int
	Waiting      []bool
	NbrResps     int
	Recorded     bool
	ChannelState map[int][]string // Estado dos canais de entrada
	Messages     []string         // Mensagens recebidas durante o snapshot
}

const TAKE_SNAPSHOT = "TAKE_SNAPSHOT"

// ------------------------------------------------------------------------------------
// ------- Inicialização
// ------------------------------------------------------------------------------------

func NewDIMEX(_addresses []string, _id int, _dbg bool) *DIMEX_Module {

	p2p := PP2PLink.NewPP2PLink(_addresses[_id], _dbg)

	dmx := &DIMEX_Module{
		Req: make(chan dmxReq, 1),
		Ind: make(chan dmxResp, 1),

		addresses: _addresses,
		id:        _id,
		st:        noMX,
		waiting:   make([]bool, len(_addresses)),
		lcl:       0,
		reqTs:     0,
		dbg:       _dbg,

		Pp2plink: p2p,

		snapshots:       make(map[int64]*SnapshotState),
		activeSnapshot:  false,
		markersReceived: make(map[int]bool),
		idSnapShot:      0}

	for i := 0; i < len(dmx.waiting); i++ {
		dmx.waiting[i] = false
	}
	dmx.Start()
	dmx.outDbg("Init DIMEX!")
	return dmx
}

// ------------------------------------------------------------------------------------
// ------- Núcleo do funcionamento
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) Start() {

	go func() {
		for {
			select {
			case dmxR := <-module.Req: // vindo da  aplicação
				switch dmxR {
				case ENTER:
					module.outDbg("app pede mx")
					module.handleUponReqEntry() // ENTRADA DO ALGORITMO
				case EXIT:
					module.outDbg("app libera mx")
					module.handleUponReqExit() // ENTRADA DO ALGORITMO
				case SNAPSHOT:
					module.outDbg("app pede snapshot")
					module.startSnapshot() // ENTRADA DO ALGORITMO
				}

			case msgOutro := <-module.Pp2plink.Ind: // Vindo de outro processo
				if strings.Contains(msgOutro.Message, "respOK") {
					module.outDbg("         <<<---- responde! " + msgOutro.Message)
					module.handleUponDeliverRespOk(msgOutro) // ENTRADA DO ALGORITMO
				} else if strings.Contains(msgOutro.Message, "reqEntry") {
					module.outDbg("          <<<---- pede??  " + msgOutro.Message)
					module.handleUponDeliverReqEntry(msgOutro) // ENTRADA DO ALGORITMO
				} else if strings.Contains(msgOutro.Message, TAKE_SNAPSHOT) {
					module.handleSnapshot(msgOutro) // ENTRADA DO ALGORITMO
				}
			}
		}
	}()
}

// ------------------------------------------------------------------------------------
// ------- Tratamento de solicitações vindas da aplicação
// ------- UPON ENTRY
// ------- UPON EXIT
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponReqEntry() {
	/*
		upon event [ dmx, Entry  |  r ]  do
		    lts.ts++
		    myTs := lts
		    resps := 0
		    para todo processo p
				trigger [ pl , Send | [ reqEntry, r, myTs ]
		    estado := queroSC
	*/
	module.mutex.Lock()
	defer module.mutex.Unlock()
	module.lcl++
	module.reqTs = module.lcl
	module.nbrResps = 0

	for i := 0; i < len(module.addresses); i++ {
		if i != module.id {
			module.sendToLink(module.addresses[i], fmt.Sprintf("reqEntry:%d:%d", module.id, module.reqTs))
		}
	}
	module.st = wantMX
}

func (module *DIMEX_Module) handleUponReqExit() {
	/*
		upon event [ dmx, Exit  |  r  ]  do
			para todo [p, r, ts ] em waiting
		    trigger [ pl, Send | p , [ respOk, r ]  ]
		    estado := naoQueroSC
			waiting := {}
	*/
	module.mutex.Lock()
	defer module.mutex.Unlock()

	module.st = noMX
	module.nbrResps = 0

	for i := 0; i < len(module.waiting); i++ {
		if module.waiting[i] {
			module.sendToLink(module.addresses[i], fmt.Sprintf("respOK:%d:%d", module.id, module.lcl))
			module.waiting[i] = false
		}
	}
}

// ------------------------------------------------------------------------------------
// ------- Tratamento de mensagens de outros processos
// ------- UPON respOK
// ------- UPON reqEntry
// ------------------------------------------------------------------------------------

func (module *DIMEX_Module) handleUponDeliverRespOk(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	/*
		upon event [ pl, Deliver | p, [ respOk, r ] ]
		    resps++
		    se resps = N
		    então trigger [ dmx, Deliver | free2Access ]
		  		estado := estouNaSC

	*/
	module.mutex.Lock()
	defer module.mutex.Unlock()
	var senderId int
	var senderTs int
	fmt.Sscanf(msgOutro.Message, "respOK:%d:%d", &senderId, &senderTs)
	module.messageInterceptor(senderId, msgOutro.Message)
	module.lcl = max(module.lcl, getTimestamp(msgOutro.Message)) + 1
	module.nbrResps++
	if module.nbrResps == len(module.addresses)-1 {
		module.Ind <- dmxResp{}
		module.st = inMX
	}
}

func (module *DIMEX_Module) handleUponDeliverReqEntry(msgOutro PP2PLink.PP2PLink_Ind_Message) {
	// outro processo quer entrar na SC
	/*
		upon event [ pl, Deliver | p, [ reqEntry, r, rts ]  do
		    se (estado == naoQueroSC)   OR
		        (estado == QueroSC AND  myTs >  ts)
			então  trigger [ pl, Send | p , [ respOk, r ]  ]
		 	senão
		        se (estado == estouNaSC) OR
		           	(estado == QueroSC AND  myTs < ts)
		        então  postergados := postergados + [p, r ]
		     	lts.ts := max(lts.ts, rts.ts)
	*/
	var senderId int
	var senderTs int
	fmt.Sscanf(msgOutro.Message, "reqEntry:%d:%d", &senderId, &senderTs)
	module.messageInterceptor(senderId, msgOutro.Message)
	module.lcl = max(module.lcl, senderTs) + 1

	if module.st == inMX {
		module.sendToLink(module.addresses[senderId], fmt.Sprintf("respOK:%d:%d", module.id, module.lcl))
	} else {
		if module.st == noMX || (module.st == wantMX && before(senderId, senderTs, module.id, module.reqTs)) {
			module.sendToLink(module.addresses[senderId], fmt.Sprintf("respOK:%d:%d", module.id, module.lcl))
		} else {
			module.waiting[senderId] = true
		}
	}
}

// ------------------------------------------------------------------------------------
// ------- funcoes de ajuda
// ------------------------------------------------------------------------------------

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func before(oneId, oneTs, othId, othTs int) bool {
	if oneTs < othTs {
		return true
	} else if oneTs > othTs {
		return false
	} else {
		return oneId < othId
	}
}

func parseMessage(message string) (int, int) {
	var msgType string
	var senderId, timestamp int
	fmt.Sscanf(message, "%[^:]:%d:%d", &msgType, &senderId, &timestamp)
	return senderId, timestamp
}

func getTimestamp(message string) int {
	_, timestamp := parseMessage(message)
	return timestamp
}

func (module *DIMEX_Module) sendToLink(address string, content string) {
	module.Pp2plink.Req <- PP2PLink.PP2PLink_Req_Message{
		To:      address,
		Message: content,
	}
}

func (module *DIMEX_Module) outDbg(s string) {
	if module.dbg {
		// fmt.Println(". . . . . . . . . . . . [ DIMEX : " + s + " ]")
	}
}

// ------------------------------------------------------------------------------------
// ------- Implementação do Snapshot (Chandy-Lamport)
// ------------------------------------------------------------------------------------

// middleware durante o snapshot
func (module *DIMEX_Module) messageInterceptor(senderId int, stringMsg string) {
	if module.activeSnapshot {
		for _, snapshot := range module.snapshots {
			if !snapshot.Waiting[senderId] {
				snapshot.Messages = append(snapshot.Messages, stringMsg)
			}
		}
	}
}

func (module *DIMEX_Module) startSnapshot() {
	// id do snapshot sequencial e único
	if module.activeSnapshot {
		fmt.Println("Snapshot já está em andamento.")
		return
	}
	snapshotId := module.idSnapShot + 1
	fmt.Println("Iniciando snapshot com o id: ", snapshotId)

	// envia mensagm para todos os processos
	module.sendToLink(module.addresses[module.id], fmt.Sprintf("%s:%d:%d", TAKE_SNAPSHOT, module.id, snapshotId))
}

func (module *DIMEX_Module) handleSnapshot(msg PP2PLink.PP2PLink_Ind_Message) {
	senderId, snapshotId := parseMarkerMessage(msg.Message)
	module.idSnapShot = snapshotId
	var repeated bool
	//testa se repetido
	if module.snapshots != nil {
		if _, exists := module.snapshots[snapshotId]; exists {
			repeated = true
		}
	}
	repeated = false

	// testa se o snapshot já foi gravado
	if !repeated {
		module.activeSnapshot = true
		module.recordState(snapshotId)
		fmt.Println("-----  ", module.snapshots[snapshotId].Waiting[senderId])
		fmt.Println("-----  ", senderId)
		module.snapshots[snapshotId].Waiting[senderId] = true
		module.outDbg(fmt.Sprintf("Gravando estado de canal de %d.", senderId))
		// envia a mensagem para todos os processos
		for i := 0; i < len(module.addresses); i++ {
			if i != module.id {
				module.sendToLink(module.addresses[i], fmt.Sprintf("%s:%d:%d", TAKE_SNAPSHOT, module.id, snapshotId))
			}
		}
	} else {
		// Apenas monitora e para de gravar mensagens do canal quando receber o marcador
		module.snapshots[snapshotId].Waiting[senderId] = true
		module.outDbg(fmt.Sprintf("Recebido marcador de volta de %d. Parando gravação para esse canal.", senderId))
	}

	// verifica se todos receberam
	var allReceived = true

	snapshot := module.snapshots[snapshotId]
	fmt.Println("Snapshot: ", snapshot)
	fmt.Println("snapshot.Waiting ", snapshot.Waiting)
	for _, waiting := range snapshot.Waiting {
		fmt.Println("Waiting ", waiting)
		if !waiting {
			allReceived = false
		}
	}
	allReceived = true

	// se todos receberam a mensagem de snapshot termina
	if allReceived {
		module.outDbg(fmt.Sprintf("Snapshot %d completo.", snapshotId))
		module.activeSnapshot = false
		module.markersReceived = make(map[int]bool) // Limpa para futuros snapshots
		module.writeSnapshotToFile(snapshotId)
	}
}

func (module *DIMEX_Module) recordState(snapshotId int64) {
	snapshot := &SnapshotState{
		ProcessState: module.st,
		Lcl:          module.lcl,
		ReqTs:        module.reqTs,
		Waiting:      []bool{false, false, false},
		NbrResps:     module.nbrResps,
		Recorded:     true,
		ChannelState: make(map[int][]string),
	}
	snapshot.Waiting[module.id] = true
	module.snapshots[snapshotId] = snapshot
}

func (module *DIMEX_Module) writeSnapshotToFile(snapshotId int64) {
	filename := fmt.Sprintf("./snapshots/process_%d.txt", module.id)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer file.Close()
	fmt.Println("snapshot id ", snapshotId)
	snapshot := module.snapshots[snapshotId]
	file.WriteString(fmt.Sprintf("Snapshot %d\n", snapshotId))
	file.WriteString(fmt.Sprintf("Estado: %d\n", snapshot.ProcessState))
	file.WriteString(fmt.Sprintf("Relógio Lógico: %d\n", snapshot.Lcl))
	file.WriteString(fmt.Sprintf("Timestamp de Requisição: %d\n", snapshot.ReqTs))
	file.WriteString(fmt.Sprintf("Receive resps: %v\n", snapshot.Waiting))
	file.WriteString(fmt.Sprintf("Waiting: %v\n", module.waiting))
	file.WriteString(fmt.Sprintf("NbrResps: %d\n", snapshot.NbrResps))
	file.WriteString(fmt.Sprintf("Mensagens:\n"))
	for _, msg := range snapshot.Messages {
		file.WriteString(fmt.Sprintf("%s\n", msg))
	}

	file.WriteString("\n")
}

func parseMarkerMessage(message string) (int, int64) {
	fmt.Println("Mensagem recebida:", message)
	parts := strings.Split(message, ":")

	senderId, err1 := strconv.Atoi(parts[1])
	snapshotId, err2 := strconv.ParseInt(parts[2], 10, 64)

	if err1 != nil || err2 != nil {
		fmt.Println("Erro ao analisar a mensagem")
		return 0, 0
	}
	fmt.Println("ID do snapshot:", snapshotId)
	fmt.Println("ID do sender:", senderId)
	return senderId, snapshotId
}
