package main

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

// Mesmas structs do servidor (tipos “compatíveis” pro net/rpc).
type ArgsAppend struct {
	ListID int
	Value  int
}

type ArgsListID struct {
	ListID int
}

type ArgsGet struct {
	ListID int
	Index  int
}

type BoolReply struct {
	OK bool
}

// ==== ESTATÍSTICAS GLOBAIS ====

// mutex para proteger os contadores
var statsMu sync.Mutex

var (
	totalOps    int
	countAppend int
	countRemove int
	countSize   int
	countGet    int
)

// helper para incrementar contadores
func incOp(op string) {
	statsMu.Lock()
	defer statsMu.Unlock()

	totalOps++
	switch op {
	case "append":
		countAppend++
	case "remove":
		countRemove++
	case "size":
		countSize++
	case "get":
		countGet++
	}
}

// Cada worker é uma goroutine que faz várias operações na MESMA lista.
func worker(id int, client *rpc.Client, listID int, ops int, wg *sync.WaitGroup) {
	defer wg.Done()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	fmt.Printf("[worker %d] começou usando listID=%d\n", id, listID)

	for i := 0; i < ops; i++ {
		op := rnd.Intn(4) // 0 = Append, 1 = Remove, 2 = Size, 3 = Get

		switch op {

		case 0: // Append
			val := rnd.Intn(10)
			args := ArgsAppend{ListID: listID, Value: val}
			var rep BoolReply

			if err := client.Call("RemoteList.Append", args, &rep); err != nil {
				fmt.Printf("[worker %d] Append error: %v\n", id, err)
			} else {
				fmt.Printf("[worker %d] Append(list=%d, value=%d)\n", id, listID, val)
			}
			incOp("append")

		case 1: // Remove
			var removed int
			if err := client.Call("RemoteList.Remove", ArgsListID{ListID: listID}, &removed); err != nil {
				fmt.Printf("[worker %d] Remove(list=%d) error: %v\n", id, listID, err)
			} else {
				fmt.Printf("[worker %d] Remove(list=%d) -> %d\n", id, listID, removed)
			}
			incOp("remove")

		case 2: // Size
			var size int
			if err := client.Call("RemoteList.Size", ArgsListID{ListID: listID}, &size); err != nil {
				fmt.Printf("[worker %d] Size(list=%d) error: %v\n", id, listID, err)
			} else {
				fmt.Printf("[worker %d] Size(list=%d) -> %d\n", id, listID, size)
			}
			incOp("size")

		case 3: // Get
			idx := rnd.Intn(5)
			var value int
			if err := client.Call("RemoteList.Get", ArgsGet{ListID: listID, Index: idx}, &value); err != nil {
				fmt.Printf("[worker %d] Get(list=%d, idx=%d) error: %v\n", id, listID, idx, err)
			} else {
				fmt.Printf("[worker %d] Get(list=%d, idx=%d) -> %d\n", id, listID, idx, value)
			}
			incOp("get")
		}

		// Pausa pequena pra embaralhar a concorrência
		time.Sleep(time.Duration(rnd.Intn(100)+20) * time.Millisecond)
	}

	fmt.Printf("[worker %d] terminou\n", id)
}

func main() {
	// CONFIGURAÇÃO
	numWorkers := 15000   // quantos “clientes lógicos”
	opsPerWorker := 10   // quantas operações cada worker faz
	listID := 1          // TODO MUNDO na mesma lista

	// Conecta UMA vez ao servidor
	client, err := rpc.Dial("tcp", ":5000") // ou "localhost:5000"
	if err != nil {
		fmt.Println("dial error:", err)
		return
	}
	defer client.Close()

	fmt.Printf("Iniciando %d workers concorrentes na listID=%d...\n", numWorkers, listID)

	// >>> INÍCIO DA MEDIÇÃO DE TEMPO <<<
	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 1; i <= numWorkers; i++ {
		go worker(i, client, listID, opsPerWorker, &wg)
	}

	wg.Wait()

	// >>> FIM DA MEDIÇÃO DE TEMPO <<<
	elapsed := time.Since(start)

	// No final, consulta o tamanho da lista
	var size int
	if err := client.Call("RemoteList.Size", ArgsListID{ListID: listID}, &size); err != nil {
		fmt.Println("erro ao chamar Size:", err)
		return
	}

	// ==== RESUMO DAS ESTATÍSTICAS ====
	fmt.Println("========================================")
	fmt.Printf("Workers usados:          %d\n", numWorkers)
	fmt.Printf("Ops por worker:          %d\n", opsPerWorker)
	fmt.Printf("Ops totais esperadas:    %d\n", numWorkers*opsPerWorker)
	fmt.Printf("Ops totais registradas:  %d\n", totalOps)
	fmt.Printf("  - Append: %d\n", countAppend)
	fmt.Printf("  - Remove: %d\n", countRemove)
	fmt.Printf("  - Size:   %d\n", countSize)
	fmt.Printf("  - Get:    %d\n", countGet)
	fmt.Printf("Tempo total de execução: %s\n", elapsed)
	fmt.Printf("Tamanho final da lista %d = %d\n", listID, size)
	fmt.Println("========================================")
}