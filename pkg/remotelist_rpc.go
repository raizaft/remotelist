package remotelist

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

type ArgsAppend struct {
	ListID int
	Value  int
}

type ArgsGet struct {
	ListID int
	Index  int
}

type ArgsListID struct {
	ListID int
}

type BoolReply struct {
	OK bool
}

type logEntry struct {
	Op     string `json:"op"`
	ListID int    `json:"list_id"`
	Value  int    `json:"value,omitempty"`
	Time   int64  `json:"time"`
}

type RemoteList struct {
	mu               sync.Mutex
	lists            map[int][]int
	logFile          string
	snapFile         string
	historyFile      string
	snapshotInterval time.Duration
	stopSnapshot     chan struct{}
	historyMaxLines  int
}

const defaultHistoryMaxLines = 20

func (l *RemoteList) Append(args ArgsAppend, reply *BoolReply) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lists[args.ListID] = append(l.lists[args.ListID], args.Value)
	if err := l.appendLog("append", args.ListID, args.Value); err != nil {
		return err
	}

	*reply = BoolReply{OK: true}
	fmt.Println("Depois do Append:", l.lists[args.ListID])
	return nil
}

func (l *RemoteList) Remove(args ArgsListID, reply *int) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	list := l.lists[args.ListID]
	if len(list) == 0 {
		return errors.New("empty list")
	}
	val := list[len(list)-1]
	l.lists[args.ListID] = list[:len(list)-1]
	if err := l.appendLog("remove", args.ListID, val); err != nil {
		return err
	}
	*reply = val
	fmt.Println("After Remove:", l.lists[args.ListID])
	return nil
}

func NewRemoteList(logFile, snapFile string, snapshotInterval time.Duration, historyMaxLines int) (*RemoteList, error) {
	if historyMaxLines <= 0 {
		historyMaxLines = defaultHistoryMaxLines
	}
	rl := &RemoteList{
		lists:            make(map[int][]int),
		logFile:          logFile,
		snapFile:         snapFile,
		historyFile:      logFile + ".history",
		snapshotInterval: snapshotInterval,
		stopSnapshot:     make(chan struct{}),
		historyMaxLines:  historyMaxLines,
	}
	if err := rl.loadSnapshot(); err != nil {
		return nil, err
	}
	if err := rl.replayLog(); err != nil {
		return nil, err
	}
	go rl.snapshotRoutine()
	return rl, nil
}

func (l *RemoteList) Size(args ArgsListID, reply *int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	*reply = len(l.lists[args.ListID])
	return nil
}

func (l *RemoteList) Get(args ArgsGet, reply *int) error {
	l.mu.Lock()
	defer l.mu.Unlock()


	list := l.lists[args.ListID]
	if args.Index < 0 || args.Index >= len(list){
		return errors.New("index out of bounds")
	}
	*reply = list[args.Index]
	return nil

}

func (l *RemoteList) appendLog(op string, listID, value int) error {
	f, err := os.OpenFile(l.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	entry := logEntry{
		Op:     op,
		ListID: listID,
		Value:  value,
		Time:   time.Now().UnixNano(),
	}
	b, _ := json.Marshal(entry)
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func (l *RemoteList) loadSnapshot() error {
	f, err := os.Open(l.snapFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var data map[string][]int
	if err := dec.Decode(&data); err != nil {
		return err
	}
	for k, v := range data {
		var id int
		fmt.Sscanf(k, "%d", &id)
		l.lists[id] = v
	}
	return nil
}

func (l *RemoteList) replayLog() error {
	f, err := os.Open(l.logFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var e logEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue
		}
		switch e.Op {
		case "append":
			l.lists[e.ListID] = append(l.lists[e.ListID], e.Value)
		case "remove":
			list := l.lists[e.ListID]
			if len(list) > 0 {
				l.lists[e.ListID] = list[:len(list)-1]
			}
		}
	}
	return scanner.Err()
}

func (l *RemoteList) snapshotRoutine() {
	ticker := time.NewTicker(l.snapshotInterval)
	for {
		select {
		case <-ticker.C:
			l.doSnapshot()
		case <-l.stopSnapshot:
			ticker.Stop()
			return
		}
	}
}

func (l *RemoteList) doSnapshot() {
	l.mu.Lock()
	copyState := make(map[int][]int, len(l.lists))
	for id, lst := range l.lists {
		newSlice := make([]int, len(lst))
		copy(newSlice, lst)
		copyState[id] = newSlice
	}
	l.mu.Unlock()

	out := make(map[string][]int, len(copyState))
	for id, lst := range copyState {
		out[fmt.Sprintf("%d", id)] = lst
	}
	tmp := l.snapFile + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("snapshot: open tmp error:", err)
		return
	}
	enc := json.NewEncoder(f)
	if err := enc.Encode(out); err != nil {
		_ = f.Close()
		fmt.Println("snapshot: encode error:", err)
		return
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		fmt.Println("snapshot: sync error:", err)
		return
	}
	if err := f.Close(); err != nil {
		fmt.Println("snapshot: close error:", err)
		return
	}
	if err := os.Rename(tmp, l.snapFile); err != nil {
		fmt.Println("snapshot: rename error:", err)
		return
	}

	if err := l.appendLogToHistoryAndTrim(); err != nil {
		fmt.Println("snapshot: append history error:", err)
		return
	}

	if err := os.Truncate(l.logFile, 0); err != nil && !os.IsNotExist(err) {
		fmt.Println("snapshot: truncate log error:", err)
	}
	fmt.Println("Snapshot salvo.")
}

func (l *RemoteList) appendLogToHistoryAndTrim() error {
	// se não existir log, nada a fazer
	info, err := os.Stat(l.logFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Size() == 0 {
		// nada novo no log
		return nil
	}

	// 1) Ler linhas do history (se existir)
	historyLines := []string{}
	if hf, err := os.Open(l.historyFile); err == nil {
		scanner := bufio.NewScanner(hf)
		for scanner.Scan() {
			historyLines = append(historyLines, scanner.Text())
		}
		_ = hf.Close()
		// ignoramos scanner.Err() — se houver problema, continuamos e recriamos o history inteiro
	}

	// 2) Ler linhas do log atual
	logLines := []string{}
	lf, err := os.Open(l.logFile)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(lf)
	for scanner.Scan() {
		logLines = append(logLines, scanner.Text())
	}
	lf.Close()
	// se scanner tiver erro aqui, ainda tentaremos usar as linhas lidas

	// 3) combinar history + log
	combined := make([]string, 0, len(historyLines)+len(logLines))
	combined = append(combined, historyLines...)
	combined = append(combined, logLines...)

	// 4) cortar linhas antigas se exceder historyMaxLines (definido no NewRemoteList)
	if l.historyMaxLines > 0 && len(combined) > l.historyMaxLines {
		start := len(combined) - l.historyMaxLines
		combined = combined[start:]
	}

	// 5) escrever combinado em arquivo temporário e renomear (atômico)
	tmp := l.historyFile + ".tmp"
	hf, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(hf)
	for _, line := range combined {
		if _, err := writer.WriteString(line + "\n"); err != nil {
			_ = hf.Close()
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		_ = hf.Close()
		return err
	}
	if err := hf.Sync(); err != nil {
		_ = hf.Close()
		return err
	}
	if err := hf.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, l.historyFile); err != nil {
		return err
	}

	return nil
}

func (l *RemoteList) Stop() {
	close(l.stopSnapshot)
}
