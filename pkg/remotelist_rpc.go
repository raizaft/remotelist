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
	Op    string `json:"op"`
	ListID int    `json:"list_id"`
	Value int    `json:"value,omitempty"`
	Time int64  `json:"time"`
}

type RemoteList struct {
	mu   sync.Mutex
	lists map[int][]int
	logFile string
	snapFile string
	snapshotInterval time.Duration
	stopSnapshot chan struct{}
}

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

func NewRemoteList(logFile, snapFile string, snapshotInterval time.Duration) (*RemoteList, error) {
	rl := &RemoteList{
		lists:            make(map[int][]int),
		logFile:          logFile,
		snapFile:         snapFile,
		snapshotInterval: snapshotInterval,
		stopSnapshot:     make(chan struct{}),
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
	if err := os.Truncate(l.logFile, 0); err != nil && !os.IsNotExist(err) {
		fmt.Println("snapshot: truncate log error:", err)
	}
	fmt.Println("Snapshot salvo.")
}

func (l *RemoteList) Stop() {
	close(l.stopSnapshot)
}