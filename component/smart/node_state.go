package smart

import (
	"encoding/json"
	"hash/crc32"
	"strings"
	"sync"
)

var nodeStateLocks [256]sync.Mutex

func nodeStateLock(group, config, node string) *sync.Mutex {
	index := crc32.ChecksumIEEE([]byte(group+"\x00"+config+"\x00"+node)) % uint32(len(nodeStateLocks))
	return &nodeStateLocks[index]
}

// NodeStateBytes reads the latest node update, including queued or in-flight writes;
// the key is read exactly: a group scan samples and may miss the node in a big group.
func (s *Store) NodeStateBytes(group, config, node string) ([]byte, bool) {
	if node == "" {
		return nil, false
	}
	key := FormatDBKey(KeyTypeNode, config, group, node)
	if raw, ok := s.nodeStates.Load(key); ok {
		return raw.([]byte), true
	}
	queue := globalOperationQueue.Load()
	for i := len(queue) - 1; i >= 0; i-- {
		if formatOperationKey(&queue[i]) == key {
			raw, _ := s.nodeStates.LoadOrStore(key, queue[i].Data)
			return raw.([]byte), true
		}
	}
	data, err := s.DBViewGetItem(key)
	if err != nil {
		return nil, false
	}
	raw, _ := s.nodeStates.LoadOrStore(key, data)
	return raw.([]byte), true
}

// UpdateNodeState merges one node's state under its per-node lock, reading queued writes.
func (s *Store) UpdateNodeState(group, config, node string, update func(*NodeState)) {
	if node == "" {
		return
	}

	mu := nodeStateLock(group, config, node)
	mu.Lock()
	defer mu.Unlock()

	var state NodeState
	if raw, ok := s.NodeStateBytes(group, config, node); ok {
		_ = json.Unmarshal(raw, &state)
	}
	state.Name = node
	update(&state)
	data, err := json.Marshal(&state)
	if err != nil {
		return
	}
	s.nodeStates.Store(FormatDBKey(KeyTypeNode, config, group, node), data)
	s.AppendToGlobalQueue(StoreOperation{
		Type:   OpSaveNodeState,
		Group:  group,
		Config: config,
		Node:   node,
		Data:   data,
	})
}

func (s *Store) clearNodeStates(level, config, group string) {
	prefix := FormatDBKey(KeyTypeNode, config)
	if level == "group" {
		prefix = FormatDBKey(KeyTypeNode, config, group)
	}
	s.nodeStates.Range(func(key, _ any) bool {
		if level == "all" || ((level == "config" || level == "group") && strings.HasPrefix(key.(string), prefix+"/")) {
			s.nodeStates.Delete(key)
		}
		return true
	})
}
