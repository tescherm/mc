package consistenthash

import (
	"crypto/md5"
	"fmt"
	"sort"
)

type keys []string
type ring map[string]string
type nodeIDs []string

const defaultReplicas = 160

type ConsistentHash struct {
	Ring    ring
	Keys    keys
	NodeIDs nodeIDs

	Replicas int
}

func New(nodes []string, replicas int) *ConsistentHash {
	nodeIDs := make(nodeIDs, 0)
	keys := make(keys, 0)
	ring := make(ring, 0)

	if replicas == 0 {
		replicas = defaultReplicas
	}

	s := &ConsistentHash{
		Keys:    keys,
		Ring:    ring,
		NodeIDs: nodeIDs,

		Replicas: replicas,
	}

	for _, node := range nodes {
		s.Add(node)
	}

	return s
}

func (s *ConsistentHash) Add(nodeID string) {
	s.NodeIDs = append(s.NodeIDs, nodeID)

	for i := 0; i < s.Replicas; i++ {
		replicaID := fmt.Sprintf("replica-%d", i)
		keyID := nodeID + ":" + replicaID

		var key = s.hash(keyID)

		s.Keys = append(s.Keys, key)
		s.Ring[key] = nodeID
	}

	sort.Strings(s.Keys)
}

func (s *ConsistentHash) Remove(nodeID string) {
	for i := 0; i < len(s.NodeIDs); i++ {
		if s.NodeIDs[i] == nodeID {
			s.NodeIDs = append(s.NodeIDs[:i], s.NodeIDs[i+1:]...)
			i--
		}
	}

	for i := 0; i < s.Replicas; i++ {
		replicaID := fmt.Sprintf("replica-%d", i)
		keyID := nodeID + ":" + replicaID

		key := s.hash(keyID)
		delete(s.Ring, key)

		for j := 0; j < len(s.Keys); j++ {
			if s.Keys[j] == key {
				s.Keys = append(s.Keys[:j], s.NodeIDs[j+1:]...)
				j--
			}
		}
	}
}

func (s *ConsistentHash) GetNode(key string) string {
	nodeID := s.getNodeID(key)
	return nodeID
}

func (s *ConsistentHash) getNodeID(key string) string {
	if s.ringLength() == 0 {
		return ""
	}

	var h = s.hash(key)
	var pos = s.getNodePosition(h)

	return s.Ring[s.Keys[pos]]
}

func (s *ConsistentHash) getNodePosition(hash string) int {
	upper := s.ringLength() - 1
	lower := 0
	idx := 0
	comp := 0

	if upper == 0 {
		return 0
	}

	for lower <= upper {
		idx = (lower + upper) / 2
		comp = s.compare(s.Keys[idx], hash)

		if comp == 0 {
			return idx
		} else if comp > 0 {
			upper = idx - 1
		} else {
			lower = idx + 1
		}
	}

	if upper < 0 {
		upper = s.ringLength() - 1
	}

	return upper
}

func (s *ConsistentHash) ringLength() int {
	return len(s.Ring)
}

func (s *ConsistentHash) hash(id string) string {
	digest := md5.New()
	digest.Write([]byte(id))
	return fmt.Sprintf("%x", digest.Sum(nil))
}

func (s *ConsistentHash) compare(v1, v2 string) int {
	if v1 > v2 {
		return 1
	} else if v1 < v2 {
		return -1
	} else {
		return 0
	}
}
