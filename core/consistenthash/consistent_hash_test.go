package consistenthash

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashing(t *testing.T) {
	t.Parallel()

	nodes := []string{
		"node-0",
		"node-1",
		"node-2",
	}

	hash := New(nodes, 1)

	require.Equal(t, 3, hash.ringLength())

	testCases := map[string]string{
		"foo": "node-1",
		"bar": "node-0",
		"baz": "node-1",
	}

	for k, v := range testCases {
		node := hash.GetNode(k)
		require.Equal(t, v, node, "key=%s,val=%s node=%s", k, v, node)
	}

	hash.Add("node-3")

	require.Equal(t, 4, hash.ringLength())

	testCases["another"] = "node-1"

	for k, v := range testCases {
		node := hash.GetNode(k)
		require.Equal(t, v, hash.GetNode(k), "key=%s,val=%s node=%s", k, v, node)
	}

}

func TestAddNode(t *testing.T) {
	t.Parallel()

	nodes := []string{
		"node-0",
		"node-1",
		"node-2",
	}

	hash := New(nodes, 160)
	hash.Add("node-3")
	require.Equal(t, 640, hash.ringLength())
	hash.Add("node-4")
	require.Equal(t, 800, hash.ringLength())
}

func TestConsistency(t *testing.T) {
	t.Parallel()

	nodes := []string{
		"node-0",
	}

	hash1 := New(nodes, 1)
	hash2 := New(nodes, 1)

	require.Equal(t, 1, hash1.ringLength())
	require.Equal(t, 1, hash2.ringLength())

	require.Equal(t, hash1.GetNode("Foo"), hash2.GetNode("Foo"))
}

func BenchmarkGetNode8(b *testing.B)   { benchmarkGetNode(b, 8) }
func BenchmarkGetNode32(b *testing.B)  { benchmarkGetNode(b, 32) }
func BenchmarkGetNode128(b *testing.B) { benchmarkGetNode(b, 128) }
func BenchmarkGetNode512(b *testing.B) { benchmarkGetNode(b, 512) }

func benchmarkGetNode(b *testing.B, nodeCount int) {
	var nodes []string

	hash := New(nodes, 50)

	for i := 0; i < nodeCount; i++ {
		node := fmt.Sprintf("node-%d", i)
		nodes = append(nodes, node)
		hash.Add(node)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hash.GetNode(nodes[i&(nodeCount-1)])
	}
}
