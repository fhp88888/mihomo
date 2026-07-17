package smart

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

func TestUpdateHostStatus_Code3FailCountsArePerNode(t *testing.T) {
	InitCache()
	hostStatusCache.Clear()

	s := &Store{}
	metadata := &C.Metadata{Host: "example.com"}

	if blocked := s.UpdateHostStatus("g", "c", "example.com", metadata, "node-a", 2, false, true, 3); blocked {
		t.Fatal("first node-a failure should not block")
	}
	if blocked := s.UpdateHostStatus("g", "c", "example.com", metadata, "node-b", 2, false, true, 3); blocked {
		t.Fatal("first node-b failure should not inherit node-a streak")
	}

	failNodes, _, _, hostBlocked := s.GetHostStatus("g", "c", "example.com")
	if hostBlocked {
		t.Fatal("host should not be globally blocked")
	}
	if failNodes["node-b"] {
		t.Fatal("node-b should not be blocked after one local code-3 failure")
	}

	if blocked := s.UpdateHostStatus("g", "c", "example.com", metadata, "node-b", 2, false, true, 3); !blocked {
		t.Fatal("second local node-b failure should block")
	}
	failNodes, _, _, _ = s.GetHostStatus("g", "c", "example.com")
	if !failNodes["node-b"] {
		t.Fatal("node-b should be blocked after its own second failure")
	}
	if failNodes["node-a"] {
		t.Fatal("node-a should not be blocked after one local failure")
	}
}
