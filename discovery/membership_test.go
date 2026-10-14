package discovery_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/dynamic-calm/mokv/discovery"
)

type testHandler struct {
	joins  chan map[string]string
	leaves chan string
}

func (h *testHandler) Join(name, id string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"name": name,
			"id":   id,
		}
	}
	return nil
}

func (h *testHandler) Leave(id string) error {
	if h.leaves != nil {
		h.leaves <- id
	}
	return nil
}

func setupMember(t *testing.T, members []*discovery.Membership, port int) ([]*discovery.Membership, *testHandler) {
	id := len(members)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)
	tags := map[string]string{
		"rpc_addr": addr,
	}
	c := discovery.MembershipConfig{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}

	h := &testHandler{}
	if len(members) == 0 {
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		// For new members: connect to 127.0.0.1:8000 to join the cluster
		c.StartJoinAddrs = []string{members[0].BindAddr}
	}

	m, err := discovery.NewMembership(h, c)
	if err != nil {
		t.Fatalf("error creating discovery: %v", err)
	}
	t.Cleanup(func() { m.Leave() })
	members = append(members, m)
	return members, h
}

func TestMembership(t *testing.T) {
	members, handler := setupMember(t, nil, 8000)
	members, _ = setupMember(t, members, 8001)
	members, _ = setupMember(t, members, 8002)

	for range 2 {
		select {
		case <-handler.joins:
		case <-time.After(3 * time.Second):
			t.Fatal("timeout waiting for joins")
		}
	}

	if got := len(members[0].Members()); got != 3 {
		t.Fatalf("len(Members()) = %d, want 3", got)
	}

	if err := members[2].Leave(); err != nil {
		t.Fatal(err)
	}

	select {
	case leave := <-handler.leaves:
		if leave != "2" {
			t.Errorf("Leave() handled %q, want %q", leave, "2")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for leave")
	}
}
