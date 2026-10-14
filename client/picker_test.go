package client

import (
	"testing"

	"github.com/dynamic-calm/mokv/api"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

type subConn struct {
	balancer.SubConn
	name string
}

func TestPickerRoutesWritesToLeader(t *testing.T) {
	t.Parallel()

	leader := &subConn{name: "leader"}
	follower := &subConn{name: "follower"}
	p := (&pickerBuilder{}).Build(base.PickerBuildInfo{
		ReadySCs: map[balancer.SubConn]base.SubConnInfo{
			leader:   {Address: resolver.Address{Attributes: attributes.New(isLeaderAttr, true)}},
			follower: {Address: resolver.Address{Attributes: attributes.New(isLeaderAttr, false)}},
		},
	})

	for method, want := range map[string]*subConn{
		api.KV_Set_FullMethodName:        leader,
		api.KV_Delete_FullMethodName:     leader,
		api.KV_Get_FullMethodName:        follower,
		api.KV_List_FullMethodName:       follower,
		api.KV_GetServers_FullMethodName: follower,
	} {
		res, err := p.Pick(balancer.PickInfo{FullMethodName: method})
		if err != nil {
			t.Fatalf("Pick(%q) error = %v", method, err)
		}
		if got := res.SubConn.(*subConn); got != want {
			t.Errorf("Pick(%q) = %s, want %s", method, got.name, want.name)
		}
	}
}
