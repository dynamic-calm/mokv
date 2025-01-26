package auth

import (
	"fmt"

	"github.com/casbin/casbin/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func New(model, policy string) *Authorizer {
	enforcer, err := casbin.NewEnforcer(model, policy)
	if err != nil {
		fmt.Printf("Debug: Failed to create enforcer: %v\n", err)
		panic(err)
	}

	return &Authorizer{enforcer: enforcer}
}

func (a *Authorizer) Authorize(subject, object, action string) error {
	ok, err := a.enforcer.Enforce(subject, object, action)
	if err != nil {
		return status.Errorf(codes.Internal, "authorization error: %v", err)
	}

	if !ok {
		msg := fmt.Sprintf(
			"%s not permitted to %s to %s",
			subject,
			action,
			object,
		)
		return status.New(codes.PermissionDenied, msg).Err()
	}
	return nil
}
