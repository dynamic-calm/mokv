package mokv

import (
	"fmt"

	"github.com/casbin/casbin/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Enforcer struct {
	enforcer *casbin.Enforcer
}

func NewAuthorizer(model, policy string) *Enforcer {
	enforcer, err := casbin.NewEnforcer(model, policy)
	if err != nil {
		panic(err)
	}

	return &Enforcer{enforcer: enforcer}
}

func (a *Enforcer) Authorize(subject, object, action string) error {
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
