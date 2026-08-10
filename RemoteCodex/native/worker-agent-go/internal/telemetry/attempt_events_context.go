package telemetry

import "context"

type attemptEventMachineContextKey struct{}

func WithAttemptEventMachine(ctx context.Context, machine *AttemptEventMachine) context.Context {
	if ctx == nil || machine == nil {
		return ctx
	}
	return context.WithValue(ctx, attemptEventMachineContextKey{}, machine)
}

func AttemptEventMachineFromContext(ctx context.Context) *AttemptEventMachine {
	if ctx == nil {
		return nil
	}
	machine, _ := ctx.Value(attemptEventMachineContextKey{}).(*AttemptEventMachine)
	return machine
}
