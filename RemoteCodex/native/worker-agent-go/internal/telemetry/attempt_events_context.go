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

type milestoneRecorderContextKey struct{}

func WithMilestoneRecorder(ctx context.Context, recorder *AttemptMilestoneRecorder) context.Context {
	if ctx == nil || recorder == nil {
		return ctx
	}
	return context.WithValue(ctx, milestoneRecorderContextKey{}, recorder)
}

func MilestoneRecorderFromContext(ctx context.Context) *AttemptMilestoneRecorder {
	if ctx == nil {
		return nil
	}
	recorder, _ := ctx.Value(milestoneRecorderContextKey{}).(*AttemptMilestoneRecorder)
	return recorder
}
