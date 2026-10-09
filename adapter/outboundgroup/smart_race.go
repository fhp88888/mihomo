package outboundgroup

import (
	"context"
	"errors"
	"sync"
)

// smartRaceCoordinator owns cancellation and drain lifetimes of normal fallback races.
type smartRaceCoordinator struct {
	mu     sync.Mutex
	closed bool
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newSmartRaceCoordinator() *smartRaceCoordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &smartRaceCoordinator{ctx: ctx, cancel: cancel}
}

// TrackRace registers a fallback race before it can create asynchronous
// loser-drain work. Registration and the closed check share pc.mu with Close,
// so Wait can never overtake a later WaitGroup.Add. The returned context is
// canceled by either the caller or the coordinator.
func (pc *smartRaceCoordinator) TrackRace(ctx context.Context) (context.Context, func(), error) {
	pc.mu.Lock()
	if pc.closed {
		pc.mu.Unlock()
		return nil, nil, errors.New("race coordinator closed")
	}
	pc.wg.Add(1)
	pc.mu.Unlock()

	raceCtx, cancel := context.WithCancel(ctx)
	stopCoordinatorCancel := context.AfterFunc(pc.ctx, cancel)
	done := func() {
		stopCoordinatorCancel()
		cancel()
		pc.wg.Done()
	}
	return raceCtx, done, nil
}

// Close stops new races, cancels active work, and waits for all drains.
func (pc *smartRaceCoordinator) Close() error {
	pc.mu.Lock()
	pc.closed = true
	pc.mu.Unlock()
	pc.cancel()
	pc.wg.Wait()
	return nil
}
