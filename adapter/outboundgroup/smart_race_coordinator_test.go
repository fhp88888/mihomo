package outboundgroup

import (
	"context"
	"testing"
	"time"
)

func TestTrackRaceBlocksCloseAndReceivesCancellation(t *testing.T) {
	pc := newSmartRaceCoordinator()
	trackedCtx, finish, err := pc.TrackRace(context.Background())
	if err != nil {
		t.Fatalf("TrackRace error: %v", err)
	}

	closed := make(chan struct{})
	go func() {
		_ = pc.Close()
		close(closed)
	}()

	select {
	case <-trackedCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel tracked race")
	}
	select {
	case <-closed:
		t.Fatal("Close returned before tracked race released its registration")
	default:
	}

	finish()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after tracked race finished")
	}
	if _, _, err := pc.TrackRace(context.Background()); err == nil {
		t.Fatal("TrackRace accepted work after Close")
	}
}
