// HYDRA-UMC-TELEMETRY-COLLECTOR - dedup/dedup_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package dedup

import "testing"

func TestTracker_FirstSequenceIsAlwaysAllowed(t *testing.T) {
	tr := New(4)
	if !tr.Allow("robot-1", 1) {
		t.Fatal("first sequence for a new source must be allowed")
	}
}

func TestTracker_ExactDuplicateIsRejected(t *testing.T) {
	tr := New(4)
	tr.Allow("robot-1", 1)
	if tr.Allow("robot-1", 1) {
		t.Fatal("resending the exact same sequence must be rejected")
	}
}

func TestTracker_ReconnectResendOfAlreadyAckedMessagesIsRejected(t *testing.T) {
	// Simulates a real disconnect/reconnect: a device sends 1,2,3, the
	// connection drops before it gets an ack, it reconnects and resends
	// 2,3 (it wasn't sure they arrived) followed by the genuinely new 4.
	tr := New(8)
	for _, seq := range []uint64{1, 2, 3} {
		if !tr.Allow("robot-1", seq) {
			t.Fatalf("initial sequence %d should have been allowed", seq)
		}
	}
	if tr.Allow("robot-1", 2) {
		t.Fatal("resent sequence 2 after reconnect must be rejected as a duplicate")
	}
	if tr.Allow("robot-1", 3) {
		t.Fatal("resent sequence 3 after reconnect must be rejected as a duplicate")
	}
	if !tr.Allow("robot-1", 4) {
		t.Fatal("genuinely new sequence 4 after the resend must still be allowed")
	}
}

func TestTracker_OutOfOrderArrivalWithinWindowIsAllowed(t *testing.T) {
	tr := New(8)
	if !tr.Allow("robot-1", 5) {
		t.Fatal("sequence 5 should be allowed")
	}
	if !tr.Allow("robot-1", 3) {
		t.Fatal("a real reordered, earlier sequence within the window should still be allowed once")
	}
	if tr.Allow("robot-1", 3) {
		t.Fatal("sequence 3 arriving a second time must be rejected")
	}
}

func TestTracker_StaleSequenceBeyondWindowIsRejected(t *testing.T) {
	tr := New(4)
	tr.Allow("robot-1", 100)
	// 95 is 5 behind 100, outside a window of 4 - a real stale replay,
	// not a legitimately reordered late arrival.
	if tr.Allow("robot-1", 95) {
		t.Fatal("a sequence far behind the high-water mark must be rejected as stale")
	}
}

func TestTracker_DifferentSourcesAreTrackedIndependently(t *testing.T) {
	tr := New(4)
	tr.Allow("robot-1", 1)
	if !tr.Allow("robot-2", 1) {
		t.Fatal("the same sequence number from a DIFFERENT source must be allowed - independent producers")
	}
}

func TestTracker_MemoryPerSourceStaysBounded(t *testing.T) {
	tr := New(4)
	for seq := uint64(1); seq <= 1000; seq++ {
		tr.Allow("robot-1", seq)
	}
	st := tr.sources["robot-1"]
	if len(st.seen) > 4 {
		t.Fatalf("tracked %d sequences for one source with window=4, want <= 4 (bounded memory)", len(st.seen))
	}
}
