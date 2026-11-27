// HYDRA-UMC-TELEMETRY-COLLECTOR - spool/spool_outside_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package spool

import (
	"os"
	"testing"
)

func TestSpool_AFileRemovedOutsideDoesNotBlockTheQueue(t *testing.T) {
	f, path := open(t, 1<<20)
	if err := f.Append(samples(0, 3)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if got, err := f.Next(10); err != nil || len(got) != 0 {
		t.Fatalf("Next after the removal = %v, %v", got, err)
	}
	if f.Pending() != 0 || f.Bytes() != 0 {
		t.Fatalf("Pending=%d Bytes=%d, want 0 and 0", f.Pending(), f.Bytes())
	}
	if err := f.Append(samples(3, 2)); err != nil {
		t.Fatal(err)
	}
	got, err := f.Next(10)
	if err != nil || len(got) != 2 || got[0].Sample.SourceID != "s3" {
		t.Fatalf("Next after a new append = %+v, %v", got, err)
	}
}

func TestSpool_AFileCutOutsideIsReplayedFromItsStart(t *testing.T) {
	f, path := open(t, 1<<20)
	if err := f.Append(samples(0, 4)); err != nil {
		t.Fatal(err)
	}
	first, _ := f.Next(3)
	f.Ack(first[2].End)
	// keep the first line and a piece of the second one
	if err := os.Truncate(path, first[0].End+5); err != nil {
		t.Fatal(err)
	}
	if err := f.Append(samples(9, 1)); err != nil {
		t.Fatal(err)
	}
	got, err := f.Next(10)
	if err != nil || len(got) != 2 || got[0].Sample.SourceID != "s0" || got[1].Sample.SourceID != "s9" {
		t.Fatalf("Next = %+v, %v", got, err)
	}
	f.Ack(got[1].End)
	if f.Pending() != 0 || f.Bytes() != 0 {
		t.Fatalf("Pending=%d Bytes=%d, want 0 and 0", f.Pending(), f.Bytes())
	}
}
