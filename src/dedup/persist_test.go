// HYDRA-UMC-TELEMETRY-COLLECTOR - dedup/persist_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package dedup

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPersist_AResendAfterARestartIsStillADuplicate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dedup.json")
	first := New(64)
	for seq := uint64(1); seq <= 100; seq++ {
		first.Allow("robot-1", seq)
	}
	if err := first.Save(path); err != nil {
		t.Fatal(err)
	}
	second := New(64)
	n, err := second.Load(path)
	if err != nil || n != 1 {
		t.Fatalf("Load = %d, %v; want 1 producer", n, err)
	}
	for seq := uint64(95); seq <= 100; seq++ {
		if second.Allow("robot-1", seq) {
			t.Fatalf("sequence %d was accepted again after the restart", seq)
		}
	}
	if !second.Allow("robot-1", 101) {
		t.Fatal("the next new sequence must be accepted")
	}
}

func TestPersist_WithoutAFileTheTrackerStartsEmpty(t *testing.T) {
	tr := New(64)
	n, err := tr.Load(filepath.Join(t.TempDir(), "missing.json"))
	if n != 0 || err != nil {
		t.Fatalf("Load = %d, %v", n, err)
	}
	if !tr.Allow("a", 1) {
		t.Fatal("a fresh tracker must accept")
	}
}

func TestPersist_AFileThatDoesNotFitIsRefusedAndLeavesTheTrackerEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dedup.json")
	a := New(64)
	a.Allow("x", 5)
	if err := a.Save(path); err != nil {
		t.Fatal(err)
	}
	if _, err := New(128).Load(path); err == nil {
		t.Fatal("another window must be refused")
	}
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	b := New(64)
	if _, err := b.Load(bad); err == nil {
		t.Fatal("a broken file must be refused")
	}
	if b.Sources() != 0 {
		t.Fatalf("Sources = %d, want 0", b.Sources())
	}
}

func TestPersist_ALoadRespectsTheLimitOfProducers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dedup.json")
	a := New(64)
	for i, id := range []string{"a", "b", "c", "d"} {
		a.Allow(id, uint64(10+i))
	}
	if err := a.Save(path); err != nil {
		t.Fatal(err)
	}
	b := New(64)
	b.SetMaxSources(2)
	n, err := b.Load(path)
	if err != nil || n != 2 || b.Sources() != 2 {
		t.Fatalf("Load = %d, %v, Sources = %d; want 2", n, err, b.Sources())
	}
}

func TestPersist_SavingTwiceKeepsOneWholeFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dedup.json")
	tr := New(64)
	tr.Allow("a", 1)
	for i := 0; i < 2; i++ {
		if err := tr.Save(path); err != nil {
			t.Fatal(err)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("%d files left in the folder, want 1", len(entries))
	}
}
