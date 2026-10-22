// HYDRA-UMC-TELEMETRY-COLLECTOR - spool/spool_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package spool

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/JuanenRac/HYDRA-UMC-TELEMETRY-COLLECTOR/telemetry"
)

func samples(from, n int) []telemetry.Sample {
	out := make([]telemetry.Sample, n)
	for i := range out {
		out[i] = telemetry.Sample{SourceID: fmt.Sprintf("s%d", from+i), Kind: "k", Timestamp: 1_700_000_000_000 + int64(from+i), Fields: map[string]float64{"v": 1}}
	}
	return out
}

func open(t *testing.T, max int64) (*File, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sub", "spool.ndjson")
	f, err := Open(path, max)
	if err != nil {
		t.Fatal(err)
	}
	return f, path
}

func TestSpool_DeliversInOrderAndEmptiesTheFile(t *testing.T) {
	f, path := open(t, 1<<20)
	if err := f.Append(samples(0, 5)); err != nil {
		t.Fatal(err)
	}
	if err := f.Append(samples(5, 3)); err != nil {
		t.Fatal(err)
	}
	if f.Pending() != 8 {
		t.Fatalf("Pending() = %d, want 8", f.Pending())
	}
	first, _ := f.Next(5)
	if len(first) != 5 || first[0].Sample.SourceID != "s0" || first[4].Sample.SourceID != "s4" {
		t.Fatalf("first batch = %+v", first)
	}
	f.Ack(first[4].End)
	if f.Pending() != 3 {
		t.Fatalf("Pending() after Ack = %d, want 3", f.Pending())
	}
	rest, _ := f.Next(10)
	if len(rest) != 3 || rest[0].Sample.SourceID != "s5" {
		t.Fatalf("rest = %+v", rest)
	}
	f.Ack(rest[2].End)
	if f.Pending() != 0 || f.Bytes() != 0 {
		t.Fatalf("Pending=%d Bytes=%d, want 0 and 0", f.Pending(), f.Bytes())
	}
	if info, err := os.Stat(path); err != nil || info.Size() != 0 {
		t.Fatalf("the file must be empty again, got %v %v", info, err)
	}
}

func TestSpool_KeepsWhatWasNotAcknowledgedAcrossARestart(t *testing.T) {
	f, path := open(t, 1<<20)
	_ = f.Append(samples(0, 4))
	got, _ := f.Next(2)
	f.Ack(got[1].End) // two delivered, the process stops
	again, err := Open(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	// the new process cannot know about the acknowledgement: it replays all four (at least once)
	if again.Pending() != 4 {
		t.Fatalf("Pending() after restart = %d, want 4", again.Pending())
	}
	all, _ := again.Next(10)
	if len(all) != 4 || all[0].Sample.SourceID != "s0" {
		t.Fatalf("replay = %+v", all)
	}
}

func TestSpool_RefusesAppendBeyondItsByteBudgetAndKeepsWhatItHas(t *testing.T) {
	f, _ := open(t, 700)
	if err := f.Append(samples(0, 3)); err != nil {
		t.Fatal(err)
	}
	if err := f.Append(samples(3, 10)); !errors.Is(err, ErrFull) {
		t.Fatalf("err = %v, want ErrFull", err)
	}
	if f.Pending() != 3 {
		t.Fatalf("a refused append must store nothing: Pending() = %d, want 3", f.Pending())
	}
}

func TestSpool_CutsATornLastLineAndSkipsDamagedOnes(t *testing.T) {
	f, path := open(t, 1<<20)
	_ = f.Append(samples(0, 2))
	file, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o640)
	_, _ = file.WriteString("not json at all\n{\"sourceId\":\"torn")
	_ = file.Close()
	again, err := Open(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := again.Next(10)
	if len(got) != 2 {
		t.Fatalf("got %d samples, want the 2 valid ones (the damaged and the torn line are not samples)", len(got))
	}
	again.Ack(got[1].End)
	next, _ := again.Next(10)
	if len(next) != 0 {
		t.Fatalf("nothing valid should remain, got %+v", next)
	}
	if again.Pending() != 0 {
		t.Fatalf("Pending() = %d, want 0 after the damaged line was acknowledged away", again.Pending())
	}
}

func TestSpool_OnlyDamagedLinesNeverBlockTheQueue(t *testing.T) {
	f, path := open(t, 1<<20)
	if err := os.WriteFile(path, []byte("garbage\nmore garbage\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	again, _ := Open(path, 1<<20)
	if got, _ := again.Next(5); len(got) != 0 {
		t.Fatalf("got %+v, want nothing", got)
	}
	if again.Pending() != 0 {
		t.Fatalf("Pending() = %d, want 0", again.Pending())
	}
	_ = f
}

func TestSpool_NeedsAPositiveBudget(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "x"), 0); err == nil {
		t.Fatal("a zero budget must be refused")
	}
}
