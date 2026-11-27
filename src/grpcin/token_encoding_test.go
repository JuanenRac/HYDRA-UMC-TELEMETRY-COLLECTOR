// HYDRA-UMC-TELEMETRY-COLLECTOR - grpcin/token_encoding_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package grpcin

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTheTokenFileMayCarryAByteOrderMarkButNoOtherEncoding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "grpc.token")
	if err := os.WriteFile(path, []byte("\ufeffa-token-of-the-collector\r\nsecond line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadToken(path); err != nil || got != "a-token-of-the-collector" {
		t.Fatalf("with a mark: %q, %v", got, err)
	}
	if err := os.WriteFile(path, []byte{0xFF, 0xFE, 'a', 0, 'b', 0, '\n', 0}, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadToken(path); err == nil {
		t.Fatal("a UTF-16 token file was accepted")
	}
}
