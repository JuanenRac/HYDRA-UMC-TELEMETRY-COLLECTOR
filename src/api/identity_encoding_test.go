// HYDRA-UMC-TELEMETRY-COLLECTOR - api/identity_encoding_test.go
// Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
// GPL-3.0 - see LICENSE
package api

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf16"
)

// wide is the text as `>` of Windows PowerShell writes it: UTF-16 little endian with its mark.
func wide(text string) []byte {
	out := []byte{0xFF, 0xFE}
	for _, unit := range utf16.Encode([]rune(text)) {
		out = append(out, byte(unit), byte(unit>>8))
	}
	return out
}

func TestTheIdentityKeyFileMayCarryAByteOrderMarkButNoOtherEncoding(t *testing.T) {
	key := strings.Repeat("k", MinIdentityKeyLength)
	path := filepath.Join(t.TempDir(), "identity.key")
	if err := os.WriteFile(path, []byte("\ufeff"+key+"\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadIdentityKey(path); err != nil || string(got) != key {
		t.Fatalf("with a mark: %q, %v", got, err)
	}
	if err := os.WriteFile(path, wide(key), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadIdentityKey(path); err == nil {
		t.Fatal("a UTF-16 key file was accepted")
	}
}
