package main

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/hoangdodev/jcode-agentpet-bridge/internal/jcode"
)

// Bug #4: Detail truncation must not cut a multi-byte rune, which would
// produce a U+FFFD in the JSON payload and show as mojibake in AgentPet.
func TestComposeMessageRuneSafeTruncate(t *testing.T) {
	// 127 runes, 175 bytes. The pre-fix `d[:80]` cuts byte 80 mid-rune for
	// this input (verified: produces `\uFFFD` in JSON marshal).
	long := "đây là một chuỗi tiếng Việt dài đây là một chuỗi tiếng Việt dài đây là một chuỗi tiếng Việt dài đây là một chuỗi tiếng Việt dài"
	if utf8.RuneCountInString(long) <= 80 {
		t.Fatalf("test input must be >80 runes, got %d", utf8.RuneCountInString(long))
	}
	msg := composeMessage(jcode.Session{Detail: long})
	if !utf8.ValidString(msg) {
		t.Fatalf("composeMessage produced invalid utf-8: %q", msg)
	}
	if strings.ContainsRune(msg, '\uFFFD') {
		t.Fatalf("composeMessage produced replacement char: %q", msg)
	}
	if utf8.RuneCountInString(msg) > 80 {
		t.Fatalf("expected <=80 runes, got %d", utf8.RuneCountInString(msg))
	}
}

func TestComposeMessageOrdering(t *testing.T) {
	got := composeMessage(jcode.Session{
		FriendlyName: "fn",
		Model:        "claude",
		Detail:       "doing stuff",
	})
	want := "fn | claude | doing stuff"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestComposeMessageSkipsEmpty(t *testing.T) {
	got := composeMessage(jcode.Session{Model: "m"})
	if got != "m" {
		t.Fatalf("got %q", got)
	}
}
