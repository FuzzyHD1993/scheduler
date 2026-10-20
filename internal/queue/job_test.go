package queue

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestTruncateErrorProducesValidUTF8(t *testing.T) {
	// Invalid UTF-8 bytes are replaced with U+FFFD even when the
	// message is shorter than the limit.
	in := string([]byte{0xff, 0xfe, 'x'})
	got := truncateError(in, 2048)
	if !utf8.ValidString(got) {
		t.Fatalf("result %q is not valid UTF-8", got)
	}
	if !strings.Contains(got, "\uFFFD") {
		t.Fatalf("expected replacement characters in %q", got)
	}

	// Mixed valid/invalid input: valid runes survive, invalid bytes
	// become U+FFFD, and everything still round-trips into UTF-8.
	in = "héllo" + string([]byte{0xff}) + "wörld"
	got = truncateError(in, lastErrorLimit)
	if !utf8.ValidString(got) || !strings.Contains(got, "\uFFFD") {
		t.Fatalf("sanitization broken: %q", got)
	}
}

func TestTruncateErrorCutsOnRuneBoundary(t *testing.T) {
	// "жж" is 2 runes x 2 bytes = 4 bytes; limit 3 must not split the
	// second rune.
	got := truncateError("жж", 3)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2 (one rune)", len(got))
	}
	if got != "ж" {
		t.Fatalf("got %q, want the first rune intact", got)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("cut produced invalid UTF-8")
	}
}

func TestTruncateErrorRespectsLimit(t *testing.T) {
	in := strings.Repeat("ab", 3000)
	got := truncateError(in, lastErrorLimit)
	if len(got) != lastErrorLimit {
		t.Fatalf("len = %d, want %d", len(got), lastErrorLimit)
	}
	if got != in[:lastErrorLimit] {
		t.Fatal("ASCII truncation changed the prefix")
	}
}
