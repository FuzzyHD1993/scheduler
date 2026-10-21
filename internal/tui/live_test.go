package tui

import (
	"os"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

// TestLiveAPI exercises the whole stack against a running scheduler:
// it loads the real job list, creates a job through the form and checks
// that the refreshed list contains it. It is skipped unless
// SCHEDULER_API_URL points at a live API, so `go test ./...` stays
// independent of the environment.
//
//	SCHEDULER_API_URL=http://localhost:18080 go test ./internal/tui -run TestLiveAPI -v
func TestLiveAPI(t *testing.T) {
	base := os.Getenv("SCHEDULER_API_URL")
	if base == "" {
		t.Skip("SCHEDULER_API_URL is not set")
	}

	m := New(NewClient(base))
	m, _ = step(t, m, runCmd(t, m.Init()))
	if m.errMsg != "" {
		t.Fatalf("initial load failed: %s", m.errMsg)
	}

	// Fill the form: noop job with a small payload and one attempt.
	m, _ = press(t, m, runes("n"))
	m, _ = press(t, m, runes("noop"))
	m, _ = press(t, m, keyOf(tea.KeyTab))
	m, _ = press(t, m, runes(`{"source":"tui-live-test"}`))
	m, _ = press(t, m, keyOf(tea.KeyTab))
	m, _ = press(t, m, runes("1"))

	m, cmd := press(t, m, keyOf(tea.KeyEnter))
	if cmd == nil {
		t.Fatalf("the form was rejected: %q", m.inputErr)
	}

	m, refresh := step(t, m, runCmd(t, cmd))
	if m.inputErr != "" {
		t.Fatalf("create failed: %s", m.inputErr)
	}
	m, _ = step(t, m, runCmd(t, refresh))

	// The list is capped at listLimit, so look for the job itself
	// instead of comparing counts.
	var found bool
	for _, j := range m.jobs {
		if string(j.Payload) == `{"source":"tui-live-test"}` {
			found = true
		}
	}
	if !found {
		t.Fatalf("the created job is not in the refreshed list (%d jobs)", len(m.jobs))
	}
	if !strings.Contains(m.flash, "создана") {
		t.Fatalf("flash = %q", m.flash)
	}

	// The created job must be visible in the rendered table.
	view := m.View()
	if !strings.Contains(view, "noop") {
		t.Fatalf("the created job is not rendered:\n%s", view)
	}
	t.Logf("создана задача, всего задач: %d", len(m.jobs))
}
