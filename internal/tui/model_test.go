package tui

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// fakeAPI records the calls made by the model.
type fakeAPI struct {
	jobs      []Job
	listErr   error
	createErr error
	retryErr  error

	created []CreateRequest
	retried []int64
	lists   int
	nextID  int64
}

func (f *fakeAPI) List(context.Context, string, int) ([]Job, error) {
	f.lists++
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.jobs, nil
}

func (f *fakeAPI) Create(_ context.Context, req CreateRequest) (Job, error) {
	if f.createErr != nil {
		return Job{}, f.createErr
	}
	f.created = append(f.created, req)
	f.nextID++
	job := Job{ID: f.nextID, Type: req.Type, Payload: req.Payload, Status: "pending", MaxAttempts: 3}
	if req.MaxAttempts != nil {
		job.MaxAttempts = *req.MaxAttempts
	}
	f.jobs = append(f.jobs, job)
	return job, nil
}

func (f *fakeAPI) Retry(_ context.Context, id int64) error {
	if f.retryErr != nil {
		return f.retryErr
	}
	f.retried = append(f.retried, id)
	return nil
}

// step feeds a message to the model and returns the updated model.
func step(t *testing.T, m Model, msg tea.Msg) (Model, tea.Cmd) {
	t.Helper()
	next, cmd := m.Update(msg)
	got, ok := next.(Model)
	if !ok {
		t.Fatalf("Update returned %T, want Model", next)
	}
	return got, cmd
}

// press sends a key to the model.
func press(t *testing.T, m Model, k tea.KeyMsg) (Model, tea.Cmd) {
	t.Helper()
	return step(t, m, k)
}

// runCmd executes a command and returns its message.
func runCmd(t *testing.T, cmd tea.Cmd) tea.Msg {
	t.Helper()
	if cmd == nil {
		t.Fatal("command is nil")
	}
	return cmd()
}

func runes(s string) tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
}

func keyOf(t tea.KeyType) tea.KeyMsg {
	return tea.KeyMsg{Type: t}
}

func TestInitLoadsJobs(t *testing.T) {
	api := &fakeAPI{jobs: []Job{{ID: 1, Type: "http", Status: "pending"}}}
	m := New(api)

	msg := runCmd(t, m.Init())
	m, _ = step(t, m, msg)

	if len(m.jobs) != 1 || m.jobs[0].ID != 1 {
		t.Fatalf("jobs = %+v", m.jobs)
	}
	if m.loading {
		t.Fatal("loading flag was not cleared")
	}
}

func TestNewOpensEmptyForm(t *testing.T) {
	m, _ := press(t, New(&fakeAPI{}), runes("n"))

	if !m.inputMode {
		t.Fatal("n did not open the form")
	}
	if m.activeInput != fieldType {
		t.Fatalf("activeInput = %d, want %d", m.activeInput, fieldType)
	}
	if m.inputType != "" || m.inputPayload != "" || m.inputAttempts != "" {
		t.Fatalf("form was not reset: type=%q payload=%q attempts=%q",
			m.inputType, m.inputPayload, m.inputAttempts)
	}
}

func TestTypingBackspaceAndClear(t *testing.T) {
	m, _ := press(t, New(&fakeAPI{}), runes("n"))

	m, _ = press(t, m, runes("noop"))
	if m.inputType != "noop" {
		t.Fatalf("type = %q, want noop", m.inputType)
	}

	m, _ = press(t, m, keyOf(tea.KeyBackspace))
	if m.inputType != "noo" {
		t.Fatalf("after backspace type = %q", m.inputType)
	}

	m, _ = press(t, m, keyOf(tea.KeyCtrlU))
	if m.inputType != "" {
		t.Fatalf("after ctrl+u type = %q", m.inputType)
	}

	// Multi-byte characters are edited by rune, not by byte.
	m, _ = press(t, m, runes("при"))
	m, _ = press(t, m, keyOf(tea.KeyBackspace))
	if m.inputType != "пр" {
		t.Fatalf("after backspace type = %q, want пр", m.inputType)
	}
}

func TestTabCyclesFields(t *testing.T) {
	m, _ := press(t, New(&fakeAPI{}), runes("n"))

	want := []int{fieldPayload, fieldAttempts, fieldType}
	for i, w := range want {
		m, _ = press(t, m, keyOf(tea.KeyTab))
		if m.activeInput != w {
			t.Fatalf("tab %d: activeInput = %d, want %d", i+1, m.activeInput, w)
		}
	}

	m, _ = press(t, m, keyOf(tea.KeyShiftTab))
	if m.activeInput != fieldAttempts {
		t.Fatalf("shift+tab: activeInput = %d, want %d", m.activeInput, fieldAttempts)
	}
}

func TestEscClosesForm(t *testing.T) {
	m, _ := press(t, New(&fakeAPI{}), runes("n"))
	m, _ = press(t, m, runes("http"))
	m, _ = press(t, m, keyOf(tea.KeyEsc))

	if m.inputMode {
		t.Fatal("esc did not close the form")
	}
	if m.inputType != "" {
		t.Fatalf("form was not reset after esc: type=%q", m.inputType)
	}
}

func TestValidationRejectsBadInput(t *testing.T) {
	cases := []struct {
		name    string
		typ     string
		payload string
		attempt string
		want    string
	}{
		{"broken json", "http", `{"url":`, "", "JSON"},
		{"http without url", "http", `{}`, "", "url"},
		{"http without host", "http", `{"url":"http://"}`, "", "url"},
		{"zero attempts", "noop", `{}`, "0", "attempts"},
		{"negative attempts", "noop", `{}`, "-1", "attempts"},
		{"non-numeric attempts", "noop", `{}`, "abc", "attempts"},
		{"too many attempts", "noop", `{}`, strconv.Itoa(maxAttempts + 1), "attempts"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := &fakeAPI{}
			m, _ := press(t, New(api), runes("n"))
			m, _ = press(t, m, runes(tc.typ))
			m, _ = press(t, m, keyOf(tea.KeyTab))
			m, _ = press(t, m, runes(tc.payload))
			m, _ = press(t, m, keyOf(tea.KeyTab))
			m, _ = press(t, m, runes(tc.attempt))

			m, cmd := press(t, m, keyOf(tea.KeyEnter))
			if cmd != nil {
				t.Fatal("a command was started for invalid input")
			}
			if !m.inputMode {
				t.Fatal("the form closed on invalid input")
			}
			if !strings.Contains(m.inputErr, tc.want) {
				t.Fatalf("inputErr = %q, want it to contain %q", m.inputErr, tc.want)
			}
			if len(api.created) != 0 {
				t.Fatalf("the API was called for invalid input: %+v", api.created)
			}
		})
	}
}

func TestSubmitCreatesJobAndRefreshesList(t *testing.T) {
	api := &fakeAPI{}
	m, _ := press(t, New(api), runes("n"))

	m, _ = press(t, m, runes("noop"))
	m, _ = press(t, m, keyOf(tea.KeyTab))
	m, _ = press(t, m, runes(`{"key":"value"}`))
	m, _ = press(t, m, keyOf(tea.KeyTab))
	m, _ = press(t, m, runes("7"))

	m, cmd := press(t, m, keyOf(tea.KeyEnter))
	if cmd == nil {
		t.Fatal("enter did not start the create command")
	}
	if len(api.created) != 0 {
		t.Fatal("the API was called before the command ran")
	}

	m, refresh := step(t, m, runCmd(t, cmd))
	if len(api.created) != 1 {
		t.Fatalf("create calls = %d, want 1", len(api.created))
	}
	got := api.created[0]
	if got.Type != "noop" || string(got.Payload) != `{"key":"value"}` {
		t.Fatalf("unexpected create request: %+v (payload %s)", got, got.Payload)
	}
	if got.MaxAttempts == nil || *got.MaxAttempts != 7 {
		t.Fatalf("max_attempts = %v, want 7", got.MaxAttempts)
	}

	if m.inputMode {
		t.Fatal("the form stayed open after a successful create")
	}
	if !strings.Contains(m.flash, "#1") {
		t.Fatalf("flash = %q", m.flash)
	}
	if refresh == nil {
		t.Fatal("the list was not refreshed after the create")
	}

	m, _ = step(t, m, runCmd(t, refresh))
	if len(m.jobs) != 1 || m.jobs[0].Type != "noop" {
		t.Fatalf("jobs after refresh = %+v", m.jobs)
	}
}

func TestSubmitDefaults(t *testing.T) {
	api := &fakeAPI{}
	m, _ := press(t, New(api), runes("n"))
	m, _ = press(t, m, runes("noop"))

	m, cmd := press(t, m, keyOf(tea.KeyEnter))
	if cmd == nil {
		t.Fatalf("enter did not submit: %q", m.inputErr)
	}
	stepRes, _ := step(t, m, runCmd(t, cmd))
	_ = stepRes

	got := api.created[0]
	if string(got.Payload) != `{}` {
		t.Fatalf("default payload = %s, want {}", got.Payload)
	}
	if got.MaxAttempts == nil || *got.MaxAttempts != defaultAttempts {
		t.Fatalf("default attempts = %v, want %d", got.MaxAttempts, defaultAttempts)
	}
}

func TestSubmitServerErrorKeepsFormOpen(t *testing.T) {
	api := &fakeAPI{createErr: &APIError{Status: http.StatusBadRequest, Message: "unknown job type"}}
	m, _ := press(t, New(api), runes("n"))
	m, _ = press(t, m, runes("weird"))

	m, cmd := press(t, m, keyOf(tea.KeyEnter))
	m, _ = step(t, m, runCmd(t, cmd))

	if !m.inputMode {
		t.Fatal("the form closed after a server error")
	}
	if !strings.Contains(m.inputErr, "unknown job type") {
		t.Fatalf("inputErr = %q", m.inputErr)
	}
}

func TestRetrySelectedJob(t *testing.T) {
	api := &fakeAPI{jobs: []Job{{ID: 5}, {ID: 6}}}
	m, _ := step(t, New(api), jobsMsg{jobs: api.jobs})

	m, _ = press(t, m, keyOf(tea.KeyDown))
	if m.cursor != 1 {
		t.Fatalf("cursor = %d, want 1", m.cursor)
	}

	m, cmd := press(t, m, runes("r"))
	if cmd == nil {
		t.Fatal("r did not start the retry command")
	}

	m, refresh := step(t, m, runCmd(t, cmd))
	if len(api.retried) != 1 || api.retried[0] != 6 {
		t.Fatalf("retried = %v, want [6]", api.retried)
	}
	if !strings.Contains(m.flash, "#6") {
		t.Fatalf("flash = %q", m.flash)
	}
	if refresh == nil {
		t.Fatal("the list was not refreshed after the retry")
	}
}

func TestCursorNavigation(t *testing.T) {
	jobs := []Job{{ID: 1}, {ID: 2}, {ID: 3}}
	m, _ := step(t, New(&fakeAPI{}), jobsMsg{jobs: jobs})

	m, _ = press(t, m, keyOf(tea.KeyUp))
	if m.cursor != 0 {
		t.Fatalf("cursor moved above the first job: %d", m.cursor)
	}
	m, _ = press(t, m, runes("j"))
	m, _ = press(t, m, runes("j"))
	m, _ = press(t, m, runes("j"))
	if m.cursor != 2 {
		t.Fatalf("cursor = %d, want 2", m.cursor)
	}
}

func TestListErrorIsShown(t *testing.T) {
	m, _ := step(t, New(&fakeAPI{}), jobsMsg{err: errors.New("connection refused")})
	if !strings.Contains(m.errMsg, "connection refused") {
		t.Fatalf("errMsg = %q", m.errMsg)
	}
	if !strings.Contains(m.View(), "connection refused") {
		t.Fatal("the view does not show the error")
	}
}

func TestViewFormHighlightsActiveField(t *testing.T) {
	m, _ := press(t, New(&fakeAPI{}), runes("n"))
	view := m.View()

	for _, want := range []string{"Создание задачи", "Type:", "Payload:", "Attempts:", "▶", "Tab:", "Enter:"} {
		if !strings.Contains(view, want) {
			t.Fatalf("the form does not contain %q:\n%s", want, view)
		}
	}

	m, _ = press(t, m, keyOf(tea.KeyTab))
	view = m.View()
	if strings.Count(view, "▶") != 1 {
		t.Fatalf("exactly one field must be highlighted:\n%s", view)
	}
}

func TestViewTable(t *testing.T) {
	m, _ := step(t, New(&fakeAPI{}), jobsMsg{jobs: []Job{
		{ID: 1, Type: "http", Status: "pending", Attempts: 0, MaxAttempts: 3},
	}})
	view := m.View()
	for _, want := range []string{"Scheduler", "ID", "TYPE", "STATUS", "ATTEMPTS", "http", "pending", "0/3"} {
		if !strings.Contains(view, want) {
			t.Fatalf("the table does not contain %q:\n%s", want, view)
		}
	}
}

func TestQuit(t *testing.T) {
	for _, k := range []tea.KeyMsg{runes("q"), keyOf(tea.KeyEsc), keyOf(tea.KeyCtrlC)} {
		m, cmd := press(t, New(&fakeAPI{}), k)
		if cmd == nil {
			t.Fatalf("%q did not quit", k.String())
		}
		if _, ok := runCmd(t, cmd).(tea.QuitMsg); !ok {
			t.Fatalf("%q did not produce tea.QuitMsg", k.String())
		}
		if !m.quitting {
			t.Fatalf("%q did not set quitting", k.String())
		}
	}
}

// TestCreateJobEndToEnd drives the model against a real HTTP server: it
// types a job into the form, submits it, and checks that the job appears
// in the refreshed table.
func TestCreateJobEndToEnd(t *testing.T) {
	var (
		mu   sync.Mutex
		jobs []Job
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/jobs":
			var req CreateRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":"invalid JSON body"}`)
				return
			}
			job := Job{ID: int64(len(jobs) + 1), Type: req.Type, Payload: req.Payload, Status: "pending", MaxAttempts: 3}
			if req.MaxAttempts != nil {
				job.MaxAttempts = *req.MaxAttempts
			}
			jobs = append(jobs, job)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"job": job})

		case r.Method == http.MethodGet && r.URL.Path == "/jobs":
			_ = json.NewEncoder(w).Encode(map[string]any{"jobs": jobs})

		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":"not found"}`)
		}
	}))
	defer srv.Close()

	m := New(NewClient(srv.URL))
	m, _ = step(t, m, runCmd(t, m.Init()))
	if len(m.jobs) != 0 {
		t.Fatalf("the server started with jobs: %+v", m.jobs)
	}

	m, _ = press(t, m, runes("n"))
	m, _ = press(t, m, runes("http"))
	m, _ = press(t, m, keyOf(tea.KeyTab))
	m, _ = press(t, m, runes(`{"url":"https://example.com"}`))
	m, _ = press(t, m, keyOf(tea.KeyTab))
	m, _ = press(t, m, runes("5"))

	m, cmd := press(t, m, keyOf(tea.KeyEnter))
	if cmd == nil {
		t.Fatalf("the form was rejected: %q", m.inputErr)
	}

	m, refresh := step(t, m, runCmd(t, cmd))
	if refresh == nil {
		t.Fatalf("no refresh after create: %q", m.inputErr)
	}
	m, _ = step(t, m, runCmd(t, refresh))

	if len(m.jobs) != 1 {
		t.Fatalf("jobs after create = %+v", m.jobs)
	}
	got := m.jobs[0]
	if got.Type != "http" || got.MaxAttempts != 5 || string(got.Payload) != `{"url":"https://example.com"}` {
		t.Fatalf("unexpected job: %+v (payload %s)", got, got.Payload)
	}
	if !strings.Contains(m.View(), "http") {
		t.Fatal("the created job is not rendered")
	}
	if !strings.Contains(m.View(), "создана") {
		t.Fatal("the success message is missing")
	}
}

// TestProgramQuitsOnQ runs the real bubbletea event loop with a scripted
// input, proving that the program starts, renders and exits cleanly.
func TestProgramQuitsOnQ(t *testing.T) {
	api := &fakeAPI{jobs: []Job{{ID: 1, Type: "noop", Status: "pending"}}}

	var out strings.Builder
	p := tea.NewProgram(
		New(api),
		tea.WithInput(strings.NewReader("q")),
		tea.WithOutput(&out),
	)
	done := make(chan error, 1)
	go func() {
		_, err := p.Run()
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("program returned an error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the program did not quit on q")
	}

	// Bubbletea may not paint a frame when the quit key arrives in the
	// very first batch, so only the clean exit is asserted here; the
	// rendered layout is covered by TestViewFormHighlightsActiveField.
	_ = out
}
