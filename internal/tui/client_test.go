package tui

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// recordingServer is a minimal stand-in for the scheduler API.
type recordingServer struct {
	mu       sync.Mutex
	lastBody []byte
	lastPath string
	lastVerb string
	status   int
	response string
}

func (s *recordingServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	s.lastBody = body
	s.lastPath = r.URL.RequestURI()
	s.lastVerb = r.Method
	status, resp := s.status, s.response
	s.mu.Unlock()

	if status != 0 {
		w.WriteHeader(status)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	if resp != "" {
		_, _ = io.WriteString(w, resp)
	}
}

func newTestClient(t *testing.T, srv *recordingServer) (*Client, *httptest.Server) {
	t.Helper()
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return NewClient(ts.URL), ts
}

func TestListParsesJobsAndQuery(t *testing.T) {
	srv := &recordingServer{response: `{"jobs":[
		{"id":7,"type":"http","status":"pending","attempts":0,"max_attempts":3},
		{"id":8,"type":"noop","status":"dead","attempts":3,"max_attempts":3}
	],"count":2}`}
	c, _ := newTestClient(t, srv)

	jobs, err := c.List(context.Background(), "pending", 50)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(jobs) != 2 || jobs[0].ID != 7 || jobs[1].Type != "noop" {
		t.Fatalf("unexpected jobs: %+v", jobs)
	}
	if srv.lastVerb != http.MethodGet {
		t.Fatalf("verb = %s, want GET", srv.lastVerb)
	}
	if srv.lastPath != "/jobs?limit=50&status=pending" {
		t.Fatalf("path = %q", srv.lastPath)
	}
}

func TestListWithoutFilters(t *testing.T) {
	srv := &recordingServer{response: `{"jobs":[]}`}
	c, _ := newTestClient(t, srv)

	if _, err := c.List(context.Background(), "", 0); err != nil {
		t.Fatalf("List: %v", err)
	}
	if srv.lastPath != "/jobs" {
		t.Fatalf("path = %q, want /jobs", srv.lastPath)
	}
}

func TestAPIErrorCarriesServerMessage(t *testing.T) {
	srv := &recordingServer{status: http.StatusBadRequest, response: `{"error":"unknown job type"}`}
	c, _ := newTestClient(t, srv)

	_, err := c.Create(context.Background(), CreateRequest{Type: "nope"})
	if err == nil {
		t.Fatal("expected an error")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error is not *APIError: %v", err)
	}
	if apiErr.Status != http.StatusBadRequest || apiErr.Message != "unknown job type" {
		t.Fatalf("unexpected APIError: %+v", apiErr)
	}
	if !strings.Contains(err.Error(), "unknown job type") {
		t.Fatalf("error text lost the server message: %v", err)
	}
}

func TestCreateSendsRequestAndParsesJob(t *testing.T) {
	srv := &recordingServer{
		status:   http.StatusCreated,
		response: `{"job":{"id":42,"type":"http","status":"pending","attempts":0,"max_attempts":5}}`,
	}
	c, _ := newTestClient(t, srv)

	attempts := 5
	job, err := c.Create(context.Background(), CreateRequest{
		Type:        "http",
		Payload:     json.RawMessage(`{"url":"https://example.com"}`),
		MaxAttempts: &attempts,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if job.ID != 42 || job.MaxAttempts != 5 {
		t.Fatalf("unexpected job: %+v", job)
	}

	var sent struct {
		Type        string          `json:"type"`
		Payload     json.RawMessage `json:"payload"`
		MaxAttempts *int            `json:"max_attempts"`
	}
	if err := json.Unmarshal(srv.lastBody, &sent); err != nil {
		t.Fatalf("request body is not JSON: %v (%q)", err, srv.lastBody)
	}
	if sent.Type != "http" || string(sent.Payload) != `{"url":"https://example.com"}` {
		t.Fatalf("unexpected body: %s", srv.lastBody)
	}
	if sent.MaxAttempts == nil || *sent.MaxAttempts != 5 {
		t.Fatalf("max_attempts not sent: %s", srv.lastBody)
	}
	if srv.lastVerb != http.MethodPost || srv.lastPath != "/jobs" {
		t.Fatalf("%s %s, want POST /jobs", srv.lastVerb, srv.lastPath)
	}
}

func TestRetryPostsToJob(t *testing.T) {
	srv := &recordingServer{status: http.StatusOK, response: `{"job":{"id":7}}`}
	c, _ := newTestClient(t, srv)

	if err := c.Retry(context.Background(), 7); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if srv.lastVerb != http.MethodPost || srv.lastPath != "/jobs/7/retry" {
		t.Fatalf("%s %s, want POST /jobs/7/retry", srv.lastVerb, srv.lastPath)
	}
}

func TestNewClientTrimsTrailingSlash(t *testing.T) {
	c := NewClient(" http://localhost:8080/ ")
	if c.BaseURL != "http://localhost:8080" {
		t.Fatalf("BaseURL = %q", c.BaseURL)
	}
}

func TestClientRejectsBrokenJSON(t *testing.T) {
	srv := &recordingServer{response: `not json`}
	c, _ := newTestClient(t, srv)

	if _, err := c.List(context.Background(), "", 0); err == nil {
		t.Fatal("expected a decode error")
	}
}
