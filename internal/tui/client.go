// Package tui implements an interactive terminal UI for the scheduler:
// a live job list, retry of the selected job and a form for creating
// jobs through the HTTP API.
package tui

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Job is the read-only view of a job returned by the scheduler API.
type Job struct {
	ID          int64           `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	CreatedAt   time.Time       `json:"created_at"`
}

// CreateRequest is the JSON body of POST /jobs.
type CreateRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts *int            `json:"max_attempts,omitempty"`
}

// API is the part of the scheduler HTTP API the TUI depends on.
// The concrete implementation is *Client; tests provide their own.
type API interface {
	List(ctx context.Context, status string, limit int) ([]Job, error)
	Create(ctx context.Context, req CreateRequest) (Job, error)
	Retry(ctx context.Context, id int64) error
}

// APIError is a non-2xx response from the scheduler.
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("scheduler API: HTTP %d", e.Status)
	}
	return fmt.Sprintf("scheduler API: HTTP %d: %s", e.Status, e.Message)
}

// Client talks to the scheduler HTTP API.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// NewClient returns a client for baseURL, for example
// http://localhost:8080.
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		HTTP:    &http.Client{Timeout: 10 * time.Second},
	}
}

// do sends a request and returns the response body. A non-2xx status
// is converted into *APIError carrying the server's error message.
func (c *Client) do(ctx context.Context, method, path string, body any) ([]byte, error) {
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode request: %w", err)
		}
		rdr = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rdr)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("scheduler API: %w", err)
	}
	defer resp.Body.Close()

	// Job lists are small; the limit protects against a hostile or
	// broken server streaming an unbounded body into the TUI.
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		return nil, &APIError{Status: resp.StatusCode, Message: e.Error}
	}
	return data, nil
}

// List returns the most recent jobs, optionally filtered by status.
func (c *Client) List(ctx context.Context, status string, limit int) ([]Job, error) {
	q := url.Values{}
	if status != "" {
		q.Set("status", status)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	path := "/jobs"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}

	data, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Jobs []Job `json:"jobs"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("decode jobs: %w", err)
	}
	return resp.Jobs, nil
}

// Create queues a new job and returns it as stored by the server.
func (c *Client) Create(ctx context.Context, req CreateRequest) (Job, error) {
	data, err := c.do(ctx, http.MethodPost, "/jobs", req)
	if err != nil {
		return Job{}, err
	}
	var resp struct {
		Job Job `json:"job"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return Job{}, fmt.Errorf("decode job: %w", err)
	}
	return resp.Job, nil
}

// Retry puts a dead job back into the queue.
func (c *Client) Retry(ctx context.Context, id int64) error {
	_, err := c.do(ctx, http.MethodPost, "/jobs/"+strconv.FormatInt(id, 10)+"/retry", nil)
	return err
}
