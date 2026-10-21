package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Input fields of the creation form.
const (
	fieldType = iota
	fieldPayload
	fieldAttempts
	fieldCount
)

const (
	// defaultAttempts is sent when the attempts field is left empty.
	defaultAttempts = 3
	// maxAttempts mirrors the API limit on max_attempts.
	maxAttempts = 100
	// listLimit is the number of jobs shown in the table.
	listLimit = 100
	// defaultType is used when the type field is left empty.
	defaultType = "http"
)

// Styles. lipgloss drops colours when the output is not a terminal, so
// the layout stays readable in tests and in redirected output.
var (
	styleTitle  = lipgloss.NewStyle().Bold(true)
	styleDim    = lipgloss.NewStyle().Faint(true)
	styleErr    = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	styleOK     = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))
	styleActive = lipgloss.NewStyle().Reverse(true)
	styleBox    = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1).MarginTop(1)
)

// Messages produced by the commands of the model.
type jobsMsg struct {
	jobs []Job
	err  error
}

type createdMsg struct {
	job Job
	err error
}

type retriedMsg struct {
	id  int64
	err error
}

// Model is the bubbletea model of the scheduler TUI.
type Model struct {
	api API

	jobs    []Job
	cursor  int
	loading bool
	errMsg  string
	flash   string

	// Creation form. inputMode is true while the form is open;
	// activeInput is the index of the field being edited.
	inputMode     bool
	inputType     string
	inputPayload  string
	inputAttempts string
	activeInput   int
	inputErr      string

	width, height int
	quitting      bool
}

// New returns a model that talks to api.
func New(api API) Model {
	return Model{api: api, loading: true}
}

// Init loads the job list.
func (m Model) Init() tea.Cmd {
	return m.fetchJobs()
}

// fetchJobs returns a command that reloads the job list.
func (m Model) fetchJobs() tea.Cmd {
	api := m.api
	return func() tea.Msg {
		jobs, err := api.List(context.Background(), "", listLimit)
		return jobsMsg{jobs: jobs, err: err}
	}
}

// Update handles key presses and the messages of the API commands.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil

	case tea.KeyMsg:
		// Ctrl+C is an unconditional escape hatch.
		if msg.String() == "ctrl+c" {
			m.quitting = true
			return m, tea.Quit
		}
		if m.inputMode {
			return m.updateForm(msg)
		}
		return m.updateList(msg)

	case jobsMsg:
		m.loading = false
		if msg.err != nil {
			m.errMsg = msg.err.Error()
			return m, nil
		}
		m.errMsg = ""
		m.jobs = msg.jobs
		if m.cursor >= len(m.jobs) {
			m.cursor = max(0, len(m.jobs)-1)
		}
		return m, nil

	case createdMsg:
		m.loading = false
		if msg.err != nil {
			// Stay in the form so the user can correct the input.
			m.inputErr = msg.err.Error()
			return m, nil
		}
		m.inputMode = false
		m.inputErr = ""
		m.resetFields()
		m.flash = fmt.Sprintf("Задача #%d создана", msg.job.ID)
		return m, m.fetchJobs()

	case retriedMsg:
		m.loading = false
		if msg.err != nil {
			m.errMsg = msg.err.Error()
			return m, nil
		}
		m.flash = fmt.Sprintf("Задача #%d поставлена в очередь", msg.id)
		return m, m.fetchJobs()
	}
	return m, nil
}

// updateList handles keys while the job table has the focus.
func (m Model) updateList(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "esc":
		m.quitting = true
		return m, tea.Quit

	case "n":
		m.inputMode = true
		m.activeInput = fieldType
		m.inputErr = ""
		m.resetFields()
		return m, nil

	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}

	case "down", "j":
		if m.cursor < len(m.jobs)-1 {
			m.cursor++
		}

	case "r":
		if len(m.jobs) == 0 {
			return m, nil
		}
		job := m.jobs[m.cursor]
		api := m.api
		m.flash = ""
		m.errMsg = ""
		return m, func() tea.Msg {
			err := api.Retry(context.Background(), job.ID)
			return retriedMsg{id: job.ID, err: err}
		}

	case "R":
		m.loading = true
		return m, m.fetchJobs()
	}
	return m, nil
}

// updateForm handles keys while the creation form has the focus.
func (m Model) updateForm(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEsc:
		m.inputMode = false
		m.inputErr = ""
		m.resetFields()
		return m, nil

	case tea.KeyTab, tea.KeyDown:
		m.activeInput = (m.activeInput + 1) % fieldCount
		return m, nil

	case tea.KeyShiftTab, tea.KeyUp:
		m.activeInput = (m.activeInput + fieldCount - 1) % fieldCount
		return m, nil

	case tea.KeyEnter:
		return m.submit()

	case tea.KeyBackspace:
		m.setField(trimLastRune(m.field()))
		return m, nil

	case tea.KeyCtrlU:
		m.setField("")
		return m, nil

	case tea.KeySpace:
		m.setField(m.field() + " ")
		return m, nil

	case tea.KeyRunes:
		m.setField(m.field() + string(msg.Runes))
		return m, nil
	}
	return m, nil
}

// submit validates the form and, when it is valid, sends the job.
func (m Model) submit() (tea.Model, tea.Cmd) {
	req, problem := m.buildRequest()
	if problem != "" {
		m.inputErr = problem
		return m, nil
	}

	m.inputErr = ""
	m.loading = true
	api := m.api
	return m, func() tea.Msg {
		job, err := api.Create(context.Background(), req)
		return createdMsg{job: job, err: err}
	}
}

// buildRequest turns the form into an API request, or returns a
// human-readable problem with the input.
func (m Model) buildRequest() (CreateRequest, string) {
	typ := strings.TrimSpace(m.inputType)
	if typ == "" {
		typ = defaultType
	}

	payload := strings.TrimSpace(m.inputPayload)
	if payload == "" {
		payload = "{}"
	}
	if !json.Valid([]byte(payload)) {
		return CreateRequest{}, "payload: невалидный JSON"
	}
	if typ == "http" {
		var p struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal([]byte(payload), &p); err != nil {
			return CreateRequest{}, "payload: для http нужен JSON-объект"
		}
		u, err := url.Parse(strings.TrimSpace(p.URL))
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return CreateRequest{}, `payload: для http нужен {"url":"http(s)://..."}`
		}
	}

	attempts := defaultAttempts
	if s := strings.TrimSpace(m.inputAttempts); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > maxAttempts {
			return CreateRequest{}, fmt.Sprintf("attempts: целое число от 1 до %d", maxAttempts)
		}
		attempts = n
	}

	return CreateRequest{
		Type:        typ,
		Payload:     json.RawMessage(payload),
		MaxAttempts: &attempts,
	}, ""
}

// resetFields clears the creation form.
func (m *Model) resetFields() {
	m.inputType = ""
	m.inputPayload = ""
	m.inputAttempts = ""
}

// field returns the value of the active field.
func (m Model) field() string {
	switch m.activeInput {
	case fieldType:
		return m.inputType
	case fieldPayload:
		return m.inputPayload
	default:
		return m.inputAttempts
	}
}

// setField writes the value of the active field.
func (m *Model) setField(v string) {
	switch m.activeInput {
	case fieldType:
		m.inputType = v
	case fieldPayload:
		m.inputPayload = v
	default:
		m.inputAttempts = v
	}
}

// trimLastRune removes the last character of s, never splitting a
// multi-byte rune.
func trimLastRune(s string) string {
	r := []rune(s)
	if len(r) == 0 {
		return s
	}
	return string(r[:len(r)-1])
}

// View renders the job table and, when open, the creation form.
func (m Model) View() string {
	if m.quitting {
		return ""
	}

	var b strings.Builder
	b.WriteString(styleTitle.Render("Scheduler"))
	b.WriteString(styleDim.Render(fmt.Sprintf("   задач: %d", len(m.jobs))))
	b.WriteString("\n")
	b.WriteString(styleDim.Render("n — создать · r — retry · R — обновить · ↑/↓ — выбор · q — выход"))
	b.WriteString("\n\n")

	switch {
	case len(m.jobs) > 0:
		b.WriteString(fmt.Sprintf("   %-6s %-10s %-11s %-10s %s\n",
			"ID", "TYPE", "STATUS", "ATTEMPTS", "CREATED"))
		for i, j := range m.jobs {
			marker := "  "
			if i == m.cursor {
				marker = "▶ "
			}
			line := fmt.Sprintf("%s%-6d %-10s %-11s %d/%-8d %s",
				marker, j.ID, j.Type, j.Status, j.Attempts, j.MaxAttempts,
				j.CreatedAt.Format("15:04:05"))
			if i == m.cursor {
				line = styleActive.Render(line)
			}
			b.WriteString(line)
			b.WriteString("\n")
		}
	case m.loading:
		b.WriteString(styleDim.Render("  загрузка…"))
		b.WriteString("\n")
	default:
		b.WriteString(styleDim.Render("  задач нет — нажмите n, чтобы создать"))
		b.WriteString("\n")
	}

	if m.errMsg != "" {
		b.WriteString("\n")
		b.WriteString(styleErr.Render("ошибка: " + m.errMsg))
		b.WriteString("\n")
	}
	if m.flash != "" {
		b.WriteString("\n")
		b.WriteString(styleOK.Render(m.flash))
		b.WriteString("\n")
	}
	if m.inputMode {
		b.WriteString(m.viewForm())
	}
	return b.String()
}

// viewForm renders the job creation form with the active field
// highlighted.
func (m Model) viewForm() string {
	lines := []string{
		styleTitle.Render("Создание задачи"),
		m.formLine(fieldType, "Type:", m.inputType, "http"),
		m.formLine(fieldPayload, "Payload:", m.inputPayload, `{"url":"https://example.com"}`),
		m.formLine(fieldAttempts, "Attempts:", m.inputAttempts, strconv.Itoa(defaultAttempts)),
		"",
		styleDim.Render("Tab: следующее поле · Enter: создать · Esc: отмена"),
	}
	if m.inputErr != "" {
		lines = append(lines, styleErr.Render("ошибка: "+m.inputErr))
	}
	return styleBox.Render(strings.Join(lines, "\n"))
}

// formLine renders one field, marking and highlighting the active one.
func (m Model) formLine(idx int, label, value, placeholder string) string {
	shown := value
	if shown == "" {
		shown = styleDim.Render(placeholder)
	}
	line := fmt.Sprintf("%-10s %s", label, shown)
	if idx == m.activeInput {
		return styleActive.Render("▶ " + line)
	}
	return "  " + line
}
