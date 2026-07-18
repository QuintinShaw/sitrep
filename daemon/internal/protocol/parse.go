// Package protocol parses the Sitrep stdout line convention (proto/SPEC.md).
package protocol

import (
	"fmt"
	"strconv"
	"strings"
)

type Kind string

const (
	TaskStart    Kind = "task.start"
	TaskProgress Kind = "task.progress"
	TaskStep     Kind = "task.step"
	TaskDone     Kind = "task.done"
	TaskFail     Kind = "task.fail"
	MetricUpdate Kind = "metric.update"
	MessageSend  Kind = "message.send"
	TaskLog      Kind = "task.log" // daemon-generated output tail (not a stdout verb)
)

const sentinel = "::sitrep "

// Event is one parsed protocol line. Fields are populated per Kind.
type Event struct {
	Kind    Kind   `json:"kind"`
	Title   string `json:"title,omitempty"`
	Percent int    `json:"percent,omitempty"`
	Step    string `json:"step,omitempty"`
	Key     string `json:"key,omitempty"`
	Value   string `json:"value,omitempty"`
	Label   string `json:"label,omitempty"`
	Text    string `json:"text,omitempty"`
	Level   string `json:"level,omitempty"`
	// Presentation hints (docs/design/presentation.md); free-form strings,
	// clients fall back to defaults on anything they don't recognize.
	Icon     string `json:"icon,omitempty"`
	Tint     string `json:"tint,omitempty"`
	Template string `json:"template,omitempty"`
	// Display metadata for metrics: renderers derive gauges/goal text from
	// value+target — evaluation stays in the emitting script.
	Target string `json:"target,omitempty"`
	Min    string `json:"min,omitempty"`
	Max    string `json:"max,omitempty"`
	// Alert lines: the SERVER does edge detection (notify once on crossing,
	// re-arm when the value comes back) so scripts just restate thresholds.
	AlertAbove string `json:"alert_above,omitempty"`
	AlertBelow string `json:"alert_below,omitempty"`
}

// parseHints strips leading --key=value flags and returns the remainder.
func parseHints(args string, ev *Event) string {
	for strings.HasPrefix(args, "--") {
		flag, rest, _ := strings.Cut(args, " ")
		k, v, ok := strings.Cut(strings.TrimPrefix(flag, "--"), "=")
		if !ok {
			break // not a hint flag (e.g. a bare "--"); leave for the caller
		}
		switch k {
		case "icon":
			ev.Icon = v
		case "tint":
			ev.Tint = v
		case "template":
			ev.Template = v
		case "level":
			ev.Level = v
		case "target":
			ev.Target = v
		case "min":
			ev.Min = v
		case "max":
			ev.Max = v
		case "alert-above":
			ev.AlertAbove = v
		case "alert-below":
			ev.AlertBelow = v
		default:
			// Unknown hints are dropped, not errors: forward compatibility.
		}
		args = strings.TrimSpace(rest)
	}
	return args
}

// ParseLine parses a single output line. ok is false when the line is not a
// sitrep line at all (normal program output). err is non-nil when the line
// carries the sentinel but is malformed; per spec such lines are ignored by
// callers, never fatal.
func ParseLine(line string) (ev Event, ok bool, err error) {
	s := strings.TrimLeft(line, " \t")
	if !strings.HasPrefix(s, sentinel) {
		return Event{}, false, nil
	}
	rest := strings.TrimSpace(s[len(sentinel):])
	verb, args, _ := strings.Cut(rest, " ")
	args = strings.TrimSpace(args)
	if strings.HasPrefix(args, "{") {
		// JSON form is reserved in v0: tolerate, ignore payload.
		args = ""
	}
	hints := Event{}
	args = parseHints(args, &hints)

	switch Kind(verb) {
	case TaskStart:
		hints.Kind = TaskStart
		hints.Title = unquote(args)
		return hints, true, nil
	case TaskProgress:
		pctStr, step, _ := strings.Cut(args, " ")
		pct, perr := strconv.Atoi(pctStr)
		if perr != nil || pct < 0 || pct > 100 {
			return Event{}, true, fmt.Errorf("task.progress: bad percent %q", pctStr)
		}
		return Event{Kind: TaskProgress, Percent: pct, Step: unquote(strings.TrimSpace(step))}, true, nil
	case TaskStep:
		if args == "" {
			return Event{}, true, fmt.Errorf("task.step: missing step text")
		}
		return Event{Kind: TaskStep, Step: unquote(args)}, true, nil
	case TaskDone:
		return Event{Kind: TaskDone, Text: unquote(args)}, true, nil
	case TaskFail:
		return Event{Kind: TaskFail, Text: unquote(args)}, true, nil
	case MetricUpdate:
		key, rest2, _ := strings.Cut(args, " ")
		if !validKey(key) {
			return Event{}, true, fmt.Errorf("metric.update: bad key %q", key)
		}
		value, label := splitValueLabel(strings.TrimSpace(rest2))
		if value == "" {
			return Event{}, true, fmt.Errorf("metric.update: missing value")
		}
		hints.Kind = MetricUpdate
		hints.Key, hints.Value, hints.Label = key, value, label
		return hints, true, nil
	case MessageSend:
		if hints.Level == "" {
			hints.Level = "info"
		}
		if hints.Level != "info" && hints.Level != "warn" && hints.Level != "error" {
			return Event{}, true, fmt.Errorf("message.send: bad level %q", hints.Level)
		}
		if args == "" {
			return Event{}, true, fmt.Errorf("message.send: missing text")
		}
		hints.Kind = MessageSend
		hints.Text = unquote(args)
		return hints, true, nil
	default:
		return Event{}, true, fmt.Errorf("unknown verb %q", verb)
	}
}

// unquote strips one matching pair of single or double quotes.
func unquote(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// splitValueLabel splits `<value> [label]` where value is a single token and
// label (possibly quoted) runs to end of line.
func splitValueLabel(s string) (value, label string) {
	value, rest, _ := strings.Cut(s, " ")
	return value, unquote(strings.TrimSpace(rest))
}

func validKey(key string) bool {
	if key == "" || len(key) > 64 {
		return false
	}
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '.', r == '-':
		default:
			return false
		}
	}
	return true
}
