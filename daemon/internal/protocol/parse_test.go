package protocol

import "testing"

func TestParseLine(t *testing.T) {
	cases := []struct {
		name    string
		line    string
		ok      bool
		wantErr bool
		want    Event
	}{
		{"plain output", "epoch 3/10 loss=0.42", false, false, Event{}},
		{"sentinel mid-line", "log: ::sitrep task.progress 50", false, false, Event{}},
		{"progress", "::sitrep task.progress 45 downloading model weights", true, false,
			Event{Kind: TaskProgress, Percent: 45, Step: "downloading model weights"}},
		{"progress no step", "::sitrep task.progress 100", true, false,
			Event{Kind: TaskProgress, Percent: 100}},
		{"progress leading whitespace", "  ::sitrep task.progress 5 x", true, false,
			Event{Kind: TaskProgress, Percent: 5, Step: "x"}},
		{"progress bad percent", "::sitrep task.progress abc", true, true, Event{}},
		{"progress out of range", "::sitrep task.progress 101", true, true, Event{}},
		{"step quoted", `::sitrep task.step "waiting for API rate limit"`, true, false,
			Event{Kind: TaskStep, Step: "waiting for API rate limit"}},
		{"start with title", "::sitrep task.start 'nightly build'", true, false,
			Event{Kind: TaskStart, Title: "nightly build"}},
		{"done", "::sitrep task.done all green", true, false,
			Event{Kind: TaskDone, Text: "all green"}},
		{"fail bare", "::sitrep task.fail", true, false, Event{Kind: TaskFail}},
		{"metric", "::sitrep metric.update gh_stars 1284", true, false,
			Event{Kind: MetricUpdate, Key: "gh_stars", Value: "1284"}},
		{"metric with label", `::sitrep metric.update btc_usd 67231.50 "BTC/USD"`, true, false,
			Event{Kind: MetricUpdate, Key: "btc_usd", Value: "67231.50", Label: "BTC/USD"}},
		{"metric bad key", "::sitrep metric.update GH*Stars 1", true, true, Event{}},
		{"metric missing value", "::sitrep metric.update gh_stars", true, true, Event{}},
		{"message", "::sitrep message.send Sketch 101.2 released", true, false,
			Event{Kind: MessageSend, Text: "Sketch 101.2 released", Level: "info"}},
		{"message with level", "::sitrep message.send --level=error 'training diverged, loss=NaN'", true, false,
			Event{Kind: MessageSend, Text: "training diverged, loss=NaN", Level: "error"}},
		{"start with hints", `::sitrep task.start --icon=brain.head.profile --tint=purple --template=timer "train resnet"`, true, false,
			Event{Kind: TaskStart, Title: "train resnet", Icon: "brain.head.profile", Tint: "purple", Template: "timer"}},
		{"metric with hints", "::sitrep metric.update --icon=star.fill --tint=orange gh_stars 1284 GitHub", true, false,
			Event{Kind: MetricUpdate, Key: "gh_stars", Value: "1284", Label: "GitHub", Icon: "star.fill", Tint: "orange"}},
		{"unknown hint dropped", "::sitrep task.start --future=x hello", true, false,
			Event{Kind: TaskStart, Title: "hello"}},
		{"metric with target", "::sitrep metric.update --target=350 --template=gauge aapl 343 AAPL", true, false,
			Event{Kind: MetricUpdate, Key: "aapl", Value: "343", Label: "AAPL", Target: "350", Template: "gauge"}},
		{"message bad level", "::sitrep message.send --level=fatal boom", true, true, Event{}},
		{"message missing text", "::sitrep message.send", true, true, Event{}},
		{"unknown verb", "::sitrep task.pause", true, true, Event{}},
		{"json form tolerated", `::sitrep task.done {"message":"ok"}`, true, false,
			Event{Kind: TaskDone}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := ParseLine(tc.line)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil || !ok {
				return
			}
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}
