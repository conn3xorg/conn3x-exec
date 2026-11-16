// Copyright (c) The Conn3x Authors
// SPDX-License-Identifier: MPL-2.0

package conn3xexec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"testing"
	"time"
)

const applyLine = `{"@level":"info","@message":"conn3x_command.box__build[\"web1/a\"]: Creation complete after 1s","@timestamp":"2026-11-15T15:33:02.1-07:00","hook":{"resource":{"addr":"conn3x_command.box__build[\"web1/a\"]","module":"","resource_type":"conn3x_command","resource_name":"box__build","resource_key":"web1/a","conn3x":{"circuit":"box","step":"build","host":"web1","item":"a"}},"action":"create","elapsed_seconds":1.5},"type":"apply_complete"}`

// TestMain lets the test binary stand in for conn3x: CONN3X_FAKE picks
// what it does.
func TestMain(m *testing.M) {
	switch os.Getenv("CONN3X_FAKE") {
	case "":
		os.Exit(m.Run())
	case "apply":
		fmt.Println(`{"@level":"info","@message":"Conn3x v0.21.0","conn3x":"0.21.0","tofu":"1.13.9","type":"version","ui":"1.2"}`)
		fmt.Println(strings.Join(os.Args[1:], " "))
		fmt.Println(applyLine)
	case "into":
		// Writes the stream to -json-into a line at a time, with a pause, as
		// a real apply would, and prints human output on stdout.
		path := strings.TrimPrefix(os.Args[2], "-json-into=")
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			os.Exit(3)
		}
		fmt.Println("Apply complete!")
		fmt.Fprintln(f, `{"type":"apply_start"}`)
		time.Sleep(300 * time.Millisecond)
		fmt.Fprintln(f, applyLine)
		if os.Getenv("CONN3X_FAKE_WAIT") != "" {
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, os.Interrupt)
			<-ch
			os.Exit(1)
		}
	case "fail":
		fmt.Println(`{"@level":"error","@message":"Error: no host","diagnostic":{"severity":"error","summary":"no host","detail":""},"type":"diagnostic"}`)
		os.Exit(2)
	case "wait":
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt)
		fmt.Println(`{"type":"apply_start"}`)
		<-ch
		os.Exit(1)
	}
}

func fake(t *testing.T, mode string) *Conn3x {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return &Conn3x{Path: exe, Env: []string{"CONN3X_FAKE=" + mode}, Grace: 5 * time.Second}
}

func TestRunEvents(t *testing.T) {
	var evs []Event
	err := fake(t, "apply").Run(context.Background(), func(ev Event) error { evs = append(evs, ev); return nil }, "apply", "-auto-approve")
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 || evs[0].Type != "version" || evs[0].Conn3x != "0.21.0" || evs[0].UI != "1.2" {
		t.Fatalf("events %+v", evs)
	}
	if got := string(evs[1].Raw); evs[1].Type != "" || got != "apply -json -auto-approve" {
		t.Errorf("a non-JSON line came as %q (type %q)", got, evs[1].Type)
	}
	h := evs[2].Hook
	if h == nil || h.Action != "create" || h.Elapsed != 1.5 || h.Resource.Step == nil || *h.Resource.Step != (Step{"box", "build", "web1", "a"}) {
		t.Fatalf("hook %+v", h)
	}
	if evs[2].Timestamp.IsZero() {
		t.Error("no timestamp")
	}
}

func TestRunExitError(t *testing.T) {
	err := fake(t, "fail").Run(context.Background(), func(Event) error { return nil }, "plan", "-detailed-exitcode")
	var exit *ExitError
	if !errors.As(err, &exit) || exit.Code != 2 || len(exit.Diagnostics) != 1 || exit.Diagnostics[0].Summary != "no host" {
		t.Fatalf("got %v", err)
	}
}

func TestRunCancelInterrupts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	err := fake(t, "wait").Run(ctx, func(ev Event) error {
		if ev.Type == "apply_start" {
			cancel()
		}
		return nil
	}, "apply")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > 4*time.Second {
		t.Error("conn3x wasn't interrupted; it waited to be killed")
	}
}

func TestRunFnError(t *testing.T) {
	stop := errors.New("stop")
	err := fake(t, "wait").Run(context.Background(), func(Event) error { return stop }, "apply")
	if !errors.Is(err, stop) {
		t.Fatalf("got %v", err)
	}
}

// RunInto follows the -json-into file while conn3x writes it, and gets
// every line, the last one written just before it exits.
func TestRunInto(t *testing.T) {
	stream := t.TempDir() + "/stream.json"
	var types []string
	err := fake(t, "into").RunInto(context.Background(), func(ev Event) error { types = append(types, ev.Type); return nil }, stream, "apply", "-report=r.json")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(types) != "[apply_start apply_complete]" {
		t.Errorf("events %v", types)
	}
}

func TestRunIntoCancelInterrupts(t *testing.T) {
	c := fake(t, "into")
	c.Env = append(c.Env, "CONN3X_FAKE_WAIT=1")
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	err := c.RunInto(ctx, func(ev Event) error {
		if ev.Type == "apply_complete" {
			cancel()
		}
		return nil
	}, t.TempDir()+"/stream.json", "apply")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > 4*time.Second {
		t.Error("conn3x wasn't interrupted; it waited to be killed")
	}
}

func TestReadLongLines(t *testing.T) {
	defer func(n int) { MaxLine = n }(MaxLine)
	MaxLine = 100
	in := `{"type":"a"}` + "\n" + strings.Repeat("x", 300) + "\n" + `{"type":"b"}` + "\n" + strings.Repeat("y", 300)
	var types []string
	err := Read(strings.NewReader(in), func(ev Event) error { types = append(types, ev.Type); return nil })
	if fmt.Sprint(types) != "[a b]" {
		t.Errorf("read %v", types)
	}
	if n, ok := err.(LongLines); !ok || n != 2 {
		t.Errorf("got %v, want 2 long lines", err)
	}
}

func TestReadLineOverBuffer(t *testing.T) {
	big := `{"type":"outputs","@message":"` + strings.Repeat("z", 200<<10) + `"}`
	var got []Event
	if err := Read(strings.NewReader(big+"\n"+`{"type":"c"}`), func(ev Event) error { got = append(got, ev); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || len(got[0].Message) != 200<<10 || got[1].Type != "c" {
		t.Fatalf("read %d events", len(got))
	}
}

// With CONN3X_BIN set, plans a real circuit with no hosts to reach.
func TestRealConn3x(t *testing.T) {
	bin := os.Getenv("CONN3X_BIN")
	if bin == "" {
		t.Skip("CONN3X_BIN not set")
	}
	dir := t.TempDir()
	os.WriteFile(dir+"/main.tf", []byte(`output "x" { value = 1 }`), 0o644)
	c := &Conn3x{Path: bin, Dir: dir}
	if err := c.Run(context.Background(), func(Event) error { return nil }, "init"); err != nil {
		t.Fatal(err)
	}
	var sum *ChangeSummary
	var version string
	err := c.Run(context.Background(), func(ev Event) error {
		if ev.Type == "version" {
			version = ev.Conn3x
		}
		if ev.Changes != nil {
			sum = ev.Changes
		}
		return nil
	}, "plan")
	if err != nil || version == "" || sum == nil || sum.Operation != "plan" {
		t.Fatalf("err %v, version %q, summary %+v", err, version, sum)
	}
}
