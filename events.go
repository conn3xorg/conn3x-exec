// Copyright (c) The Conn3x Authors
// SPDX-License-Identifier: MPL-2.0

// Package conn3xexec runs the conn3x command and reads the events of its
// -json stream, so a program can drive Conn3x without parsing its terminal
// output. The stream is described at
// https://docs.conn3x.org/reference/cli/json-output/.
package conn3xexec

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// Event is one line of the stream. Type says which of the other fields are
// set: Hook for apply_*, Change for planned_change and resource_drift,
// Changes for change_summary, Diagnostic for diagnostic, and Conn3x and UI
// for version. Raw is the whole line, for anything else.
type Event struct {
	Type       string         `json:"type"`
	Level      string         `json:"@level"`
	Message    string         `json:"@message"`
	Timestamp  time.Time      `json:"@timestamp"`
	Hook       *Hook          `json:"hook,omitempty"`
	Change     *Change        `json:"change,omitempty"`
	Changes    *ChangeSummary `json:"changes,omitempty"`
	Diagnostic *Diagnostic    `json:"diagnostic,omitempty"`
	Conn3x     string         `json:"conn3x,omitempty"`
	UI         string         `json:"ui,omitempty"`

	Raw json.RawMessage `json:"-"`
}

// Resource is the resource instance an event is about.
type Resource struct {
	Addr         string `json:"addr"`
	Module       string `json:"module"`
	ResourceType string `json:"resource_type"`
	ResourceName string `json:"resource_name"`
	// Step is set when the instance is a circuit's step or handler.
	Step *Step `json:"conn3x,omitempty"`
}

// Step names the circuit step or handler, and the host, an instance is for.
// Item is set only for a step with for_each.
type Step struct {
	Circuit string `json:"circuit"`
	Step    string `json:"step"`
	Host    string `json:"host"`
	Item    string `json:"item,omitempty"`
}

// Hook is a step starting, running, finishing or failing on a host.
type Hook struct {
	Resource Resource `json:"resource"`
	Action   string   `json:"action"`
	Elapsed  float64  `json:"elapsed_seconds"`
	IDKey    string   `json:"id_key,omitempty"`
	IDValue  string   `json:"id_value,omitempty"`
}

// Change is a planned change, or drift found while planning.
type Change struct {
	Resource Resource `json:"resource"`
	Action   string   `json:"action"`
	Reason   string   `json:"reason,omitempty"`
}

// ChangeSummary is a plan's or an apply's counts.
type ChangeSummary struct {
	Add       int    `json:"add"`
	Change    int    `json:"change"`
	Import    int    `json:"import"`
	Remove    int    `json:"remove"`
	Forget    int    `json:"forget"`
	Operation string `json:"operation"`
}

// Diagnostic is a warning or an error.
type Diagnostic struct {
	Severity string `json:"severity"`
	Summary  string `json:"summary"`
	Detail   string `json:"detail"`
	Address  string `json:"address,omitempty"`
	Range    *struct {
		Filename string `json:"filename"`
		Start    struct {
			Line   int `json:"line"`
			Column int `json:"column"`
		} `json:"start"`
	} `json:"range,omitempty"`
}

// MaxLine is the longest line Read decodes. A longer one, such as a huge
// output value, is read past and counted in a LongLines error.
var MaxLine = 16 << 20

// LongLines is Read's error for a stream with lines over MaxLine.
type LongLines int

func (n LongLines) Error() string {
	return fmt.Sprintf("%d line(s) over %d bytes weren't read", int(n), MaxLine)
}

// Read calls fn with each event in r, to its end, as written by -json or
// -json-into. A line that isn't JSON, such as a panic's output, is passed
// to fn as an event of Type "" with Raw set. fn returning an error stops
// Read, which returns it.
func Read(r io.Reader, fn func(Event) error) error {
	br := bufio.NewReaderSize(r, 64<<10)
	long := 0
	for {
		line, tooLong, err := readLine(br)
		if tooLong {
			long++
		} else if len(line) > 0 {
			var ev Event
			if json.Unmarshal(line, &ev) != nil {
				ev = Event{}
			}
			ev.Raw = append(json.RawMessage(nil), line...)
			if ferr := fn(ev); ferr != nil {
				return ferr
			}
		}
		if err == io.EOF {
			if long > 0 {
				return LongLines(long)
			}
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// readLine reads one line without its newline. A line over MaxLine is
// drained, not kept, so a writer is never left blocked.
func readLine(br *bufio.Reader) (line []byte, tooLong bool, err error) {
	for {
		part, err := br.ReadSlice('\n')
		if !tooLong && len(line)+len(part) > MaxLine+1 {
			tooLong, line = true, nil
		}
		if !tooLong {
			line = append(line, part...)
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if n := len(line); n > 0 && line[n-1] == '\n' {
			line = line[:n-1]
		}
		return line, tooLong, err
	}
}
