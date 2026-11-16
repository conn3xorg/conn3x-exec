// Copyright (c) The Conn3x Authors
// SPDX-License-Identifier: MPL-2.0

package conn3xexec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"
)

// Conn3x runs one conn3x binary in one working directory.
type Conn3x struct {
	// Path is the conn3x binary. Empty means "conn3x" on PATH.
	Path string
	// Dir is the working directory, the one with the circuits.
	Dir string
	// Env is added to this process's environment.
	Env []string
	// Stderr gets conn3x's standard error. Nil discards it.
	Stderr io.Writer
	// Grace is how long conn3x gets to stop after an interrupt when ctx is
	// done, releasing its state lock, before it's killed. Zero means 30s.
	Grace time.Duration
}

// ExitError is conn3x exiting with a status other than 0. Diagnostics are
// the errors the stream carried. plan -detailed-exitcode exits 2 when
// there are changes, which is this error with Code 2.
type ExitError struct {
	Code        int
	Diagnostics []Diagnostic
}

func (e *ExitError) Error() string {
	if len(e.Diagnostics) > 0 {
		return fmt.Sprintf("conn3x exited %d: %s", e.Code, e.Diagnostics[0].Summary)
	}
	return fmt.Sprintf("conn3x exited %d", e.Code)
}

// Run runs conn3x COMMAND -json ARGS... and calls fn with each event as it
// comes. COMMAND is one that takes -json: plan, apply, destroy or refresh.
// When ctx is done, conn3x is interrupted, as Ctrl-C would, and killed
// after Grace. If fn returns an error, conn3x is stopped the same way and
// Run returns that error.
func (c *Conn3x) Run(ctx context.Context, fn func(Event) error, command string, args ...string) error {
	return c.run(ctx, fn, "", command, args)
}

// RunInto is Run with the stream written to the file stream, through
// -json-into, and followed as conn3x writes it. It's for flags that need
// stdout free of -json, such as apply -report. conn3x's human output on
// stdout is discarded.
func (c *Conn3x) RunInto(ctx context.Context, fn func(Event) error, stream, command string, args ...string) error {
	return c.run(ctx, fn, stream, command, args)
}

func (c *Conn3x) run(ctx context.Context, fn func(Event) error, stream, command string, args []string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	path := c.Path
	if path == "" {
		path = "conn3x"
	}
	flag := "-json"
	if stream != "" {
		flag = "-json-into=" + stream
	}
	cmd := exec.CommandContext(ctx, path, append([]string{command, flag}, args...)...)
	cmd.Dir = c.Dir
	if len(c.Env) > 0 {
		cmd.Env = append(os.Environ(), c.Env...)
	}
	cmd.Stderr = c.Stderr
	cmd.Cancel = func() error {
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			return cmd.Process.Kill() // Windows can't interrupt
		}
		return nil
	}
	cmd.WaitDelay = c.Grace
	if cmd.WaitDelay == 0 {
		cmd.WaitDelay = 30 * time.Second
	}
	var diags []Diagnostic
	var fnErr error
	read := func(r io.Reader) error {
		return Read(r, func(ev Event) error {
			if ev.Diagnostic != nil && ev.Diagnostic.Severity == "error" {
				diags = append(diags, *ev.Diagnostic)
			}
			if fnErr == nil {
				if fnErr = fn(ev); fnErr != nil {
					cancel() // keep reading, so conn3x isn't blocked writing
				}
			}
			return nil
		})
	}
	var readErr, waitErr error
	if stream == "" {
		out, err := cmd.StdoutPipe()
		if err != nil {
			return err
		}
		if err := cmd.Start(); err != nil {
			return err
		}
		readErr = read(out)
		waitErr = cmd.Wait()
	} else {
		f, err := os.Create(stream)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := cmd.Start(); err != nil {
			return err
		}
		exited := make(chan struct{})
		go func() { waitErr = cmd.Wait(); close(exited) }()
		readErr = read(follow{f, exited})
		<-exited
	}
	if fnErr != nil {
		return fnErr
	}
	var exit *exec.ExitError
	if errors.As(waitErr, &exit) && ctx.Err() == nil {
		return &ExitError{Code: exit.ExitCode(), Diagnostics: diags}
	}
	if waitErr != nil {
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), waitErr)
		}
		return waitErr
	}
	return readErr
}

// follow reads a file another process is still writing, waiting at its
// end until done is closed.
type follow struct {
	f    *os.File
	done <-chan struct{}
}

func (fl follow) Read(b []byte) (int, error) {
	for {
		n, err := fl.f.Read(b)
		if n > 0 || err != io.EOF {
			return n, err
		}
		select {
		case <-fl.done:
			return fl.f.Read(b) // whatever came before it exited
		case <-time.After(100 * time.Millisecond):
		}
	}
}
