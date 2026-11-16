# conn3x-exec

Drive [Conn3x](https://conn3x.org) from Go. This package runs the `conn3x` command, reads its `-json` stream as it comes, and hands you each event, with the circuit, step and host it's about already worked out.

It's for the web UI, CI tools, bots and anything else that wants Conn3x's results without scraping the terminal.

```go
c := &conn3xexec.Conn3x{Dir: "./circuits"}
err := c.Run(ctx, func(ev conn3xexec.Event) error {
	if ev.Hook != nil && ev.Hook.Resource.Step != nil && ev.Type == "apply_complete" {
		s := ev.Hook.Resource.Step
		fmt.Printf("%s: %s.%s done in %.0fs\n", s.Host, s.Circuit, s.Step, ev.Hook.Elapsed)
	}
	return nil
}, "apply", "-auto-approve")
```

- **`Run`** adds `-json` after the command, so pass `plan`, `apply`, `destroy` or `refresh` and their flags as you'd type them.
- **`RunInto`** writes the stream to a file with `-json-into` instead and follows it as conn3x writes, for flags that need stdout free of `-json`, such as `apply -report`.
- **Cancel the context** and Conn3x gets an interrupt, as Ctrl-C sends, so it stops cleanly and releases its state lock. It's killed only if it hasn't stopped after `Grace` (30 seconds by default). On Windows it's killed straight away.
- **A non-zero exit** comes back as an `*ExitError` with its code and the error diagnostics from the stream. `plan -detailed-exitcode` exiting 2 for "there are changes" is `ExitError{Code: 2}`.
- **`Read`** reads a stream you already have, such as a file from `-json-into`.

The events and the `conn3x` step object are described in [JSON output](https://docs.conn3x.org/reference/cli/json-output/). The step object needs Conn3x v0.21.0 or later. Older releases still stream every event, just without `Step`.

## Status

Experimental, like the stream's step object, until Conn3x v1. Expect the API to change before then.

## License

[MPL-2.0](LICENSE), like Conn3x.
