//go:build linux

// This file exercises taskHandle.monitor()/execMainResult() against a real
// systemd/D-Bus connection. It requires root (StartTransientUnit is not
// permitted for unprivileged users by default) and a Linux host running
// systemd, so it only runs there:
//
//	sudo env PATH=$PATH go test -run TestExecMainResult -v ./...
package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	systemd "github.com/coreos/go-systemd/v22/dbus"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"
)

func newTestConn(t *testing.T) *systemd.Conn {
	t.Helper()
	if os.Getuid() != 0 {
		t.Skip("requires root to manage transient system units; re-run with sudo")
	}
	conn, err := systemd.NewSystemConnectionContext(context.Background())
	if err != nil {
		t.Fatalf("failed to connect to system dbus: %v", err)
	}
	t.Cleanup(conn.Close)
	return conn
}

// startUnit starts cmd as a transient unit using the same ExecStart
// properties StartTask sets, and returns a taskHandle wired up to monitor
// it. It does not wait for the unit to finish; callers that need to
// interact with the running process (e.g. to kill it) should do so before
// calling monitor().
func startUnit(t *testing.T, conn *systemd.Conn, cmd []string) *taskHandle {
	t.Helper()
	name := fmt.Sprintf("jdc-test-%d.service", time.Now().UnixNano())

	// See the comment on PropExecStart in driver.go: false is what makes a
	// non-zero exit mark the unit "failed" so monitor() can observe it.
	props := []systemd.Property{
		systemd.PropExecStart(cmd, false),
	}

	startCh := make(chan string, 1)
	if _, err := conn.StartTransientUnitContext(context.Background(), name, "replace", props, startCh); err != nil {
		t.Fatalf("failed to start unit %s: %v", name, err)
	}
	if res := <-startCh; res != "done" {
		t.Fatalf("unit %s start job result: %s", name, res)
	}

	t.Cleanup(func() {
		stopCh := make(chan string, 1)
		if _, err := conn.StopUnitContext(context.Background(), name, "replace", stopCh); err == nil {
			<-stopCh
		}
	})

	return &taskHandle{
		conn:               conn,
		unitName:           name,
		taskConfig:         &drivers.TaskConfig{ID: name, Name: name},
		logger:             hclog.New(&hclog.LoggerOptions{Name: "test", Level: hclog.Trace}),
		collectionInterval: 100 * time.Millisecond,
	}
}

// runUnitAndWait starts cmd as a transient unit and blocks until monitor()
// observes it exit.
func runUnitAndWait(t *testing.T, conn *systemd.Conn, cmd []string) *taskHandle {
	t.Helper()
	h := startUnit(t, conn, cmd)
	h.monitor()
	return h
}

func TestExecMainResult_CleanExit(t *testing.T) {
	conn := newTestConn(t)
	h := runUnitAndWait(t, conn, []string{"/bin/true"})

	status := h.TaskStatus()
	if status.State != drivers.TaskStateExited {
		t.Fatalf("expected TaskStateExited, got %v", status.State)
	}
	if status.ExitResult.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", status.ExitResult.ExitCode)
	}
	if status.ExitResult.Signal != 0 {
		t.Errorf("expected signal 0, got %d", status.ExitResult.Signal)
	}
}

func TestExecMainResult_NonZeroExit(t *testing.T) {
	conn := newTestConn(t)
	h := runUnitAndWait(t, conn, []string{"/bin/sh", "-c", "exit 42"})

	status := h.TaskStatus()
	if status.ExitResult.ExitCode != 42 {
		t.Errorf("expected exit code 42, got %d", status.ExitResult.ExitCode)
	}
	if status.ExitResult.Signal != 0 {
		t.Errorf("expected signal 0, got %d", status.ExitResult.Signal)
	}
}

func TestExecMainResult_Signal(t *testing.T) {
	conn := newTestConn(t)
	h := startUnit(t, conn, []string{"/bin/sleep", "30"})

	// Give the unit a moment to reach "running" before killing it, then let
	// monitor() observe the result via D-Bus, same as it would in a real
	// StopTask/kill scenario.
	time.Sleep(100 * time.Millisecond)
	if err := conn.KillUnitWithTarget(context.Background(), h.unitName, systemd.All, 9); err != nil {
		t.Fatalf("failed to kill unit %s: %v", h.unitName, err)
	}
	h.monitor()

	status := h.TaskStatus()
	if status.ExitResult.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", status.ExitResult.ExitCode)
	}
	if status.ExitResult.Signal != 9 {
		t.Errorf("expected signal 9 (SIGKILL), got %d", status.ExitResult.Signal)
	}
}
