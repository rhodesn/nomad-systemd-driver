package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	systemd "github.com/coreos/go-systemd/v22/dbus"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"
)

// waitid si_code values (see wait(2)), as reported by systemd's
// ExecMainCode property. CLD_EXITED means ExecMainStatus is an exit code;
// CLD_KILLED/CLD_DUMPED mean it's a signal number.
const (
	cldExited = 1
	cldKilled = 2
	cldDumped = 3
)

// taskHandle should store all relevant runtime information
// such as process ID if this is a local task or other meta
// data if this driver deals with external APIs
type taskHandle struct {
	// conn
	conn *systemd.Conn

	// unitName
	unitName string

	// collectionInterval
	collectionInterval time.Duration

	// stateLock syncs access to all fields below
	stateLock sync.RWMutex

	logger hclog.Logger

	taskConfig  *drivers.TaskConfig
	procState   drivers.TaskState
	startedAt   time.Time
	completedAt time.Time
	exitResult  *drivers.ExitResult
}

func (h *taskHandle) TaskStatus() *drivers.TaskStatus {
	h.stateLock.RLock()
	defer h.stateLock.RUnlock()

	return &drivers.TaskStatus{
		ID:               h.taskConfig.ID,
		Name:             h.taskConfig.Name,
		State:            h.procState,
		StartedAt:        h.startedAt,
		CompletedAt:      h.completedAt,
		ExitResult:       h.exitResult,
		DriverAttributes: map[string]string{},
	}
}

func (h *taskHandle) IsRunning() bool {
	h.stateLock.RLock()
	defer h.stateLock.RUnlock()
	return h.procState == drivers.TaskStateRunning
}

func (h *taskHandle) monitor() {
	h.stateLock.Lock()
	if h.exitResult == nil {
		h.exitResult = &drivers.ExitResult{}
	}
	h.stateLock.Unlock()

	h.logger.Debug("monitoring unit", "name", h.unitName)
	var state drivers.TaskState
	for {
		timerChan := time.After(h.collectionInterval)
		// We can't use SubscribeUnitsCustom because it only returns either
		// active or enabled units, not inactive ones.
		units, err := h.conn.ListUnitsByNamesContext(context.TODO(), []string{h.unitName})
		if err != nil {
			h.logger.Error("encountered error monitoring unit", err)
			state = drivers.TaskStateUnknown
			break
		}

		// A non-zero exit or a fatal signal puts the unit in "failed", not
		// "inactive" - both mean the process is done and it's safe to read
		// its exit status.
		if units[0].ActiveState == "inactive" || units[0].ActiveState == "failed" {
			h.logger.Debug("unit stopped", "name", h.unitName, "active_state", units[0].ActiveState)
			state = drivers.TaskStateExited
			break
		}

		<-timerChan
	}

	var exitCode, signal int
	if state == drivers.TaskStateExited {
		var err error
		exitCode, signal, err = h.execMainResult()
		if err != nil {
			h.logger.Warn("failed to determine exit status", "name", h.unitName, "error", err)
		}
	}

	h.stateLock.Lock()
	defer h.stateLock.Unlock()
	h.procState = state
	h.completedAt = time.Now().Round(time.Millisecond)
	if h.exitResult == nil {
		h.exitResult = &drivers.ExitResult{}
	}
	h.exitResult.ExitCode = exitCode
	h.exitResult.Signal = signal
}

// execMainResult reads the unit's ExecMainCode/ExecMainStatus properties and
// translates them into a process exit code and/or signal, mirroring how
// "systemctl status" reports a service's outcome.
func (h *taskHandle) execMainResult() (exitCode, signal int, err error) {
	codeProp, err := h.conn.GetUnitTypePropertyContext(context.TODO(), h.unitName, "Service", "ExecMainCode")
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get ExecMainCode: %w", err)
	}
	statusProp, err := h.conn.GetUnitTypePropertyContext(context.TODO(), h.unitName, "Service", "ExecMainStatus")
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get ExecMainStatus: %w", err)
	}

	code, ok := codeProp.Value.Value().(int32)
	if !ok {
		return 0, 0, fmt.Errorf("unexpected type for ExecMainCode: %T", codeProp.Value.Value())
	}
	status, ok := statusProp.Value.Value().(int32)
	if !ok {
		return 0, 0, fmt.Errorf("unexpected type for ExecMainStatus: %T", statusProp.Value.Value())
	}

	switch code {
	case cldExited:
		return int(status), 0, nil
	case cldKilled, cldDumped:
		return 0, int(status), nil
	default:
		// Process never ran, or is still running/stopped/continued.
		return 0, 0, nil
	}
}
