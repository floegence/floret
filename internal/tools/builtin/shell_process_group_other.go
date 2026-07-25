//go:build !(aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris)

package builtin

import (
	"os/exec"
	"time"
)

func configureCommandCancellation(cmd *exec.Cmd) {
	cmd.WaitDelay = 100 * time.Millisecond
}
