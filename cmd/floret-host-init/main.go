package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/floegence/floret/v2/internal/hostscaffold"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("floret-host-init", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	profileValue := flags.String("profile", "", "scaffold profile")
	packageName := flags.String("package", "main", "generated Go package")
	directory := flags.String("dir", ".", "output directory")
	write := flags.Bool("write", false, "create generated files")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("floret-host-init accepts flags only")
	}
	profile, err := hostscaffold.ParseProfile(*profileValue)
	if err != nil {
		return err
	}
	files, err := hostscaffold.Generate(hostscaffold.Config{Profile: profile, Package: strings.TrimSpace(*packageName)})
	if err != nil {
		return err
	}
	root, err := filepath.Abs(*directory)
	if err != nil {
		return err
	}
	targets := make([]generatedTarget, 0, len(files))
	for _, file := range files {
		target := filepath.Join(root, file.Name)
		existing, readErr := os.ReadFile(target)
		switch {
		case readErr == nil && bytes.Equal(existing, file.Content):
			targets = append(targets, generatedTarget{path: target, content: file.Content, unchanged: true})
		case readErr == nil:
			return fmt.Errorf("%w: refusing to overwrite %s", hostscaffold.ErrConflict, target)
		case !errors.Is(readErr, os.ErrNotExist):
			return readErr
		default:
			targets = append(targets, generatedTarget{path: target, content: file.Content})
		}
	}
	for _, target := range targets {
		if target.unchanged {
			fmt.Fprintf(stdout, "unchanged %s\n", target.path)
			continue
		}
		if !*write {
			printNewFileDiff(stdout, target.path, target.content)
		}
	}
	if !*write {
		return nil
	}
	if err := writeNewFilesAtomically(root, targets); err != nil {
		return err
	}
	for _, target := range targets {
		if !target.unchanged {
			fmt.Fprintf(stdout, "created %s\n", target.path)
		}
	}
	return nil
}

type generatedTarget struct {
	path      string
	content   []byte
	unchanged bool
}

type stagedTarget struct {
	path          string
	temporaryPath string
}

func writeNewFilesAtomically(root string, targets []generatedTarget) (returnErr error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	staged := make([]stagedTarget, 0, len(targets))
	defer func() {
		for _, target := range staged {
			if removeErr := os.Remove(target.temporaryPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) && returnErr == nil {
				returnErr = removeErr
			}
		}
	}()
	for _, target := range targets {
		if target.unchanged {
			continue
		}
		if _, err := os.Lstat(target.path); err == nil {
			return fmt.Errorf("%w: refusing to overwrite %s", hostscaffold.ErrConflict, target.path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		temporary, err := os.CreateTemp(root, ".floret-host-init-*")
		if err != nil {
			return err
		}
		temporaryPath := temporary.Name()
		if err := temporary.Chmod(0o644); err != nil {
			_ = temporary.Close()
			_ = os.Remove(temporaryPath)
			return err
		}
		if _, err := temporary.Write(target.content); err != nil {
			_ = temporary.Close()
			_ = os.Remove(temporaryPath)
			return err
		}
		if err := temporary.Sync(); err != nil {
			_ = temporary.Close()
			_ = os.Remove(temporaryPath)
			return err
		}
		if err := temporary.Close(); err != nil {
			_ = os.Remove(temporaryPath)
			return err
		}
		staged = append(staged, stagedTarget{path: target.path, temporaryPath: temporaryPath})
	}
	committed := make([]string, 0, len(staged))
	for _, target := range staged {
		if _, err := os.Lstat(target.path); err == nil {
			returnErr = fmt.Errorf("%w: refusing to overwrite %s", hostscaffold.ErrConflict, target.path)
		} else if !errors.Is(err, os.ErrNotExist) {
			returnErr = err
		} else if err := os.Rename(target.temporaryPath, target.path); err != nil {
			returnErr = err
		} else {
			committed = append(committed, target.path)
			continue
		}
		for _, path := range committed {
			if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				return errors.Join(returnErr, fmt.Errorf("roll back generated file %s: %w", path, removeErr))
			}
		}
		return returnErr
	}
	return nil
}

func printNewFileDiff(stdout io.Writer, path string, content []byte) {
	lines := bytes.Count(content, []byte("\n"))
	fmt.Fprintln(stdout, "--- /dev/null")
	fmt.Fprintf(stdout, "+++ %s\n", path)
	fmt.Fprintf(stdout, "@@ -0,0 +1,%d @@\n", lines)
	for _, line := range strings.Split(strings.TrimSuffix(string(content), "\n"), "\n") {
		fmt.Fprintf(stdout, "+%s\n", line)
	}
}
