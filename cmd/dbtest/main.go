// dbtest runs only suite-owned local databases, never an external endpoint.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type event struct{ Action, Package, Test, Output string }
type report struct {
	FuzzExecuted                    bool
	Policy                          string
	Workers                         int
	Duration                        string
	Subcases                        int
	Profile                         string
	Seed                            int64
	Started                         time.Time
	Elapsed                         string
	Revision, WorkingTree, Platform string
	Passed, Failed, Skipped         int
	Complete                        bool
	Error                           string
	Coverage                        []string
}

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	profile := flag.String("profile", "quick", "quick, extended, soak, replay")
	seed := flag.Int64("seed", 1, "workload/fault seed")
	duration := flag.Duration("duration", 0, "soak duration")
	artifacts := flag.String("artifacts", "tmp/dbtest", "artifact parent")
	replay := flag.String("case", "", "failed case directory")
	workers := flag.Int("workers", 4, "Go test concurrency limit")
	policy := flag.String("policy", "strict", "soak sync policy")
	flag.Parse()
	if *workers < 1 || *duration < 0 {
		return fmt.Errorf("workers and duration must be positive")
	}
	if *profile != "quick" && *profile != "extended" && *profile != "soak" && *profile != "replay" {
		return fmt.Errorf("unknown profile")
	}
	if *policy != "strict" && *policy != "journal" && *policy != "group" {
		return fmt.Errorf("unknown policy")
	}
	if *duration == 0 {
		*duration = 2 * time.Second
		if *profile == "extended" {
			*duration = 55 * time.Minute
		}
		if *profile == "soak" {
			*duration = 24 * time.Hour
		}
	}
	root, e := filepath.Abs(*artifacts)
	if e != nil {
		return e
	}
	if e = os.MkdirAll(root, 0700); e != nil {
		return e
	}
	dir, e := os.MkdirTemp(root, "run-")
	if e != nil {
		return e
	}
	r := report{Policy: *policy, Workers: *workers, Duration: duration.String(), Profile: *profile, Seed: *seed, Started: time.Now(), Platform: runtime.GOOS + "/" + runtime.GOARCH, Coverage: []string{"engine regressions", "modeled persistence (not physical power loss)", "bounded serial histories", "server SIGKILL", "model workload and backup restore"}}
	git := func(args ...string) string {
		b, _ := exec.Command("git", args...).Output()
		return strings.TrimSpace(string(b))
	}
	r.Revision = git("rev-parse", "HEAD")
	r.WorkingTree = git("status", "--porcelain")
	executionProfile := *profile
	timeout := 10 * time.Minute
	pattern := "^Test(DBSuite(DiskModel|Persistence|Isolation|IOFaults|RecoveryInterrupted|BackupConcurrent|Publication|RestorePublication|DirectoryDurability|Churn|Workload|ServerCrash)$|ACID|Freelist|Snapshot|WrongJournal|MetadataRecovery|CleanV03|StaleSame|TornWAL|MirrorIdentity|ExplicitLegacy|JournalSyncFault|WALRetirement|AbortedSave|DatabaseAdmin|DatabaseTransfer)"
	if *profile == "extended" {
		timeout = time.Hour
	}
	if *profile == "soak" {
		timeout = *duration + 5*time.Minute
		pattern = "^TestDBSuite(Churn|ServerCrash)$"
	}
	if *profile == "replay" {
		if *replay == "" {
			return fmt.Errorf("--case is required")
		}
		pattern = "^TestDBSuitePersistence$"
		if raw, err := os.ReadFile(filepath.Join(*replay, "scenario.json")); err == nil {
			var metadata struct {
				Test                               string
				Seed                               int64
				Profile, Duration, Policy, Workers string
			}
			if err = json.Unmarshal(raw, &metadata); err != nil {
				return err
			}
			if !strings.HasPrefix(metadata.Test, "TestDBSuite") {
				return fmt.Errorf("not a suite case")
			}
			pattern = "^" + regexp.QuoteMeta(metadata.Test) + "$"
			*seed = metadata.Seed
			if metadata.Profile != "" {
				executionProfile = metadata.Profile
			}
			if metadata.Duration != "" {
				parsed, err := time.ParseDuration(metadata.Duration)
				if err != nil || parsed <= 0 {
					return fmt.Errorf("invalid case duration")
				}
				*duration = parsed
			}
			if metadata.Policy != "" {
				if metadata.Policy != "strict" && metadata.Policy != "journal" && metadata.Policy != "group" {
					return fmt.Errorf("invalid case policy")
				}
				*policy = metadata.Policy
			}
			if metadata.Workers != "" {
				parsed, err := strconv.Atoi(metadata.Workers)
				if err != nil || parsed < 1 {
					return fmt.Errorf("invalid case workers")
				}
				*workers = parsed
			}
			if executionProfile == "extended" {
				timeout = time.Hour
			}
			if executionProfile == "soak" {
				timeout = *duration + 5*time.Minute
			}
		}
	}
	r.Seed = *seed
	r.Policy = *policy
	r.Workers = *workers
	r.Duration = duration.String()
	args := []string{"test", "-json", "-count=1", "-parallel", strconv.Itoa(*workers), "-timeout", timeout.String(), "./storage", "./orm/...", "./cmd/pirindb", "-run", pattern}
	cmd := exec.Command("go", args...)
	cmd.Env = append(os.Environ(), "PIRIN_DBTEST=1", "PIRIN_DBTEST_PROFILE="+executionProfile, "PIRIN_DBTEST_SEED="+fmt.Sprint(*seed), "PIRIN_DBTEST_DURATION="+duration.String(), "PIRIN_DBTEST_ARTIFACTS="+dir, "PIRIN_DBTEST_REPLAY="+*replay, "PIRIN_DBTEST_WORKERS="+fmt.Sprint(*workers), "PIRIN_DBTEST_POLICY="+*policy)
	log, e := os.Create(filepath.Join(dir, "events.jsonl"))
	if e != nil {
		return e
	}
	defer log.Close()
	cmd.Stderr = log
	pipe, e := cmd.StdoutPipe()
	if e != nil {
		return e
	}
	if e = cmd.Start(); e != nil {
		return e
	}
	fmt.Printf("Database suite %s seed=%d; artifacts %s\n", *profile, *seed, dir)
	scanner := bufio.NewScanner(pipe)
	scanner.Buffer(make([]byte, 65536), 4*1024*1024)
	seen := map[string]bool{}
	for scanner.Scan() {
		line := scanner.Bytes()
		if _, err := log.Write(append(append([]byte(nil), line...), '\n')); err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			return fmt.Errorf("write test evidence: %w", err)
		}
		var v event
		if json.Unmarshal(line, &v) != nil {
			continue
		}
		if v.Action == "pass" && strings.Contains(v.Test, "/") {
			r.Subcases++
		}
		if v.Test != "" && !strings.Contains(v.Test, "/") {
			switch v.Action {
			case "pass":
				r.Passed++
				seen[v.Test] = true
				fmt.Println("PASS", v.Test)
			case "fail":
				r.Failed++
				fmt.Println("FAIL", v.Test)
			case "skip":
				r.Skipped++
			}
		}
	}
	e = cmd.Wait()
	if scanner.Err() != nil {
		e = scanner.Err()
	}
	required := []string{"TestDBSuiteDiskModel", "TestDBSuitePersistence", "TestDBSuiteIsolation", "TestDBSuiteIOFaults", "TestDBSuiteRecoveryInterrupted", "TestDBSuiteBackupConcurrent", "TestDBSuitePublication", "TestDBSuiteRestorePublication", "TestDBSuiteDirectoryDurability", "TestDBSuiteChurn", "TestDBSuiteWorkload", "TestDBSuiteServerCrash"}
	if *profile == "soak" {
		required = []string{"TestDBSuiteChurn", "TestDBSuiteServerCrash"}
	}
	if *profile == "replay" {
		required = []string{strings.Split(strings.TrimSuffix(strings.TrimPrefix(pattern, "^"), "$"), "/")[0]}
	}
	for _, name := range required {
		if !seen[name] {
			e = fmt.Errorf("required scenario did not pass: %s", name)
		}
	}
	if e == nil && *profile == "extended" {
		output, err := os.Create(filepath.Join(dir, "fuzz.log"))
		if err != nil {
			return err
		}
		fuzz := exec.Command("go", "test", "./storage", "-run", "^$", "-fuzz", "^FuzzDBNode$", "-fuzztime", "10s", "-parallel", fmt.Sprint(*workers), "-timeout", "1m")
		fuzz.Stdout = output
		fuzz.Stderr = output
		fuzzErr := fuzz.Run()
		closeErr := output.Close()
		r.FuzzExecuted = true
		if fuzzErr != nil {
			e = fmt.Errorf("decoder fuzz campaign: %w", fuzzErr)
		} else if closeErr != nil {
			e = closeErr
		}
	}
	r.Complete = e == nil
	r.Elapsed = time.Since(r.Started).String()
	if e != nil {
		r.Error = e.Error()
	}
	data, _ := json.MarshalIndent(r, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "report.json"), data, 0600); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	if err := log.Sync(); err != nil {
		return fmt.Errorf("sync evidence: %w", err)
	}
	fmt.Printf("passed=%d failed=%d skipped=%d complete=%v elapsed=%s\n", r.Passed, r.Failed, r.Skipped, r.Complete, r.Elapsed)
	return e
}
