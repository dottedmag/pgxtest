// Spawns a PostgreSQL server with a single database configured. Ideal for unit
// tests where you want a clean instance each time. Then clean up afterwards.
//
// Requires PostgreSQL to be installed on your system (but it doesn't have to be running).
package pgxtest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	pgxslog "github.com/mcosta74/pgx-slog"
)

type Config struct {
	BinDir         string   // Directory to look for postgresql binaries including initdb, postgres
	Dir            string   // Directory for storing database files, removed for non-persistent configs
	AdditionalArgs []string // Additional arguments to pass to the postgres command
}

type PG struct {
	dir  string
	cmd  *exec.Cmd
	Pool *pgxpool.Pool

	Host string
	User string
	Name string

	stderr io.ReadCloser
	stdout io.ReadCloser
}

func postgresqlDBConf(sockDir string, dbName string) (*pgxpool.Config, error) {
	url := "postgres://test@localhost/" + dbName + "?host=" + sockDir
	return pgxpool.ParseConfig(url)
}

func createTestDB(ctx context.Context, pool *pgxpool.Pool) error {
	var conn *pgxpool.Conn
	// Prepare test database
	err := retry(func() error {
		var err error
		conn, err = pool.Acquire(ctx)
		return err
	}, 1000, 10*time.Millisecond)
	if err != nil {
		return err
	}
	defer func() {
		conn.Release()
	}()

	if _, err := conn.Exec(ctx, "CREATE DATABASE test"); err != nil {
		return err
	}
	return nil
}

// Start a new PostgreSQL database, on temporary storage.
//
// This database has fsync disabled for performance, so it might run faster
// than your production database. This makes it less reliable in case of system
// crashes, but we don't care about that anyway during unit testing.
//
// Use the Pool field to access the database pool
func Start(ctx context.Context, config Config) (*PG, error) {
	// Find executables root path
	binPath, err := findBinPath(config.BinDir)
	if err != nil {
		return nil, err
	}

	// Prepare data directory
	dir := config.Dir
	if config.Dir == "" {
		d, err := os.MkdirTemp("", "pgxtest")
		if err != nil {
			return nil, err
		}
		dir = d
	}

	dataDir := filepath.Join(dir, "data")
	sockDir := filepath.Join(dir, "sock")

	err = os.MkdirAll(dataDir, 0711)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(sockDir, 0711)
	if err != nil {
		return nil, err
	}

	init := prepareCommand(filepath.Join(binPath, "initdb"),
		"-D", dataDir,
		"--no-sync",
		"--username=test",
	)
	out, err := init.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("Failed to initialize DB: %w -> %s", err, string(out))
	}

	// Start PostgreSQL
	args := []string{
		"-D", dataDir, // Data directory
		"-k", sockDir, // Location for the UNIX socket
		"-h", "", // Disable TCP listening
		"-F", // No fsync, just go fast
	}
	if len(config.AdditionalArgs) > 0 {
		args = append(args, config.AdditionalArgs...)
	}
	cmd := prepareCommand(filepath.Join(binPath, "postgres"),
		args...,
	)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		stderr.Close()
		return nil, err
	}

	err = cmd.Start()
	if err != nil {
		return nil, abort("Failed to start PostgreSQL", cmd, stderr, stdout, err)
	}

	// Connect to postgres DB
	postgresConf, err := postgresqlDBConf(sockDir, "postgres")
	if err != nil {
		return nil, abort("Failed to create pgx pool config", cmd, stderr, stdout, err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, postgresConf)
	if err != nil {
		return nil, abort("Failed to connect to postgres DB", cmd, stderr, stdout, err)
	}

	if err := createTestDB(ctx, pool); err != nil {
		return nil, abort("Failed to create test DB", cmd, stderr, stdout, err)
	}

	pool.Close()

	// Connect to it properly
	testConf, err := postgresqlDBConf(sockDir, "test")
	if err != nil {
		return nil, abort("Failed to create pgx pool config", cmd, stderr, stdout, err)
	}
	testConf.ConnConfig.Tracer = &tracelog.TraceLog{
		Logger: pgxslog.NewLogger(
			// TODO (misha): change to a proper test logger
			slog.Default(),
		),
		LogLevel: tracelog.LogLevelTrace,
	}
	pool, err = pgxpool.NewWithConfig(ctx, testConf)
	if err != nil {
		return nil, abort("Failed to connect to test DB", cmd, stderr, stdout, err)
	}

	pg := &PG{
		cmd: cmd,
		dir: dir,

		Pool: pool,

		Host: sockDir,
		User: "test",
		Name: "test",

		stderr: stderr,
		stdout: stdout,
	}

	return pg, nil
}

// Stop the database and remove storage files.
func (p *PG) Stop() error {
	if p == nil {
		return nil
	}

	p.Pool.Close()

	defer func() {
		// Always try to remove it
		os.RemoveAll(p.dir)
	}()

	err := p.cmd.Process.Signal(os.Interrupt)
	if err != nil {
		return err
	}

	// Doesn't matter if the server exists with an error
	err = p.cmd.Wait()
	if err != nil {
		_ = p.cmd.Process.Signal(os.Kill)

		// Remove UNIX sockets
		files, err := os.ReadDir(p.Host)
		if err == nil {
			for _, file := range files {
				_ = os.Remove(filepath.Join(p.Host, file.Name()))
			}
		}
	}

	if p.stderr != nil {
		p.stderr.Close()
	}

	if p.stdout != nil {
		p.stdout.Close()
	}

	return nil
}

// Needed because Ubuntu doesn't put initdb in $PATH
// binDir a path to a directory that contains postgresql binaries
func findBinPath(binDir string) (string, error) {
	// In $PATH (e.g. Fedora) great!
	if binDir == "" {
		p, err := exec.LookPath("initdb")
		if err == nil {
			return path.Dir(p), nil
		}
	}

	// Look for a PostgreSQL in one of the folders Ubuntu uses
	folders := []string{
		binDir,
		"/usr/lib/postgresql/",
	}
	for _, folder := range folders {
		f, err := os.Stat(folder)
		if os.IsNotExist(err) {
			continue
		}
		if !f.IsDir() {
			continue
		}

		files, err := os.ReadDir(folder)
		if err != nil {
			return "", err
		}
		for _, fi := range files {
			if !fi.IsDir() && "initdb" == fi.Name() {
				return filepath.Join(folder), nil
			}

			if !fi.IsDir() {
				continue
			}

			binPath := filepath.Join(folder, fi.Name(), "bin")
			_, err := os.Stat(filepath.Join(binPath, "initdb"))
			if err == nil {
				return binPath, nil
			}
		}
	}

	return "", fmt.Errorf("Did not find PostgreSQL executables installed")
}

func retry(fn func() error, attempts int, interval time.Duration) error {
	for {
		err := fn()
		if err == nil {
			return nil
		}

		attempts -= 1
		if attempts <= 0 {
			return err
		}

		time.Sleep(interval)
	}
}

func prepareCommand(command string, args ...string) *exec.Cmd {
	cmd := exec.Command(command, args...)

	cmd.Env = append(
		os.Environ(),
		"LC_ALL=en_US.UTF-8", // Fix for https://github.com/Homebrew/homebrew-core/issues/124215 in Mac OS X
	)

	return cmd
}

func abort(msg string, cmd *exec.Cmd, stderr, stdout io.ReadCloser, err error) error {
	_ = cmd.Process.Signal(os.Interrupt)
	_ = cmd.Wait()

	serr, _ := io.ReadAll(stderr)
	sout, _ := io.ReadAll(stdout)
	_ = stderr.Close()
	_ = stdout.Close()
	return fmt.Errorf("%s: %s\nOUT: %s\nERR: %s", msg, err, string(sout), string(serr))
}
