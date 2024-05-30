package pgxtest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestPostgreSQL(t *testing.T) {
	ctx := context.Background()
	t.Parallel()

	pg, err := Start(ctx, Config{})
	if err != nil {
		t.Errorf("failed to start pgxtest: %v", err)
	}

	defer func() {
		if err = pg.Stop(); err != nil {
			t.Errorf("failed to stop pgxtest: %v", err)
		}
	}()

	if pg == nil {
		t.Errorf("null pg returned unexpectedly")
	}

	conn, err := pg.Pool.Acquire(ctx)
	if err != nil {
		t.Errorf("failed to acquire a connection: %v", err)
	}

	defer conn.Release()

	_, err = conn.Exec(ctx, "CREATE TABLE test (val text)")
	if err != nil {
		t.Errorf("failed to create table: %v", err)
	}
}

func TestPostgreSQLWithConfig(t *testing.T) {
	ctx := context.Background()
	t.Parallel()

	var pgBinDir string

	for _, binDir := range []string{
		"/usr/bin",
		"/opt/homebrew/bin",
		"/usr/lib/postgresql/16/bin",
		"/usr/lib/postgresql/15/bin",
		"/usr/lib/postgresql/14/bin",
	} {
		if _, err := os.Stat(filepath.Join(binDir, "postgres")); err == nil {
			pgBinDir = binDir
			break
		}
	}

	if pgBinDir == "" {
		t.Skip()
	}

	pg, err := Start(ctx, Config{BinDir: pgBinDir})
	if err != nil {
		t.Errorf("failed to start pgxtest: %v", err)
	}
	defer func() {
		if err = pg.Stop(); err != nil {
			t.Errorf("failed to stop pgxtest: %v", err)
		}
	}()

	if pg == nil {
		t.Errorf("null pg returned unexpectedly")
	}

	conn, err := pg.Pool.Acquire(ctx)
	if err != nil {
		t.Errorf("failed to acquire a connection: %v", err)
	}
	defer conn.Release()

	if _, err = conn.Exec(ctx, "CREATE TABLE test (val text)"); err != nil {
		t.Errorf("failed to create table: %v", err)
	}

	if pg.Host == "" || pg.Name == "" {
		t.Errorf("pg.Host=%q or pg.Name=%q are empty", pg.Host, pg.Name)
	}
}

func TestAdditionalArgs(t *testing.T) {
	ctx := context.Background()
	t.Parallel()

	pg, err := Start(ctx, Config{AdditionalArgs: []string{"-c", "wal_level=logical"}})
	if err != nil {
		t.Errorf("failed to start pgxtest: %v", err)
	}
	defer func() {
		if err = pg.Stop(); err != nil {
			t.Errorf("failed to stop pgxtest: %v", err)
		}
	}()

	if pg == nil {
		t.Errorf("null pg returned unexpectedly")
	}

	conn, err := pg.Pool.Acquire(ctx)
	if err != nil {
		t.Errorf("failed to acquire a connection: %v", err)
	}
	defer conn.Release()

	//Check if the wal_level is set to logical
	var walLevel string
	if err := conn.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		t.Errorf("failed to SHOW wal_level: %v", err)
	}
	if walLevel != "logical" {
		t.Errorf("expected walLevel 'logical', got %q", walLevel)
	}
}
