# pgxtest

> Go library to spawn single-use PostgreSQL servers for unit testing

[![Go Reference](https://pkg.go.dev/badge/github.com/dottedmag/pgxtest.svg)](https://pkg.go.dev/github.com/dottedmag/pgxtest)

Spawns a PostgreSQL server with a single database configured, and returns a
`*pgxpool.Pool`. Ideal for unit tests where you want a clean instance each time.
Then clean up afterwards.

Features:

* Starts a clean isolated PostgreSQL database
* Returns `*pgxconn.Pool`, not a `database/sql` connection.
* Optimized for in-memory execution, to speed up unit tests
* Less than 1 second startup / initialization time
* Automatically drops permissions when testing as root

## Usage

In your unit test:
```go
pg, err := pgxtest.Start()
defer pg.Stop()

// Do something with pg.Pool (which is a *pgxpool.Pool)
```

## License

This library is distributed under the [MIT](LICENSE) license.
