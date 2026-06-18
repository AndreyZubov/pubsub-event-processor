// Package migrations exposes the SQL migration files as an embedded filesystem
// so they can be applied at process startup without depending on a separate
// migrations directory on disk.
package migrations

import "embed"

// FS holds every SQL migration file at this package's root so the binary
// carries them and applies them on startup.
//
//go:embed *.sql
var FS embed.FS
