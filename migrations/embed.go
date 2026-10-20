// Package migrations embeds the SQL migration files so they can be
// compiled into the scheduler binary.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
