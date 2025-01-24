package ddl

import _ "embed"

//go:embed spawn_locks.sql
var SpawnLocksSql string

//go:embed activations.sql
var ActivationsSql string
