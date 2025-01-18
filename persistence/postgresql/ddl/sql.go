package ddl

import _ "embed"

//go:embed journals.sql
var JournalsSQL string

//go:embed snapshots.sql
var SnapshotsSQL string
