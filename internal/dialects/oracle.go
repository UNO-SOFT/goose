package dialects

import (
	"fmt"

	"github.com/pressly/goose/v3/database/dialect"
)

// NewOracle returns a new [dialect.Querier] for Oracle dialect.
func NewOracle() dialect.QuerierExtender {
	return &oracle{}
}

type oracle struct{}

var _ dialect.QuerierExtender = (*oracle)(nil)

func (p *oracle) CreateTable(tableName string) string {
	q := `CREATE TABLE %s (
		id integer(18) GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1),
		version_id INTEGER(18) NOT NULL,
		is_applied CHAR(1) NOT NULL,
		tstamp timestamp NOT NULL DEFAULT now()
	)`
	return fmt.Sprintf(q, tableName)
}

func (p *oracle) InsertVersion(tableName string) string {
	q := `INSERT INTO %s (version_id, is_applied) VALUES (:1, :2)`
	return fmt.Sprintf(q, tableName)
}

func (p *oracle) DeleteVersion(tableName string) string {
	q := `DELETE FROM %s WHERE version_id=:1`
	return fmt.Sprintf(q, tableName)
}

func (p *oracle) GetMigrationByVersion(tableName string) string {
	q := `SELECT tstamp, is_applied FROM %s WHERE version_id=:1 ORDER BY tstamp DESC FETCH FIRST 1 ROW ONLY`
	return fmt.Sprintf(q, tableName)
}

func (p *oracle) ListMigrations(tableName string) string {
	q := `SELECT version_id, is_applied from %s ORDER BY id DESC`
	return fmt.Sprintf(q, tableName)
}

func (p *oracle) GetLatestVersion(tableName string) string {
	q := `SELECT max(version_id) FROM %s`
	return fmt.Sprintf(q, tableName)
}

func (p *oracle) TableExists(tableName string) string {
	schemaName, tableName := parseTableIdentifier(tableName)
	if schemaName != "" {
		q := `SELECT COUNT(0) FROM DUAL WHERE EXISTS ( SELECT 1 FROM pg_tables WHERE schemaname = '%s' AND tablename = '%s' )`
		return fmt.Sprintf(q, schemaName, tableName)
	}
	q := `SELECT COUNT(0) FROM DUAL WHERE EXISTS ( SELECT 1 FROM pg_tables WHERE (current_schema() IS NULL OR schemaname = current_schema()) AND tablename = '%s' )`
	return fmt.Sprintf(q, tableName)
}
