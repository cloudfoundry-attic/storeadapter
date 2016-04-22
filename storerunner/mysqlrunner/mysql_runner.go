package mysqlrunner

import (
	"database/sql"
	"fmt"
	"os"

	. "github.com/onsi/gomega"
)

type MySQLRunner struct {
	sqlDBName string
	db        *sql.DB
}

func NewMySQLRunner(sqlDBName string) *MySQLRunner {
	return &MySQLRunner{
		sqlDBName: sqlDBName,
	}
}

func (m *MySQLRunner) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	// mysql must be set up on localhost as described in the CONTRIBUTING.md doc
	// in diego-release.
	var err error
	m.db, err = sql.Open("mysql", "diego:diego_password@/")
	Expect(err).NotTo(HaveOccurred())
	Expect(m.db.Ping()).NotTo(HaveOccurred())

	_, err = m.db.Exec(fmt.Sprintf("CREATE DATABASE %s", m.sqlDBName))
	Expect(err).NotTo(HaveOccurred())

	m.db, err = sql.Open("mysql", fmt.Sprintf("diego:diego_password@/%s", m.sqlDBName))
	Expect(err).NotTo(HaveOccurred())
	Expect(m.db.Ping()).NotTo(HaveOccurred())

	close(ready)

	<-signals

	_, err = m.db.Exec(fmt.Sprintf("DROP DATABASE %s", m.sqlDBName))
	Expect(err).NotTo(HaveOccurred())
	Expect(m.db.Close()).To(Succeed())

	return nil
}

func (m *MySQLRunner) ConnectionString() string {
	return fmt.Sprintf("diego:diego_password@/%s", m.sqlDBName)
}

func (m *MySQLRunner) Reset() {
	var truncateTablesSQL = []string{
		"TRUNCATE TABLE domains",
		"TRUNCATE TABLE configurations",
		"TRUNCATE TABLE tasks",
		"TRUNCATE TABLE desired_lrps",
		"TRUNCATE TABLE actual_lrps",
	}
	for _, query := range truncateTablesSQL {
		result, err := m.db.Exec(query)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RowsAffected()).To(BeEquivalentTo(0))
	}
}
