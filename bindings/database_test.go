package bindings

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"gopkg.in/redis.v5"
	"log"
	"testing"
)

func testLogger(t *testing.T) (*log.Logger, func()) {
	b := bytes.NewBuffer(nil)
	l := log.New(b, "", log.Lshortfile|log.Ltime)
	buf := bufio.NewReader(b)
	f := func() {
		for {
			line, e := buf.ReadString('\n')
			if e != nil {
				break
			}
			t.Logf(`dump %s`, line)
		}
	}
	return l, f
}

func TestLoadAll(t *testing.T) {
	db, sqlm, _ := sqlmock.New()

	sqlm.ExpectExec("purchases").WillReturnError(fmt.Errorf(`expectedErr`))

	sqlm.ExpectQuery("folders").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(5))

	sqlm.ExpectQuery("folders").WithArgs(5).
		WillReturnRows(sqlmock.NewRows([]string{"budget", "bid", "creative_id", "owner"}).
			AddRow(100, 50, 30, 5))
	sqlm.ExpectQuery("parent_folder").WithArgs(5).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("7"))
	sqlm.ExpectQuery("parent_folder").WithArgs(5).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("8"))
	sqlm.ExpectQuery("dimentions").WithArgs(5).WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))

	sqlm.ExpectQuery("creatives").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(5))
	sqlm.ExpectQuery("creatives").WithArgs(5).
		WillReturnRows(sqlmock.NewRows([]string{"url"}).AddRow("test.com"))

	sqlm.ExpectQuery("users").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(5))
	sqlm.ExpectQuery("ip_histories").WithArgs(5).
		WillReturnRows(sqlmock.NewRows([]string{"ip"}).AddRow("1.1.1.1"))
	sqlm.ExpectQuery("user_settings").WithArgs(5).
		WillReturnRows(sqlmock.NewRows([]string{"setting", "value"}).AddRow(6, "what"))

	sqlm.ExpectQuery("SELECT (.+) FROM countries").
		WillReturnRows(sqlmock.NewRows([]string{"id", "iso_2alpha"}))

	sqlm.ExpectQuery("SELECT (.+) FROM networks").
		WillReturnRows(sqlmock.NewRows([]string{"id", "iso_2alpha"}))

	sqlm.ExpectQuery("SELECT (.+) FROM subnetworks").
		WillReturnRows(sqlmock.NewRows([]string{"id", "iso_2alpha"}))

	sqlm.ExpectQuery("SELECT (.+) FROM subnetworks").
		WillReturnRows(sqlmock.NewRows([]string{"id", "iso_2alpha"}))

	sqlm.ExpectQuery("SELECT (.+) FROM brands").
		WillReturnRows(sqlmock.NewRows([]string{"id", "iso_2alpha"}))

	sqlm.ExpectQuery("SELECT (.+) FROM verticals").
		WillReturnRows(sqlmock.NewRows([]string{"id", "iso_2alpha"}))

	sqlm.MatchExpectationsInOrder(false)

	out, dump := testLogger(t)
	if err := (&Production{ConfigDB: db, StatsDB: db, Logger: out, Debug: out, DefaultKey: ":", Redis: redis.NewClient(&redis.Options{})}).Cycle(); err != nil {
		t.Log("failed to cycle, dumping")
		dump()
		t.Log("err", err.Error())
	} else {
		dump()
	}
	if err := sqlm.ExpectationsWereMet(); err != nil {
		t.Error("err", err.Error())
	}
}
