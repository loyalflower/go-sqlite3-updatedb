/**
 * 程序中sqlite3数据库升级迁移工具
 */
package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var rootPath, _ = filepath.Abs(filepath.Dir(os.Args[0]))

func main() {

	if len(os.Args) != 3 {
		fmt.Println(":help")
		fmt.Println("migrate \"origin sqlite3 DB path\" \"target sqlite3 DB path\"")
		return
	}
	// originDBPath := "f:/golang/reports.bin"
	// targetDBPath := "f:/golang/reports1.bin"
	originDBPath := os.Args[1]
	targetDBPath := os.Args[2]

	mdb := new(migrateDB)
	mdb.originDBPath = originDBPath
	mdb.targetDBPath = targetDBPath
	mdb.pageSize = 10000
	mdb.insertTables = []string{"trpt"}

	mdb.Open()

	mdb.Migrate()

	defer mdb.Close()

	os.Remove(rootPath + "/migrateError.log")
}

type urow []interface{}

type row struct {
	columns []string
	data    []urow
}

type table struct {
	name     string
	dataType map[string]string
}

type migrateDB struct {
	originDBPath string
	targetDBPath string
	originDB     *sql.DB
	targetDB     *sql.DB
	originTables []table
	targetTables []table
	pageSize     int

	insertTables []string
}

func (mdb *migrateDB) Open() bool {
	var err error
	mdb.originDB, err = sql.Open("sqlite3", mdb.originDBPath)
	checkErr(err)
	mdb.targetDB, err = sql.Open("sqlite3", mdb.targetDBPath)
	checkErr(err)
	return true
}

func (mdb *migrateDB) getTables() {
	getTableNameSql := "SELECT name,sql FROM sqlite_master WHERE type='table'"

	row, err := mdb.originDB.Query(getTableNameSql)
	checkErr(err)
	defer row.Close()

	initTableStruct := func(row *sql.Rows) table {
		var name string
		var createSql string
		var tableStruct table
		err = row.Scan(&name, &createSql)
		checkErr(err)
		tableStruct.name = name
		tableStruct.dataType = mdb.parseDataType(createSql)
		return tableStruct
	}

	for row.Next() {
		mdb.originTables = append(mdb.originTables, initTableStruct(row))
	}

	trow, err := mdb.targetDB.Query(getTableNameSql)
	checkErr(err)
	defer trow.Close()

	for trow.Next() {
		mdb.targetTables = append(mdb.targetTables, initTableStruct(trow))
	}
}

func (mdb *migrateDB) parseDataType(createSql string) map[string]string {
	var result = make(map[string]string)

	typeString := createSql[strings.Index(createSql, "(")+1 : strings.LastIndex(createSql, ")")]

	typeSlice := strings.Split(typeString, ",")

	for _, v := range typeSlice {
		col := strings.SplitN(strings.Trim(v, " "), " ", 3)
		if 2 > len(col) {
			result[col[0]] = ""
		} else {
			result[col[0]] = strings.ToUpper(col[1])
		}
	}

	return result
}

func (mdb *migrateDB) CheckDatatype(tableStruct table, colName string, dataType string) bool {
	return mdb.getDatatype(tableStruct, colName) == strings.ToUpper(dataType)
}

func (mdb *migrateDB) getDatatype(tableStruct table, colName string) string {
	if "[" != string(colName[0]) {
		colName = "[" + colName + "]"
	}
	for c, t := range tableStruct.dataType {
		if c == colName {
			return t
		}
	}
	return ""
}

func (mdb *migrateDB) tableExists(name string) bool {

	for _, value := range mdb.targetTables {
		if value.name == name {
			return true
		}
	}
	return false

}

func (mdb *migrateDB) createTable(name string) bool {
	sql := "SELECT sql FROM sqlite_master WHERE name=?"
	row, err := mdb.targetDB.Query(sql, name)
	checkErr(err)

	defer row.Close()

	row.Next()

	var csql string

	row.Scan(&csql)

	_, err = mdb.targetDB.Exec(csql)
	checkErr(err)

	return true
}

func (mdb *migrateDB) read(tableStruct table, page int) (row, error) {
	myrow := row{}

	page_size := mdb.pageSize

	offset := "0"

	if page > 1 {
		offset = strconv.Itoa((page - 1) * page_size)
	}

	row, err := mdb.originDB.Query("select * from `" + tableStruct.name + "` limit " + offset + ", " + strconv.Itoa(page_size))

	defer row.Close()

	checkErr(err)

	myrow.columns, err = row.Columns()

	values := make([]interface{}, len(myrow.columns))

	result := make([]interface{}, len(myrow.columns))

	for i, _ := range myrow.columns {
		result[i] = &values[i]
	}

	for row.Next() {
		row.Scan(result...)

		unkownrow := make([]interface{}, len(myrow.columns))

		for i, col := range values {
			if num, ok := col.(int64); ok {
				t := num
				unkownrow[i] = t
			} else if s, ok := col.(string); ok {
				fmt.Println("string: ", s)
				t := s
				unkownrow[i] = t
			} else if dt, ok := col.(time.Time); ok {
				t := dt
				unkownrow[i] = t
			} else if b, ok := col.(bool); ok {
				t := b
				unkownrow[i] = t
			} else if bt, ok := col.([]byte); ok {
				t := bt
				if mdb.CheckDatatype(tableStruct, myrow.columns[i], "blob") {
					unkownrow[i] = t
				} else {
					unkownrow[i] = string(t)
				}
			} else if bt, ok := col.(byte); ok {
				t := bt
				unkownrow[i] = t
			} else if ui8, ok := col.([]uint8); ok {
				t := string(ui8)
				unkownrow[i] = t
			}
		}

		myrow.data = append(myrow.data, unkownrow)
	}
	return myrow, nil
}

/**
 * 使用事务提高处理性能
 */
func (mdb *migrateDB) replace(tableStruct table, rows row) bool {
	rowMaxNum := 20
	offset := 0

	sql := "REPLACE INTO `" + tableStruct.name + "`(`" +
		strings.Join(rows.columns, "`, `") + "`) VALUES"
	tx, err := mdb.targetDB.Begin()
	checkErr(err)
	for {
		var insertData []interface{}
		curSize := rowMaxNum
		if len(rows.data)-offset < rowMaxNum {
			curSize = len(rows.data) - offset
		}

		if curSize < 1 {
			break
		}

		dt2 := make([]string, curSize)
		for j := 0; j < curSize; j++ {
			dt := make([]string, len(rows.columns))
			for i := 0; i < len(rows.columns); i++ {
				dt[i] = "?"
			}

			dt2[j] = "(" + strings.Join(dt, ",") + ")"
		}

		insertSql := sql + strings.Join(dt2, ",")

		for _, r := range rows.data[offset : curSize+offset] {
			insertData = append(insertData, r...)
		}
		// fmt.Println("\n\n\n", insertSql, insertData)
		_, err := tx.Exec(insertSql, insertData...)
		checkErr(err)
		if curSize != rowMaxNum {
			break
		}
		offset = offset + curSize
	}
	tx.Commit()

	return true
}

func (mdb *migrateDB) Migrate() bool {
	mdb.getTables()

	for _, tableStruct := range mdb.originTables {
		tbl := tableStruct.name
		if !mdb.tableExists(tbl) {
			mdb.createTable(tbl)
		}

		page := 1
		for {
			rows, err := mdb.read(tableStruct, page)
			if 0 == len(rows.data) || nil != err {
				break
			}
			checkErr(err)
			mdb.replace(tableStruct, rows)
			if len(rows.data) < mdb.pageSize {
				break
			}
			page = page + 1
		}
	}
	return true
}

func (mdb *migrateDB) Close() bool {
	mdb.originDB.Close()
	mdb.targetDB.Close()
	return true
}

func checkErr(err error) bool {
	if nil != err {
		fmt.Println("run error:", err)
		ioutil.WriteFile(rootPath+"/migrateError.log", []byte(err.Error()), 0766)
		panic(err)
		return false
	}
	return true
}
