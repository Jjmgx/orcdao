// daoutil
package orcdao

import (
	"database/sql"
	"errors"
	"strconv"

	"reflect"
	"strings"

	"github.com/axgle/mahonia"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
)

type MgxOrm struct {
	Db *sql.DB
}

func (this *MgxOrm) Exec(query string, args ...interface{}) (sql.Result, error) {
	return this.Db.Exec(query, args...)
}

func (this *MgxOrm) Open(driverName, dataSourceName string) error {
	var err error
	if this.Db, err = sql.Open(driverName, dataSourceName); err != nil {
		return err
	}
	return nil
}
func (this *MgxOrm) OpenEx(driverName, root, pass, host, port, database, charset string) error {
	var err error
	if this.Db, err = sql.Open(driverName, root+":"+pass+"@tcp("+host+":"+port+")/"+database+"?charset="+charset+"&parseTime=True&loc=Local"); err != nil {
		return err
	}
	return nil
}

func (this *MgxOrm) Close() {
	if this.Db != nil {
		this.Db.Close()
	}
}

func (this *MgxOrm) Query(dest interface{}, sql string, args ...interface{}) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return errors.New("传入的dest必须为指针")
	}
	if value.IsNil() {
		return errors.New("传入的dest不能为nil")
	}
	result := reflect.Indirect(value)
	t := reflect.TypeOf(dest)
	t = t.Elem().Elem()
	sqlstr := pingSql(sql, t)
	rows, err := this.Db.Query(sqlstr, args...)
	if err != nil {
		return err
	} else {
		defer rows.Close()
		vp := reflect.New(t)
		v := reflect.Indirect(vp)
		onerow := createScanAddr(v)
		for rows.Next() {
			err = rows.Scan(onerow...)
			if err != nil {
				return err
			}
			result.Set(reflect.Append(result, v))
		}
	}
	return nil
}

func (this *MgxOrm) QueryOne(dest interface{}, sql string, args ...interface{}) error {
	value := reflect.ValueOf(dest)
	if value.Kind() != reflect.Ptr {
		return errors.New("传入的dest必须为指针")
	}
	if value.IsNil() {
		return errors.New("传入的dest不能为nil")
	}
	result := reflect.Indirect(value)
	t := reflect.TypeOf(dest)
	t = t.Elem()
	sqlstr := pingSql(sql, t)
	//	fmt.Println(sqlstr)
	rows, err := this.Db.Query(sqlstr, args...)
	if err != nil {
		return err
	} else {
		defer rows.Close()
		vp := reflect.New(t)
		v := reflect.Indirect(vp)
		onerow := createScanAddr(v)
		if rows.Next() {
			err = rows.Scan(onerow...)
			if err != nil {
				return err
			}
			result.Set(v)
		}
	}
	return nil
}

func (this *MgxOrm) Utf8ToGbk(src string) string {
	desCoder := mahonia.NewEncoder("gbk")
	return desCoder.ConvertString(src)
}
func (this *MgxOrm) GbkToUtf8(src string) string {
	srcCoder := mahonia.NewDecoder("gbk")
	return srcCoder.ConvertString(src)
}

func (this *MgxOrm) Nrzm(s string) string {
	s = this.GbkToUtf8(s)
	s = strings.Replace(s, "\\", "\\\\", -1)
	s = strings.Replace(s, "\r", "\\\r", -1)
	s = strings.Replace(s, "\n", "\\\n", -1)
	s = strings.Replace(s, "'", "\\'", -1)
	s = strings.Replace(s, "(", "\\(", -1)
	s = strings.Replace(s, ")", "\\)", -1)
	s = strings.Replace(s, "`", "\\`", -1)
	return this.Utf8ToGbk(s)
}

func (this *MgxOrm) Nrzm2(s string) string {
	s = strings.Replace(s, "\\", "\\\\", -1)
	s = strings.Replace(s, "\r", "\\\r", -1)
	s = strings.Replace(s, "\n", "\\\n", -1)
	s = strings.Replace(s, "'", "\\'", -1)
	s = strings.Replace(s, "(", "\\(", -1)
	s = strings.Replace(s, ")", "\\)", -1)
	s = strings.Replace(s, "`", "\\`", -1)
	return this.Utf8ToGbk(s)
}

func pingSql(sql string, t reflect.Type) string {
	if strings.HasPrefix(strings.ToUpper(sql), "SELECT *") {
		var zds = ""
		for i := 0; i < t.NumField(); i++ {
			if i > 0 {
				zds += ","
			}
			zds += strings.ToLower(t.Field(i).Name)
		}
		return strings.Replace(sql, "*", zds, 1)
	} else {
		return sql
	}
}
func createScanAddr(v reflect.Value) []interface{} {
	leng := v.NumField()
	onerow := make([]interface{}, leng)
	for i := 0; i < leng; i++ {
		onerow[i] = v.Field(i).Addr().Interface()
	}
	return onerow
}

func (this *MgxOrm) ShowTables(sql string) ([]string, error) {
	result := []string{}
	rows, err := this.Db.Query(sql)
	if err != nil {
		return result, err
	} else {
		defer rows.Close()
		for rows.Next() {
			tabname := ""
			err = rows.Scan(&tabname)
			if err != nil {
				return result, err
			}
			result = append(result, tabname)
		}
	}
	return result, nil
}

func (this *MgxOrm) GetDataMap(sql string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := this.Db.Query(sql, args...)
	if err != nil {
		return nil, err
	} else {
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		count := len(columns)
		tableData := make([]map[string]interface{}, 0)
		values := make([]interface{}, count)
		valuePtrs := make([]interface{}, count)
		for rows.Next() {
			for i := 0; i < count; i++ {
				valuePtrs[i] = &values[i]
			}
			rows.Scan(valuePtrs...)
			entry := make(map[string]interface{})
			for i, col := range columns {
				var v interface{}
				val := values[i]
				b, ok := val.([]byte)
				if ok {
					v = string(b)
				} else {
					v = val
				}
				entry[col] = v
			}
			tableData = append(tableData, entry)
		}

		return tableData, nil
	}

}
func (this *MgxOrm) GetColumnNames(tabname string) ([]string, error) {
	rows, err := this.Db.Query("select * from `" + tabname + "` limit 0,1")
	if err != nil {
		return nil, err
	} else {
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		} else {
			return columns, nil
		}
	}
}

func (this *MgxOrm) GetDataMapAndColnumNames(sql string, args ...interface{}) ([]map[string]interface{}, []string, error) {
	rows, err := this.Db.Query(sql, args...)
	if err != nil {
		return nil, nil, err
	} else {
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			return nil, nil, err
		}
		count := len(columns)
		tableData := make([]map[string]interface{}, 0)
		values := make([]interface{}, count)
		valuePtrs := make([]interface{}, count)
		for rows.Next() {
			for i := 0; i < count; i++ {
				valuePtrs[i] = &values[i]
			}
			rows.Scan(valuePtrs...)
			entry := make(map[string]interface{})
			for i, col := range columns {
				var v interface{}
				val := values[i]
				b, ok := val.([]byte)
				if ok {
					v = string(b)
				} else {
					v = val
				}
				entry[col] = v
			}
			tableData = append(tableData, entry)
		}

		return tableData, columns, nil
	}

}

func (this *MgxOrm) GetCount(sql string) (int, error) {
	rows, err := this.Db.Query(sql)
	if err != nil {
		return 0, err
	} else {
		defer rows.Close()
		if rows.Next() {
			count := 0
			err = rows.Scan(&count)
			if err != nil {
				return 0, err
			} else {
				return count, nil
			}

		} else {
			return 0, nil
		}
	}
}

func (this *MgxOrm) GetCount2(sql string, args ...interface{}) (int, error) {
	rows, err := this.Db.Query(sql, args...)
	if err != nil {
		return 0, err
	} else {
		defer rows.Close()
		if rows.Next() {
			count := 0
			err = rows.Scan(&count)
			if err != nil {
				return 0, err
			} else {
				return count, nil
			}

		} else {
			return 0, nil
		}
	}
}

func (this *MgxOrm) GetCountFloat64(sql string, args ...interface{}) (float64, error) {
	rows, err := this.Db.Query(sql, args...)
	if err != nil {
		return 0, err
	} else {
		defer rows.Close()
		if rows.Next() {
			count := float64(0)
			err = rows.Scan(&count)
			if err != nil {
				if strings.Index(err.Error(), "converting NULL") != -1 {
					return 0, nil
				}
				return 0, err
			} else {
				return count, nil
			}

		} else {
			return 0, nil
		}
	}
}

type PageBean struct {
	AllRow          int  //总记录数
	TotalPage       int  //总页数
	CurrentPage     int  //当前第几页
	PageSize        int  //每页最多记录数
	IsFirstPage     bool // 是否首页
	IsLastPage      bool // 是否尾页
	HasPreviousPage bool //可以上翻页
	HasNextPage     bool //可以下翻页
}

func (this *MgxOrm) GetPageBean(dest interface{}, sql string, currentpage, pagesize int, orderField, orderDirection string, args ...interface{}) (PageBean, error) {
	return this.GetPageBean2(dest, "SELECT *", sql, currentpage, pagesize, orderField, orderDirection, args...)
}

func (this *MgxOrm) GetPageBean2(dest interface{}, sqlt, sql string, currentpage, pagesize int, orderField, orderDirection string, args ...interface{}) (PageBean, error) {
	if currentpage == 0 {
		currentpage = 1
	}
	if pagesize == 0 {
		pagesize = 20
	}
	allRow, _ := this.GetCount("select count(*) " + sql)
	totalPage := int((allRow-pagesize)/pagesize + 1)
	offset := pagesize * (currentpage - 1)
	if currentpage == 0 {
		currentpage = 1
	}

	if orderField != "" {
		if orderDirection == "" {
			orderDirection = "asc"
		}
		sql += " order by " + orderField + " " + orderDirection
	}
	if err := this.Query(dest, sqlt+" "+sql+" limit "+strconv.Itoa(offset)+","+strconv.Itoa(pagesize)); err != nil {
		return PageBean{}, err
	} else {
		pageBean := PageBean{
			AllRow:          allRow,
			TotalPage:       totalPage,
			CurrentPage:     currentpage,
			PageSize:        pagesize,
			IsFirstPage:     currentpage == 1,
			IsLastPage:      currentpage == totalPage,
			HasPreviousPage: currentpage != 1,
			HasNextPage:     currentpage != totalPage,
		}
		return pageBean, nil
	}

}

func (this *MgxOrm) SaveObject(obj interface{}, tabname, zj string) (int64, error) {
	t := reflect.TypeOf(obj)
	value := reflect.ValueOf(obj)
	zds := ""
	zwf := ""
	var tjs []interface{} = make([]interface{}, 0)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i).Name
		if strings.ToLower(f) != zj {
			if zds != "" {
				zds += ","
				zwf += ","
			}
			zds += "`" + strings.ToLower(f) + "`"
			zwf += "?"
			tjs = append(tjs, value.FieldByName(f).Interface())
		}
	}
	sql := "INSERT INTO `" + tabname + "` (" + zds + ") VALUES (" + zwf + ")"
	if re, err := this.Exec(sql, tjs...); err != nil {
		return 0, err
	} else {
		lastId, _ := re.LastInsertId()
		return lastId, err
	}

}

func (this *MgxOrm) UpdateObject(obj interface{}, tabname, zj string) (int64, error) {
	t := reflect.TypeOf(obj)
	value := reflect.ValueOf(obj)
	sql := ""
	var zjzhi interface{}
	var tjs []interface{} = make([]interface{}, 0)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i).Name
		if strings.ToLower(f) != zj {
			if sql != "" {
				sql += ","
			}
			sql += "`" + strings.ToLower(f) + "`=?"
			tjs = append(tjs, value.FieldByName(f).Interface())
		} else {
			zjzhi = value.FieldByName(f).Interface()
		}
	}
	sql = "UPDATE `" + tabname + "` SET " + sql + " WHERE `" + zj + "`=?"
	tjs = append(tjs, zjzhi)
	if re, err := this.Exec(sql, tjs...); err != nil {
		return 0, err
	} else {
		lastId, _ := re.LastInsertId()
		return lastId, err
	}
}

/**
type User struct {
	Id   int `mgx:"-"`
	Name string `mgx:"name"`
	Pass string
	Cw   int `mgx:"iscw"`
}
**/
func (this *MgxOrm) SaveObjectEx(tabname string, obj interface{}) (int64, error) {
	tu := reflect.TypeOf(obj)
	vu := reflect.ValueOf(obj)
	sql := ""
	sqlw := ""
	cs := []interface{}{}
	if tu.NumField() == 0 {
		return 0, errors.New("no field")
	}
	for i := 0; i < tu.NumField(); i++ {
		n := tu.Field(i).Tag.Get("mgx")
		if n != "-" {
			if n == "" {
				n = strings.ToLower(tu.Field(i).Name)
			}
			v := vu.Field(i).Interface()
			if sql == "" {
				sql = "INSERT INTO `" + tabname + "` ("
				sqlw = " VALUES ("
			} else {
				sql += ","
				sqlw += ","
			}
			sql += "`" + n + "`"
			sqlw += "?"
			cs = append(cs, v)
		}
	}
	sql += ")"
	sqlw += ")"
	if r, err := this.Exec(sql+sqlw, cs...); err != nil {
		return 0, err
	} else {
		return r.LastInsertId()
	}
}

func (this *MgxOrm) TabExist(tab string) (bool, error) {
	sql := "SHOW TABLES LIKE '%" + tab + "%'"
	rows, err := this.Db.Query(sql)
	if err != nil {
		return false, err
	} else {
		defer rows.Close()
		if rows.Next() {
			return true, nil
		} else {
			return false, nil
		}
	}
}
