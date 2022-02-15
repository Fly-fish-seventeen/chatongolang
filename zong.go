// 全全新总控简单 project main.go
package main

import (
	"database/sql"
	"fmt"
	"net"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func handleConnection(conn net.Conn, pe map[int]net.Conn, m *sync.RWMutex, db *sql.DB) {
	defer conn.Close()
	buf := make([]byte, 64)

	_, err1 := conn.Read(buf)
	if err1 != nil {
		fmt.Println("errlisten111 = ", err1)
		return
	}
	var id int = int(buf[0])
	m.Lock()
	pe[id] = conn

	m.Unlock()

	fmt.Println("storeoneid", id)
	//从数据库查询数据
	rows, err := db.Query("SELECT * FROM juku where goal=?", id)
	fmt.Println("inner", err)
	bigbuf := make([]byte, 1024*512)
	for rows.Next() {
		var id int
		var word string
		var yid int
		var st int
		err = rows.Scan(&id, &word, &yid, &st)
		fmt.Println(err, id, word, yid, st)
		if st == 4 {
			//用大量筒装文件
			//此时word是文件名，利用io读，发
			for {
				break
			}
		} else {
			_, err := conn.Write([]byte(word))
			if err != nil {
				return
			}

		}
		time.Sleep(time.Duration(500) * time.Millisecond)
	}

	stmt, err := db.Prepare("DELETE FROM juku WHERE goal=?")
	checkErr(err)
	res, err := stmt.Exec(id)
	checkErr(err)
	num, err := res.RowsAffected()
	fmt.Printf("affected：%d\n", num)

	//处理完积蓄消息，开始听新消息

	for {

		n, err1 := conn.Read(buf)
		if err1 != nil {
			m.Lock()
			delete(pe, id)
			m.Unlock()
			fmt.Println("errlisten111 = ", err1)

			break
		}
		fmt.Println("getonemessage")
		var taid int = int(buf[0])

		//	newbuf := make([]byte, n)
		newbuf := buf[:n]
		fmt.Println("getbuf = ", taid)
		m.RLock()
		v1, ok5 := pe[taid]
		m.RUnlock()
		if ok5 {
			fmt.Println("it's an existing key")
			if newbuf[1] == 0 {
				//是否为发送图片，换一个大buf装，以减少时间占用
				fileits := string(newbuf[:n])
				//放到前面定义		bigbuf := make([]byte, 1024*512)

				for {

					number1, err1 := conn.Read(bigbuf)
					if err1 != nil {
						fmt.Println("errlisten111 = ", err1, fileits)
						return
					}
					v1.(net.Conn).Write(bigbuf[:number1])
					if number1 < 1024*512 && bigbuf[0] == 1 {

						break
					}

				}
			} else {

				num, ok2 := v1.(net.Conn).Write(newbuf)
				if ok2 != nil {
					fmt.Println("send msgerror", ok2)
				} else {
					fmt.Println("send msg", num)
				}
			}

		} else {
			fmt.Println("it's an unknown key")

			if newbuf[1] == 0 {
				//是否为发送图片，换一个大buf装，以减少时间占用
				fileits := string(newbuf[:n])
				//	bigbuf := make([]byte, 1024*512)

				for {

					number1, err1 := conn.Read(bigbuf)
					if err1 != nil {
						fmt.Println("errlisten111 = ", err1, fileits)
						return
					}

					if number1 < 1024*512 && bigbuf[0] == 1 {

						break
					}
					//io写文件
					//将文件名存入数据库，并指明是图片消息

				}
			} else {

				d := string(buf[:n])
				//存入数据库
				stmt, err := db.Prepare("INSERT INTO juku(word,goal) VALUES(?,?)")
				if err != nil {
					return
				}
				res, err := stmt.Exec(d, taid)
				if err != nil {
					return
				}
				lastId, err := res.LastInsertId()
				if err != nil {
					return
				}
				rowCnt, err := res.RowsAffected()
				if err != nil {
					return
				}
				fmt.Printf("ID = %d, 存入数据库affected = %d\n", lastId, rowCnt)
			}
		}

	}

}
func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
func main() {
	fmt.Println("Hellostart")
	db, err0 := sql.Open("mysql", "root:lk19970701@/stu?charset=utf8")
	checkErr(err0)
	pe := make(map[int]net.Conn, 20)
	var m *sync.RWMutex
	m = new(sync.RWMutex)
	listener, err := net.Listen("tcp", "127.0.0.1:8183")
	if err != nil {
		fmt.Println("err = ", err)
		return
	}
	fmt.Println("start")
	//退出前把监听关闭
	defer listener.Close()

	//阻塞等待用户链接
	for {
		fmt.Println("wait log")
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("err = ", err)
			continue
		}
		fmt.Println("clientthread")
		go handleConnection(conn, pe, m, db)
	}
}
