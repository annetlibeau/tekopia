// Oracle PeopleSoft Upgrade Customization Impact Analysis Report.
// Copyright © 2015 Annet Libeau. Sun Day Consulting, Inc.

package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-oci8" // Copyright © 2014-2015 Yasuhiro Matsumoto. Governed by a separate license agreement. See https://github.com/mattn/go-oci8.
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	rid, upgrade, upgcust = "Tekopia", "UPGRADE", "UPGCUST" // Report ID, Database compare project containing records, Project created during upgrade containing custom objects
)

var (
	mode                    int                  // Tekopia can run in four modes; mode is determined by prompting when the program runs
	searchdir               string = "/psft/sqr" // Directory where custom SQRs reside
	tblmtch, fldmtch, cfrom string
)

func main() {
	db, err := sql.Open("oci8", getDSN())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	if err = prepDb(db); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Print("\nReport Options:\n\n 1. List structure changes only\n 2. Run audit only for SQRs\n 3. Run audit only for online objects\n 4. Run full report\n\n Enter level of detail needed (1, 2, 3 or 4) : ")
	fmt.Scan(&mode)
	if 1 <= mode && mode <= 4 {
		fmt.Println("Running report number", mode)
	} else {
		fmt.Println(" Invalid report option. Exiting program.")
		return
	}

	// Open log file
	file1, err := os.Create("tekopia.log")
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	fmt.Println("Start Date/Time : ", time.Now().Format(time.RFC850))
	file1.WriteString("Start Date/Time : ")
	file1.WriteString(time.Now().Format(time.RFC850))

	// Records obsolete after the upgrade
	if err = getobsrec(db); err != nil {
		fmt.Println(err)
		return
	}

	// Fields obsolete after the upgrade
	if err = getobsfld(db); err != nil {
		fmt.Println(err)
		return
	}

	// New tables, views, derived work records and subrecords - No search for refs
	if err = getnewrec(db); err != nil {
		fmt.Println(err)
		return
	}

	// New fields added to existing tables - Search for refs to records. Impacts updates and inserts.
	if err = getnewfld(db); err != nil {
		fmt.Println(err)
		return
	}

	// Records now views
	if err = getrecnowvw(db); err != nil {
		fmt.Println(err)
		return
	}

	// Views now records
	if err = getvwnowrec(db); err != nil {
		fmt.Println(err)
		return
	}

	// Changed field lengths
	if err = gettrcfld(db); err != nil {
		fmt.Println(err)
		return
	}

	// Renamed objects
	if err = getrenobj1(db); err != nil {
		fmt.Println(err)
		return
	}

	if err = getrenobj2(db); err != nil {
		fmt.Println(err)
		return
	}

	// Mode 3 runs audit for online objects
	if mode == 3 || mode == 4 {
		// Print Summary
		if err = prtsummary(db); err != nil {
			fmt.Println(err)
			return
		}
		if err = prtdetail1(db); err != nil {
			fmt.Println(err)
			return
		}
		if err = prtdetail2(db); err != nil {
			fmt.Println(err)
			return
		}
	}

	if mode == 2 || mode == 4 {
		if err = prtsqrs(db); err != nil {
			fmt.Println(err)
			return
		}
	}

	file2, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file2.Close()
	file2.WriteString("\nEnd Date/Time : ")
	file2.WriteString(time.Now().Format(time.RFC850))

}

func getDSN() string {
	var dsn string
	if len(os.Args) > 1 {
		dsn = os.Args[1]
		if dsn != "" {
			return dsn
		}
	}
	dsn = os.Getenv("GO_OCI8_CONNECT_STRING")
	if dsn != "" {
		return dsn
	}
	fmt.Fprintln(os.Stderr, `Please specifiy connection parameter in GO_OCI8_CONNECT_STRING environment variable,
or as the first argument! (The format is user/name@host:port/sid)`)
	os.Exit(1)
	return ""
}

func prepDb(db *sql.DB) error {
	rows, err := db.Query("select instance from v$thread")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var f1 string
		rows.Scan(&f1)
		println(`Connected to`, f1)
	}

	_, err = db.Exec("create or replace procedure rcd_pad (a varchar2, b out number, c out varchar2) is begin select length(a), regexp_replace(a,'([[:alnum:]]{0})',ASCIISTR(CHR(0))) into b, c from dual; end;")
	if err != nil {
		return err
	} else {
		println(`Stored procedure rcd_pad created. Used to create PeopleCode search string.`)
	}

	_, err = db.Exec("declare c int; begin select count(1) into c from dba_tables where table_name = 'DBMS_OUTPUT'; if c = 1 then execute immediate 'drop table dbms_output'; end if; end;")
	if err != nil {
		return err
	} else {
		println(`Table DBMS_OUTPUT dropped`)
	}

	_, err = db.Exec("create table dbms_output (dbms_key varchar2(15) not null, dbms_seq number(38) not null, dbms_line clob not null) tablespace psdefault storage (initial 50000 next 50000 maxextents unlimited pctincrease 0) pctfree 10 pctused 80")
	if err != nil {
		return err
	} else {
		println(`Table DBMS_OUTPUT created for storing and printing dbms_output.put_line content from PL/SQL`)
	}

	_, err = db.Exec("declare c int; begin select count(1) into c from dba_tables where table_name = 'UPGRADE_AUDIT'; if c = 1 then execute immediate 'drop table upgrade_audit'; end if; end;")
	if err != nil {
		return err
	} else {
		println(`Table UPGRADE_AUDIT dropped`)
	}

	_, err = db.Exec("create table upgrade_audit (change_type varchar2(40), sqr_object varchar2(80), pcode_object varchar2(100), sql_object varchar2(100), query_object varchar2(100)) tablespace psdefault storage (initial 50000 next 50000 maxextents unlimited pctincrease 0) pctfree 10 pctused 80")
	if err != nil {
		return err
	} else {
		println(`Table UPGRADE_AUDIT created for analyzing overall impact`)
	}

	_, err = db.Exec("declare c int; begin select count(1) into c from dba_tables where table_name = 'UPGRADE_TOTALS'; if c = 1 then execute immediate 'drop table upgrade_totals'; end if; end;")
	if err != nil {
		return err
	} else {
		println(`Table UPGRADE_AUDIT dropped`)
	}

	_, err = db.Exec("create table upgrade_totals (change_type varchar2(40), pcode_object int, sql_object int, query_object int) tablespace psdefault storage (initial 50000 next 50000 maxextents unlimited pctincrease 0) pctfree 10 pctused 80")
	if err != nil {
		return err
	} else {
		println(`Table UPGRADE_TOTALS created for analyzing overall impact`)
	}

	return nil
}

func Execute(output_buffer *bytes.Buffer, stack ...*exec.Cmd) (err error) {
	var error_buffer bytes.Buffer
	pipe_stack := make([]*io.PipeWriter, len(stack)-1)
	i := 0
	for ; i < len(stack)-1; i++ {
		stdin_pipe, stdout_pipe := io.Pipe()
		stack[i].Stdout = stdout_pipe
		stack[i].Stderr = &error_buffer
		stack[i+1].Stdin = stdin_pipe
		pipe_stack[i] = stdout_pipe
	}
	stack[i].Stdout = output_buffer
	stack[i].Stderr = &error_buffer

	if err := call(stack, pipe_stack); err != nil {
		log.Fatalln(string(error_buffer.Bytes()), err)
	}
	return err
}

func call(stack []*exec.Cmd, pipes []*io.PipeWriter) (err error) {
	if stack[0].Process == nil {
		if err = stack[0].Start(); err != nil {
			return err
		}
	}
	if len(stack) > 1 {
		if err = stack[1].Start(); err != nil {
			return err
		}
		defer func() {
			if err == nil {
				pipes[0].Close()
				err = call(stack[1:], pipes[1:])
			}
		}()
	}
	return stack[0].Wait()
}

func srchsqrs(fp string, fi os.FileInfo, err error) error {
	db, err := sql.Open("oci8", getDSN())
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer db.Close()

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	if err != nil {
		fmt.Println(err) // can't walk here,
		return nil       // but continue walking elsewhere
	}
	if !!fi.IsDir() {
		return nil // not a file.  ignore.
	}
	matched, err := filepath.Match("*.sq?", fi.Name())
	if err != nil {
		fmt.Println(err) // malformed pattern
		return err       // this is fatal.
	}
	if matched {
		// fmt.Println(fp)
		file, err := os.Open(fp)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)

		lineNumber := 0
		for scanner.Scan() {
			lineNumber += 1
			line := strings.ToUpper(scanner.Text())
			if strings.Contains(line, strings.ToUpper(tblmtch)) && fldmtch == "None" {
				fmt.Println("lFound in SQR: ", fp)
				file1.WriteString("Found in SQR: ")
				file1.WriteString(fp)
				fmt.Printf("%d\t%s\n", lineNumber, strings.TrimSpace(line))
				file1.WriteString(" => line: ")
				file1.WriteString(strconv.Itoa(lineNumber))
				file1.WriteString(" - ")
				file1.WriteString(strings.TrimSpace(line))
				file1.WriteString("\n")
				if err = logsqrs(db, fp); err != nil {
					fmt.Println(err)
					return err
				}
			} else {
				if strings.Contains(line, strings.ToUpper(fldmtch)) && strings.Contains(line, strings.ToUpper(tblmtch)) {
					fmt.Println("Found in SQR: ", fp)
					file1.WriteString("Found in SQR: ")
					file1.WriteString(fp)
					fmt.Printf("%d\t%s\n", lineNumber, strings.TrimSpace(line))
					file1.WriteString(" => line: ")
					file1.WriteString(strconv.Itoa(lineNumber))
					file1.WriteString(" - ")
					file1.WriteString(strings.TrimSpace(line))
					file1.WriteString("\n")
					if err = logsqrs(db, fp); err != nil {
						fmt.Println(err)
						return err
					}

				} // if fldmtch
			} // if tblmtch
		} // for scanner
	} // if matched

	return nil
}

func logsqrs(db *sql.DB, fp string) error {

	stmt, err := db.Prepare("insert into upgrade_audit(change_type, sqr_object, pcode_object, sql_object, query_object) values (:cfrom, :fp, null, null, null)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query(cfrom, fp)
	if err != nil {
		return err
	}
	defer rows.Close()
	return nil
}

func getobsrec(db *sql.DB) error {
	// Find obsolete records

	// upgradeaction 3 = CopyProp
	// sourcestatus 1 = Absent
	// Create database link to old demo to obtain rectype

	cfrom = "Get-Obsolete-Records"

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	fmt.Print("\nThe following records are obsolete after the upgrade :\n")
	file1.WriteString("\n\nThe following records are obsolete after the upgrade :\n")

	stmt, err := db.Prepare("select aa.objectvalue1, mm.rectype from psprojectitem aa, psrecdefn@HRDMO91 mm where aa.objecttype = 0 and aa.objectid1 = 1 and aa.sourcestatus ^= aa.targetstatus and aa.upgradeaction ^= 3 and aa.sourcestatus = 1 and aa.objectvalue2 = ' ' and substr(aa.objectvalue1,1,15) = mm.recname and aa.projectname = :upgrade order by 1")
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query(upgrade)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var o1 string
		var o2 string
		rows.Scan(&o1, &o2)
		switch o2 {
		case "0":
			fmt.Println(o1, "- Obsolete Table")
			file1.WriteString(o1)
			file1.WriteString(" - Obsolete Table\n")
		case "1":
			fmt.Println(o1, "- Obsolete View")
			file1.WriteString(o1)
			file1.WriteString(" - Obsolete View\n")
		case "2":
			fmt.Println(o1, "- Obsolete Derived/Work Record")
			file1.WriteString(o1)
			file1.WriteString(" - Obsolete Derived/Work Record\n")
		case "3":
			fmt.Println(o1, "- Obsolete SubRecord")
			file1.WriteString(o1)
			file1.WriteString(" - Obsolete SubRecord\n")
		case "5":
			fmt.Println(o1, "- Obsolete Dynamic View")
			file1.WriteString(o1)
			file1.WriteString(" - Obsolete Dynamic View\n")
		case "6":
			fmt.Println(o1, "- Obsolete Query View")
			file1.WriteString(o1)
			file1.WriteString(" - Obsolete Query View\n")
		case "7":
			fmt.Println(o1, "- Obsolete Temporary Table")
			file1.WriteString(o1)
			file1.WriteString(" - Obsolete Temporary Table\n")
		default:
			fmt.Println(o1, "- Unknown Record Type")
			file1.WriteString(o1)
			file1.WriteString(" - Unknown Record Type")
		}

		if 3 <= mode && mode <= 4 {

			if err = srchsql(db, rid, o1, "None", cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchpcode(db, rid, o1, "None", cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchqryrec(db, rid, o1, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchqryfld(db, rid, o1, "None", cfrom); err != nil {
				fmt.Println(err)
				return err
			}
		} // end mode

		tblmtch = "PS_" + o1
		fldmtch = "None"
		// Mode 2 runs report only for SQRs
		// Mode 4 runs full report
		if mode == 2 || mode == 4 {
			//SQRs
			filepath.Walk(searchdir, srchsqrs)
		}

		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func getobsfld(db *sql.DB) error {
	// Find obsolete fields

	// upgradeaction 3 = CopyProp

	cfrom = "Get-Obsolete-Fields"

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	fmt.Print("\nThe following fields are obsolete after the upgrade:\n")
	file1.WriteString("\nThe following fields are obsolete after the upgrade:\n")

	stmt, err := db.Prepare("select o.objectvalue1, o.objectvalue2, kk.rectype from psprojectitem o, psrecdefn kk where o.objecttype = 0 and o.objectid1 = 1 and sourcestatus ^= targetstatus and upgradeaction ^= 3 and sourcestatus = 1 and o.objectvalue2 ^= ' ' and o.objectvalue1 = kk.recname and o.projectname = :upgrade order by 1")
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query(upgrade)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var o1 string
		var o2 string
		var o3 string
		rows.Scan(&o1, &o2, &o3)
		switch o3 {
		case "0":
			fmt.Println(o1, ".", o2, "- Field removed from Table")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field removed from Table\n")
		case "1":
			fmt.Println(o1, ".", o2, "- Field removed from View")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field removed from View\n")
		case "2":
			fmt.Println(o1, ".", o2, "- Field removed from Derived/Work Record")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field removed from Derived/Work Record\n")
		case "3":
			fmt.Println(o1, ".", o2, "- Field removed from SubRecord")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field removed from SubRecord\n")
		case "5":
			fmt.Println(o1, ".", o2, "- Field removed from Dynamic View")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field removed from Dynamic View\n")
		case "6":
			fmt.Println(o1, ".", o2, "- Field removed from Query View")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field removed from Query View\n")
		case "7":
			fmt.Println(o1, ".", o2, "- Field removed from Temporary Table")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field removed from Temporary Table\n")
		default:
			fmt.Println(o1, ".", o2, "- Unknown Field Type")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Unknown Field Type\n")
		}

		if 3 <= mode && mode <= 4 {

			if err = srchsql(db, rid, o1, o2, cfrom); err != nil { // In case bolt-on SQL uses obsolete fields
				fmt.Println(err)
				return err
			}
			if err = srchpcode(db, rid, o1, o2, cfrom); err != nil { // In case bolt-on PCode uses obsolete fields
				fmt.Println(err)
				return err
			}
			if err = srchqryrec(db, rid, o1, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchqryfld(db, rid, o1, o2, cfrom); err != nil { // Assuming all custom queries were copied during upgrade
				fmt.Println(err)
				return err
			}
		} // end mode

		tblmtch = "PS_" + o1
		fldmtch = o2
		// Mode 2 runs report only for SQRs
		// Mode 4 runs full report
		if mode == 2 || mode == 4 {
			//SQRs
			filepath.Walk(searchdir, srchsqrs)
		}

		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func getnewrec(db *sql.DB) error {
	// Find New Records

	// upgradeaction 3 = CopyProp
	// sourcestatus 4 = *Changed
	// sourcestatus 5 = *Unchanged

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	fmt.Print("\nThe following new tables, views, derived work records and subrecords were delivered\n (Note: Renamed objects are listed in a separate section) :\n")

	stmt, err := db.Prepare("select n.objectvalue1, p.rectype from psprojectitem n, psrecdefn p where n.objecttype = 0 and n.objectid1 = 1 and n.sourcestatus ^= n.targetstatus and upgradeaction ^= 3 and n.sourcestatus not in (4,5) and n.targetstatus = 1 and n.objectvalue2 = ' ' and substr(n.objectvalue1,1,15) = p.recname and n.objectvalue1 not in (select newname from psobjchng where enttype = 'R') and n.projectname = :upgrade order by 1")
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query(upgrade)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var o1 string
		var o2 string
		rows.Scan(&o1, &o2)
		switch o2 {
		case "0":
			fmt.Println(o1, "- New Table")
			file1.WriteString(o1)
			file1.WriteString(" - New Table\n")
		case "1":
			fmt.Println(o1, "- New View")
			file1.WriteString(o1)
			file1.WriteString(" - New View\n")
		case "2":
			fmt.Println(o1, "- New Derived/Work Record")
			file1.WriteString(o1)
			file1.WriteString(" - New Derived/Work Record\n")
		case "3":
			fmt.Println(o1, "- New Subrecord")
			file1.WriteString(o1)
			file1.WriteString(" - New Subrecord\n")
		case "5":
			fmt.Println(o1, "- New Dynamic View")
			file1.WriteString(o1)
			file1.WriteString(" - New Dynamic View\n")
		case "6":
			fmt.Println(o1, "- New Query View")
			file1.WriteString(o1)
			file1.WriteString(" - New Query View\n")
		case "7":
			fmt.Println(o1, "- New Temporary Table")
			file1.WriteString(o1)
			file1.WriteString(" - New Temporary Table\n")
		default:
			fmt.Println(o1, "- Unknown Record Type")
			file1.WriteString(o1)
			file1.WriteString(" - Unknown Record Type\n")
		}
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func getnewfld(db *sql.DB) error {
	// Find New Fields

	cfrom = "Get-New-Fields"

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	fmt.Print("\nThe following fields were added to existing tables\n (Note: Renamed fields are listed in a separate section) :\n")
	file1.WriteString("\nThe following fields were added to existing tables\n (Note: Renamed fields are listed in a separate section) :\n")

	stmt, err := db.Prepare("select z.objectvalue1, z.objectvalue2, pp.rectype from psprojectitem z, psrecdefn pp where z.objecttype = 0 and z.objectid1 = 1 and z.sourcestatus ^= z.targetstatus and z.upgradeaction ^= 3 and z.targetstatus = 1 and z.objectvalue2 ^= ' ' and substr(z.objectvalue1,1,15) = pp.recname and z.objectvalue2 not in (select qq.newname from psobjchng qq where qq.enttype = '3' and z.objectvalue1 = qq.oldname2) and z.projectname = :upgrade order by 1,2")
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query(upgrade)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var o1 string
		var o2 string
		var o3 string
		rows.Scan(&o1, &o2, &o3)
		switch o3 {
		case "0":
			fmt.Println(o1, ".", o2, "- Field added to Table")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field added to Table\n")
		case "1":
			fmt.Println(o1, ".", o2, "- Field added to View")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field added to View\n")
		case "2":
			fmt.Println(o1, ".", o2, "- Field added to Derived/Work Record")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field added to Derived/Work Record\n")
		case "3":
			fmt.Println(o1, ".", o2, " - Field added to SubRecord")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field added to SubRecord\n")
		case "5":
			fmt.Println(o1, ".", o2, " - Field added to Dynamic View")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field added to Dynamic View\n")
		case "6":
			fmt.Println(o1, ".", o2, " - Field added to Query View")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field added to Query View\n")
		case "7":
			fmt.Println(o1, ".", o2, " - Field added to Temporary Table")
			file1.WriteString(o1)
			file1.WriteString(".")
			file1.WriteString(o2)
			file1.WriteString(" - Field added to Temporary Table\n")
		}

		if 3 <= mode && mode <= 4 {
			if err = srchsql(db, rid, o1, o2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchpcode(db, rid, o1, o2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchqryrec(db, rid, o1, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchqryfld(db, rid, o1, o2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
		} // end mode

		tblmtch = "PS_" + o1
		fldmtch = "None"
		// Mode 2 runs report only for SQRs
		// Mode 4 runs full report
		if mode == 2 || mode == 4 {
			//SQRs
			filepath.Walk(searchdir, srchsqrs)
		}

		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func getrecnowvw(db *sql.DB) error {
	// Find Records now Views

	// upgradeaction 3 = CopyProp
	// sourcestatus 1 = Absent
	// Create database link to old demo to obtain rectype

	cfrom = "Get-Records-Now-Views"
	o2 := "None"

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	fmt.Print("\nThe following records (old release) have been changed to views (new release) :\n")
	file1.WriteString("\nThe following records (old release) have been changed to views (new release) :\n")

	stmt, err := db.Prepare("select xx.recname from psrecdefn xx, psrecdefn@HRDMO91 yy where xx.recname = yy.recname and xx.rectype = 1 and yy.rectype = 0 order by 1")
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var o1 string
		rows.Scan(&o1)
		println(o1)
		file1.WriteString(o1)

		if 3 <= mode && mode <= 4 {
			if err = srchsql(db, rid, o1, o2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchpcode(db, rid, o1, o2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchqryrec(db, rid, o1, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchqryfld(db, rid, o1, o2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
		} // end mode

		tblmtch = "PS_" + o1
		fldmtch = "None"
		// Mode 2 runs report only for SQRs
		// Mode 4 runs full report
		if mode == 2 || mode == 4 {
			//SQRs
			filepath.Walk(searchdir, srchsqrs)
		}

		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func getvwnowrec(db *sql.DB) error {
	// Find Views now Records

	// upgradeaction 3 = CopyProp
	// sourcestatus 1 = Absent
	// Create database link to old demo to obtain rectype

	cfrom = "Get-Views-Now-Records"
	o2 := "None"

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	fmt.Print("\nThe following views (old release) have been changed to records (new release) :\n")
	file1.WriteString("\nThe following views (old release) have been changed to records (new release) :\n")

	stmt, err := db.Prepare("select ww.recname from psrecdefn ww, psrecdefn@HRDMO91 jj where ww.recname = jj.recname and ww.rectype = 0 and jj.rectype = 1 order by 1")
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var o1 string
		rows.Scan(&o1)
		println(o1)
		file1.WriteString(o1)

		if 3 <= mode && mode <= 4 {
			if err = srchsql(db, rid, o1, o2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchpcode(db, rid, o1, o2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchqryrec(db, rid, o1, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchqryfld(db, rid, o1, o2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
		} // end mode

		tblmtch = "PS_" + o1
		fldmtch = "None"
		// Mode 2 runs report only for SQRs
		// Mode 4 runs full report
		if mode == 2 || mode == 4 {
			//SQRs
			filepath.Walk(searchdir, srchsqrs)
		}

		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func gettrcfld(db *sql.DB) error {
	// Find field length changes

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	fmt.Print("\nThe following field lenghts have changed :\n")
	file1.WriteString("\nThe following field lengths have changed :\n")

	stmt, err := db.Prepare("select t1.fieldname, t1.length, t2.length from psdbfield t1, psdbfield@HRDMO91 t2 where t1.fieldname = t2.fieldname and t1.fieldtype = t2.fieldtype and t1.length < t2.length and t1.fieldtype in (2,3) order by 1")
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var o1, o2, o3 string
		rows.Scan(&o1, &o2, &o3)
		println(o1, ` - Changed from `, o2, ` to `, o3)
		file1.WriteString(o1)
		file1.WriteString(" - Changed from ")
		file1.WriteString(o2)
		file1.WriteString(" to ")
		file1.WriteString(o3)
		file1.WriteString("\n")
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func getrenobj1(db *sql.DB) error {

	cfrom = "Get-Renamed-Records"

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	fmt.Print("\nThe following objects have been renamed :\n")
	file1.WriteString("\nThe following objects have been renamed :\n")

	stmt, err := db.Prepare("select i.oldname, i.newname from psobjchng i where i.enttype = 'R' order by 1")
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var o1, o2 string
		rows.Scan(&o1, &o2)
		println(`Record `, o1, `renamed to `, o2)
		file1.WriteString("Record ")
		file1.WriteString(o1)
		file1.WriteString(" renamed to ")
		file1.WriteString(o2)
		file1.WriteString("\n")

		if 3 <= mode && mode <= 4 {
			if err = srchsql(db, rid, o1, o2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchpcode(db, rid, o1, o2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchqryrec(db, rid, o1, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchqryfld(db, rid, o1, o2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
		} // end mode

		tblmtch = "PS_" + o1
		fldmtch = "None"
		// Mode 2 runs report only for SQRs
		// Mode 4 runs full report
		if mode == 2 || mode == 4 {
			//SQRs
			filepath.Walk(searchdir, srchsqrs)
		}

		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func getrenobj2(db *sql.DB) error {

	cfrom = "Get-Renamed-Objects"

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	stmt, err := db.Prepare("select ab.oldname, ab.oldname2, ab.newname from psobjchng ab where ab.enttype = '3' order by 2,3")
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var k1, k2, k3 string
		rows.Scan(&k1, &k2, &k3)
		println(`Field: `, k1, `.`, k2, `renamed to `, k1, `.`, k3)
		file1.WriteString("Field: ")
		file1.WriteString(k1)
		file1.WriteString(".")
		file1.WriteString(k2)
		file1.WriteString(" renamed to ")
		file1.WriteString(k1)
		file1.WriteString(".")
		file1.WriteString(k3)
		file1.WriteString("\n")

		if 3 <= mode && mode <= 4 {
			if err = srchsql(db, rid, k1, k2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchpcode(db, rid, k1, k2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchqryrec(db, rid, k1, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
			if err = srchqryfld(db, rid, k1, k2, cfrom); err != nil {
				fmt.Println(err)
				return err
			}
		} // end mode

		tblmtch = "PS_" + k1
		fldmtch = k2
		// Mode 2 runs report only for SQRs
		// Mode 4 runs full report
		if mode == 2 || mode == 4 {
			//SQRs
			filepath.Walk(searchdir, srchsqrs)
		}

		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func srchsql(db *sql.DB, reportid, rec, col, calledfrom string) error {

	// Only searches for the SQL id the sqlid exists in the project that contains custom objects (UPGCUST) created during the initial upgrade

	stmt, err := db.Prepare("declare cursor b is select sqlid, case sqltype when '0' then 'Other' when '1' then 'App Engine' when '2' then 'View' end sqltype, sqltext from pssqltextdefn where sqlid in (select objectvalue1 from psprojectitem where projectname = :upgcust and sqlid = objectvalue1 and objecttype = 30) order by 1; type psql_b is table of b%rowtype; coll_b psql_b; cnt_psql integer := 0; w_key dbms_output.dbms_key%type; w_seq dbms_output.dbms_seq%type; w_line dbms_output.dbms_line%type; procedure put_dbms_output (i_dbms_key varchar2, i_dbms_seq number, i_dbms_line clob) is begin insert into dbms_output (dbms_key, dbms_seq, dbms_line) values (i_dbms_key, i_dbms_seq, i_dbms_line); end put_dbms_output; procedure delete_dbms_output (d_dbms_key varchar2) is begin delete dbms_output where dbms_key = d_dbms_key; commit; end delete_dbms_output; begin w_key := :reportid; w_seq := 0; delete_dbms_output(w_key); open b; loop fetch b bulk collect into coll_b; exit when b%notfound; end loop; for i in coll_b.first .. coll_b.last loop if dbms_lob.instr(coll_b(i).sqltext,:rec) > 0 then if :col ^= 'None' then if dbms_lob.instr(coll_b(i).sqltext,:col) > 0 then w_line := '            Found in SQL: ' || coll_b(i).sqlid || ' - ' || coll_b(i).sqltype || ' - '  || coll_b(i).sqltext; put_dbms_output (w_key,w_seq,w_line); insert into upgrade_audit (change_type, sqr_object, pcode_object, sql_object, query_object) values (:calledfrom, null, null, coll_b(i).sqlid || ' - ' || coll_b(i).sqltype, null); end if; else w_line := '            Found in SQL: ' || coll_b(i).sqlid || ' - ' || coll_b(i).sqltype || ' - ' || coll_b(i).sqltext; put_dbms_output (w_key,w_seq,w_line); insert into upgrade_audit (change_type, sqr_object, pcode_object, sql_object, query_object) values (:calledfrom, null, null, coll_b(i).sqlid || ' - ' || coll_b(i).sqltype, null); end if; end if; cnt_psql := cnt_psql+1; w_seq := cnt_psql; end loop; close b; commit; end;")

	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query(upgcust, reportid, rec, col, calledfrom)
	if err != nil {
		return err
	}
	defer rows.Close()
	if err = prtdbmsout(db, reportid); err != nil {
		fmt.Println(err)
		return err
	}
	return rows.Err()

}

func srchpcode(db *sql.DB, reportid, rec, col, calledfrom string) error {

	// Only searches for the PCode if it exists in the project that contains custom objects (UPGCUST) created during the initial upgrade
	// objecttype 43 = App Engine PeopleCode
	// objecttype 58 = App Package PeopleCode
	// objecttype 46 = Component PeopleCode
	// objecttype 48 = Component Rec Fld PeopleCode
	// objecttype 47 = Component Record PeopleCode
	// objecttype 44 = Page PeopleCode
	// objecttype 8 = Record PeopleCode
	// Note: Could alternatively search pctext CLOB on pspcmtxt
	stmt, err := db.Prepare("declare cursor a is select m.objectvalue1, m.objectvalue2, m.objectvalue3, m.objectvalue6, m.objectvalue7, m.progtxt from pspcmprog m where m.objectvalue1 in (select objectvalue1 from psprojectitem where projectname = :upgcust and objecttype in (8,43,44,46,47,48,58)) order by 1,2,3,4; type pcode_a is table of a%rowtype; coll_a pcode_a; cnt_pcode integer := 0; w_key dbms_output.dbms_key%type; w_seq dbms_output.dbms_seq%type; w_line dbms_output.dbms_line%type; dd varchar2(60); ee varchar2(60); cc number; ff number; plsql_block varchar2(100); procedure put_dbms_output (i_dbms_key varchar2, i_dbms_seq number, i_dbms_line clob) is begin insert into dbms_output (dbms_key, dbms_seq, dbms_line) values (i_dbms_key, i_dbms_seq, i_dbms_line); end put_dbms_output; procedure delete_dbms_output (d_dbms_key varchar2) is begin delete dbms_output where dbms_key = d_dbms_key; commit; end delete_dbms_output; begin w_key := :reportid; w_seq := 0; delete_dbms_output(w_key); plsql_block := 'begin rcd_pad(:zz,:yy,:xx); end;'; execute immediate plsql_block using :rec, out cc, out dd; open a; loop fetch a bulk collect into coll_a; exit when a%notfound; end loop; for i in coll_a.first .. coll_a.last loop if dbms_lob.instr(coll_a(i).progtxt,utl_raw.cast_to_raw(substr(dd,2,(cc*2)-1))) > 0 then if :col ^= 'None' then plsql_block := 'begin rcd_pad(:zz,:yy,:xx); end;'; execute immediate plsql_block using :col, out ff, out ee; if dbms_lob.instr(coll_a(i).progtxt,utl_raw.cast_to_raw(substr(ee,2,(ff*2)-1))) > 0 then w_line := '            Found in PCode: ' || coll_a(i).objectvalue1 || ' ' || coll_a(i).objectvalue2 || ' ' || coll_a(i).objectvalue3 || ' ' || coll_a(i).objectvalue6 || ' ' || coll_a(i).objectvalue7; put_dbms_output (w_key,w_seq,w_line); insert into upgrade_audit (change_type, sqr_object, pcode_object, sql_object, query_object) values (:calledfrom, null, coll_a(i).objectvalue1 || ' ' || coll_a(i).objectvalue2 || ' ' || coll_a(i).objectvalue3 || ' ' || coll_a(i).objectvalue6 || ' ' || coll_a(i).objectvalue7, null, null); end if; else w_line := '            Found in PCode: ' || coll_a(i).objectvalue1 || ' ' || coll_a(i).objectvalue2 || ' ' || coll_a(i).objectvalue3 || ' ' || coll_a(i).objectvalue6 || ' ' || coll_a(i).objectvalue7; put_dbms_output (w_key,w_seq,w_line); insert into upgrade_audit (change_type, sqr_object, pcode_object, sql_object, query_object) values (:calledfrom, null, coll_a(i).objectvalue1 || ' ' || coll_a(i).objectvalue2 || ' ' || coll_a(i).objectvalue3 || ' ' || coll_a(i).objectvalue6 || ' ' || coll_a(i).objectvalue7, null, null); end if; end if; cnt_pcode := cnt_pcode+1; w_seq := cnt_pcode; end loop; close a; commit; end;")

	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query(upgcust, reportid, rec, col, calledfrom)
	if err != nil {
		return err
	}
	defer rows.Close()
	if err = prtdbmsout(db, reportid); err != nil {
		fmt.Println(err)
		return err
	}

	return rows.Err()
}

func srchqryrec(db *sql.DB, reportid, rec, calledfrom string) error {

	// Only searches for the Query if it exists in the project that contains custom objects (UPGCUST) created during the initial upgrade

	stmt, err := db.Prepare("declare cursor c is select distinct oprid, qryname, recname from psqryrecord where (oprid, qryname) in (select objectvalue2, objectvalue1 from psprojectitem where projectname = :upgcust and objecttype = 10 and oprid = objectvalue2 and qryname = objectvalue1) order by 2; type pqry_c is table of c%rowtype; coll_c pqry_c; cnt_pqry integer := 0; w_key dbms_output.dbms_key%type; w_seq dbms_output.dbms_seq%type; w_line dbms_output.dbms_line%type; procedure put_dbms_output (i_dbms_key varchar2, i_dbms_seq number, i_dbms_line clob) is begin insert into dbms_output (dbms_key, dbms_seq, dbms_line) values (i_dbms_key, i_dbms_seq, i_dbms_line); end put_dbms_output; procedure delete_dbms_output (d_dbms_key varchar2) is begin delete dbms_output where dbms_key = d_dbms_key; commit; end delete_dbms_output; begin w_key := :reportid; w_seq := 0; delete_dbms_output(w_key); open c; loop fetch c bulk collect into coll_c; exit when c%notfound; end loop; for i in coll_c.first .. coll_c.last loop if substr(coll_c(i).recname,1,30) = :rec then if coll_c(i).oprid ^= ' ' then w_line := '            Found in Query: ' || coll_c(i).qryname || ' : ' || coll_c(i).oprid; insert into upgrade_audit (change_type, sqr_object, pcode_object, sql_object, query_object) values (:calledfrom, null, null, null, coll_c(i).qryname || ' : ' || coll_c(i).oprid); else w_line := '            Found in Query: ' || coll_c(i).qryname; insert into upgrade_audit (change_type, sqr_object, pcode_object, sql_object, query_object) values (:calledfrom, null, null, null, coll_c(i).qryname); end if; put_dbms_output (w_key,w_seq,w_line); end if; cnt_pqry := cnt_pqry+1; w_seq := cnt_pqry; end loop; close c; commit; end;")

	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query(upgcust, reportid, rec, calledfrom)
	if err != nil {
		return err
	}
	defer rows.Close()
	if err = prtdbmsout(db, reportid); err != nil {
		fmt.Println(err)
		return err
	}
	return rows.Err()

}

func srchqryfld(db *sql.DB, reportid, rec, col, calledfrom string) error {

	// Only searches for the Query if it exists in the project that contains custom objects (UPGCUST) created during the initial upgrade

	stmt, err := db.Prepare("declare cursor c is select distinct oprid, qryname, recname, fieldname from psqryfield where (oprid, qryname) in (select objectvalue2, objectvalue1 from psprojectitem where projectname = :upgcust and objecttype = 10 and oprid = objectvalue2 and qryname = objectvalue1) order by 2; type pqry_c is table of c%rowtype; coll_c pqry_c; cnt_pqry integer := 0; w_key dbms_output.dbms_key%type; w_seq dbms_output.dbms_seq%type; w_line dbms_output.dbms_line%type; procedure put_dbms_output (i_dbms_key varchar2, i_dbms_seq number, i_dbms_line clob) is begin insert into dbms_output (dbms_key, dbms_seq, dbms_line) values (i_dbms_key, i_dbms_seq, i_dbms_line); end put_dbms_output; procedure delete_dbms_output (d_dbms_key varchar2) is begin delete dbms_output where dbms_key = d_dbms_key; commit; end delete_dbms_output; begin w_key := :reportid; w_seq := 0; delete_dbms_output(w_key); open c; loop fetch c bulk collect into coll_c; exit when c%notfound; end loop; for i in coll_c.first .. coll_c.last loop if substr(coll_c(i).fieldname,1,30) = :col and substr(coll_c(i).recname,1,30) = :rec then if coll_c(i).oprid ^= ' ' then w_line := '            Found in Query: ' || coll_c(i).qryname || ' : ' || coll_c(i).oprid; insert into upgrade_audit (change_type, sqr_object, pcode_object, sql_object, query_object) values (:calledfrom, null, null, null, coll_c(i).qryname || ' : ' || coll_c(i).oprid); else w_line := '            Found in Query: ' || coll_c(i).qryname; insert into upgrade_audit (change_type, sqr_object, pcode_object, sql_object, query_object) values (:calledfrom, null, null, null, coll_c(i).qryname); end if; put_dbms_output (w_key,w_seq,w_line); end if; cnt_pqry := cnt_pqry+1; w_seq := cnt_pqry; end loop; close c; commit; end;")

	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query(upgcust, reportid, rec, col, calledfrom)
	if err != nil {
		return err
	}
	defer rows.Close()
	if err = prtdbmsout(db, reportid); err != nil {
		fmt.Println(err)
		return err
	}
	return rows.Err()

}

// Print the dbms_output results
func prtdbmsout(db *sql.DB, reportid string) error {

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	stmt, err := db.Prepare("select u.dbms_key, u.dbms_seq, u.dbms_line from dbms_output u where u.dbms_key = :reportid order by 1,2")
	if err != nil {
		return err
	}
	rows, err := stmt.Query(reportid)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var d1, d2, d3 string
		rows.Scan(&d1, &d2, &d3)
		println(d3)
		file1.WriteString(d3)
		file1.WriteString("\n")
	}
	return rows.Err()
}

// Print summary of findings
func prtsummary(db *sql.DB) error {

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()
	println("Impact Analysis - Summary:")
	file1.WriteString("\nImpact Analysis - Summary:\n")

	stmt, err := db.Prepare("select (select count(distinct pcode_object) from upgrade_audit where pcode_object is not null) cntpcode, (select count(distinct sql_object) from upgrade_audit where sql_object is not null) cntsql, (select count(distinct query_object) from upgrade_audit where query_object is not null) cntqry from dual")
	if err != nil {
		return err
	}
	rows, err := stmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var c1, c2, c3 int
		rows.Scan(&c1, &c2, &c3)
		println(c1, " PeopleCode objects are impacted by changes in the new software release.")
		file1.WriteString(strconv.Itoa(c1))
		file1.WriteString(" PeopleCode objects are impacted by changes in the new software release.\n")
		println(c2, " SQL objects are impacted by changes in the new software release.")
		file1.WriteString(strconv.Itoa(c2))
		file1.WriteString(" SQL objects are impacted by changes in the new software release.\n")
		println(c3, " Queries are impacted by changes in the new software release.")
		file1.WriteString(strconv.Itoa(c3))
		file1.WriteString(" Queries are impacted by changes in the new software release.\n")
	}
	return rows.Err()
}

// Print detail findings
func prtdetail1(db *sql.DB) error {

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	println("Impact Analysis - Detail:")
	file1.WriteString("\nImpact Analysis - Detail:\n")

	stmt, err := db.Prepare("select count(distinct query_object) cntprivqry from upgrade_audit where query_object like '%:%' and query_object is not null")
	if err != nil {
		return err
	}
	rows, err := stmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var c1 int
		rows.Scan(&c1)
		println(c1, " Private Queries are impacted by changes in the new software release.")
		file1.WriteString(strconv.Itoa(c1))
		file1.WriteString(" Private Queries are impacted by changes in the new software release.\n")
	}

	return rows.Err()
}

func prtdetail2(db *sql.DB) error {

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	_, err = db.Exec("merge into upgrade_totals a using (select change_type, count(distinct pcode_object) as pcode_object from upgrade_audit where pcode_object is not null group by change_type) b on (a.change_type = b.change_type) when matched then update set a.pcode_object = b.pcode_object when not matched then insert (a.change_type, a.pcode_object) values (b.change_type, b.pcode_object)")
	if err != nil {
		return err
	} else {
		println(`Analyzing impact on PeopleCode.`)
	}

	_, err = db.Exec("merge into upgrade_totals a using (select change_type, count(distinct sql_object) as sql_object from upgrade_audit where sql_object is not null group by change_type) b on (a.change_type = b.change_type) when matched then update set a.sql_object = b.sql_object when not matched then insert (a.change_type, a.sql_object) values (b.change_type, b.sql_object)")
	if err != nil {
		return err
	} else {
		println(`Analyzing impact on SQL.`)
	}

	_, err = db.Exec("merge into upgrade_totals a using (select change_type, count(distinct query_object) as query_object from upgrade_audit where query_object is not null group by change_type) b on (a.change_type = b.change_type) when matched then update set a.query_object = b.query_object when not matched then insert (a.change_type, a.query_object) values (b.change_type, b.query_object)")
	if err != nil {
		return err
	} else {
		println(`Analyzing impact on Queries.`)
	}

	_, err = db.Exec("update upgrade_totals set pcode_object = 0 where pcode_object is null")
	if err != nil {
		return err
	} else {
		println(`Completed PeopleCode impact analysis.`)
	}

	_, err = db.Exec("update upgrade_totals set sql_object = 0 where sql_object is null")
	if err != nil {
		return err
	} else {
		println(`Completed SQL impact analysis.`)
	}

	_, err = db.Exec("update upgrade_totals set query_object = 0 where query_object is null")
	if err != nil {
		return err
	} else {
		println(`Completed Query impact analysis.`)
	}

	println("Objects impacted by various type of changes:")
	file1.WriteString("\nObjects impacted by various type of changes:\n")

	stmt, err := db.Prepare("select change_type, pcode_object, sql_object, query_object from upgrade_totals")
	if err != nil {
		return err
	}
	rows, err := stmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var t1 string
		var c1, c2, c3 int
		rows.Scan(&t1, &c1, &c2, &c3)
		switch t1 {
		case "Get-New-Fields":
			fmt.Println("New fields added to existing tables => PCode: ", c1, "SQL: ", c2, "Queries:", c3)
			file1.WriteString("\nNew fields added to existing tables => PCode: ")
			file1.WriteString(strconv.Itoa(c1))
			file1.WriteString("\nNew fields added to existing tables => SQL: ")
			file1.WriteString(strconv.Itoa(c2))
			file1.WriteString("\nNew fields added to existing tables => Queries: ")
			file1.WriteString(strconv.Itoa(c3))
		case "Get-Obsolete-Fields":
			fmt.Println("Obsolete fields => PCode: ", c1, "SQL: ", c2, "Queries:", c3)
			file1.WriteString("\nObsolete fields => PCode: ")
			file1.WriteString(strconv.Itoa(c1))
			file1.WriteString("\nObsolete fields => SQL: ")
			file1.WriteString(strconv.Itoa(c2))
			file1.WriteString("\nObsolete fields => Queries: ")
			file1.WriteString(strconv.Itoa(c3))
		case "Get-Obsolete-Records":
			fmt.Println("Obsolete records => PCode: ", c1, "SQL: ", c2, "Queries:", c3)
			file1.WriteString("\nObsolete records => PCode: ")
			file1.WriteString(strconv.Itoa(c1))
			file1.WriteString("\nObsolete records => SQL: ")
			file1.WriteString(strconv.Itoa(c2))
			file1.WriteString("\nObsolete records => Queries: ")
			file1.WriteString(strconv.Itoa(c3))
		case "Get-Records-Now-Views":
			fmt.Println("Records now Views => PCode: ", c1, "SQL: ", c2, "Queries:", c3)
			file1.WriteString("\nRecords now Views => PCode: ")
			file1.WriteString(strconv.Itoa(c1))
			file1.WriteString("\nRecords now Views => SQL: ")
			file1.WriteString(strconv.Itoa(c2))
			file1.WriteString("\nRecords now Views => Queries: ")
			file1.WriteString(strconv.Itoa(c3))
		case "Get-Views-Now-Records":
			fmt.Println("Views now Records => PCode: ", c1, "SQL: ", c2, "Queries:", c3)
			file1.WriteString("\nViews now Records => PCode: ")
			file1.WriteString(strconv.Itoa(c1))
			file1.WriteString("\nViews now Records => SQL: ")
			file1.WriteString(strconv.Itoa(c2))
			file1.WriteString("\nViews now Records => Queries: ")
			file1.WriteString(strconv.Itoa(c3))
		case "Get-Renamed-Objects":
			fmt.Println("Renamed Objects => PCode: ", c1, "SQL: ", c2, "Queries:", c3)
			file1.WriteString("\nRenamed Objects => PCode: ")
			file1.WriteString(strconv.Itoa(c1))
			file1.WriteString("\nRenamed Objects => SQL: ")
			file1.WriteString(strconv.Itoa(c2))
			file1.WriteString("\nRenamed Objects => Queries: ")
			file1.WriteString(strconv.Itoa(c3))
		}
	}
	return rows.Err()
}

func walkpath(path string, f os.FileInfo, err error) error {

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	fmt.Printf("%s size %d k\n", path, f.Size()/1024)
	file1.WriteString(path)
	file1.WriteString(" - size ")
	file1.WriteString(strconv.FormatInt(f.Size()/1024, 10))
	file1.WriteString(" k\n")
	return nil
}

func prtsqrs(db *sql.DB) error {

	file1, err := os.OpenFile("tekopia.log", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	defer file1.Close()

	println("SQR Impact Analysis - Summary: ")
	file1.WriteString("\n\nSQR Impact Analysis - Summary: ")
	println("Number of custom SQRs found: ")
	file1.WriteString("\nNumber of custom SQRs found: ")
	files, _ := ioutil.ReadDir(searchdir)
	fmt.Println(len(files))
	file1.WriteString(strconv.Itoa(len(files)))

	println("Found the following custom SQRs:")
	file1.WriteString("\n\nFound the following custom SQRs: ")
	filepath.Walk(searchdir, walkpath)

	println("SQR Impact Analysis - Detail: ")
	file1.WriteString("\nSQR Impact Analysis - Detail: ")

	stmt, err := db.Prepare("select change_type, count(distinct sqr_object) cntsqr from upgrade_audit where sqr_object is not null group by change_type")
	if err != nil {
		return err
	}
	rows, err := stmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var t1 string
		var c1 int
		rows.Scan(&t1, &c1)
		switch t1 {
		case "Get-New-Fields":
			fmt.Println("New fields added to existing tables => ", c1)
			file1.WriteString("\nNew fields added to existing tables => ")
			file1.WriteString(strconv.Itoa(c1))
			file1.WriteString("\n")
		case "Get-Obsolete-Fields":
			fmt.Println("Obsolete fields => ", c1)
			file1.WriteString("\nObsolete fields => ")
			file1.WriteString(strconv.Itoa(c1))
			file1.WriteString("\n")
		case "Get-Obsolete-Records":
			fmt.Println("Obsolete records => ", c1)
			file1.WriteString("\nObsolete records => ")
			file1.WriteString(strconv.Itoa(c1))
			file1.WriteString("\n")
		case "Get-Records-Now-Views":
			fmt.Println("Records now Views => ", c1)
			file1.WriteString("\nRecords now Views => ")
			file1.WriteString(strconv.Itoa(c1))
			file1.WriteString("\n")
		case "Get-Views-Now-Records":
			fmt.Println("Views now Records => ", c1)
			file1.WriteString("\nViews now Records => ")
			file1.WriteString(strconv.Itoa(c1))
			file1.WriteString("\n")
		case "Get-Renamed-Objects":
			fmt.Println("Renamed Objects => ", c1)
			file1.WriteString("\nRenamed Objects => ")
			file1.WriteString(strconv.Itoa(c1))
			file1.WriteString("\n")
		}
	}
	return rows.Err()
}
