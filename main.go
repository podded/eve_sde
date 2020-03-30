package main

import (
	"bufio"
	"compress/bzip2"
	"database/sql"
	"fmt"
	"github.com/cheggaaa/pb/v3"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gobuffalo/envy"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	HashMD5URL            = "https://www.fuzzwork.co.uk/dump/mysql-latest.tar.bz2.md5"
	HashURL               = "https://www.fuzzwork.co.uk/dump/mysql-latest.tar.bz2"
	SDETempFileCompressed = "sde_dump.sql.tar.bz2"
	SDETempFile           = "sde_dump.sql"
)

var (
	client http.Client
)

func main() {

	mysqlAddress := envy.Get("DB_ADDR", "127.0.0.1")
	mysqlPortEnv := envy.Get("DB_PORT", "3306")
	mysqlUser := envy.Get("DB_USER", "root")
	mysqlPass := envy.Get("DB_PASS", "password")
	mysqlDB := envy.Get("DB_DATABASE", "sde")

	mysqlPort := 3306
	i, err := strconv.Atoi(mysqlPortEnv)
	if err == nil {
		mysqlPort = i
	}

	log.Println("Connecting to DB")

	uri := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", mysqlUser, mysqlPass, mysqlAddress, mysqlPort, mysqlDB)

	conn, err := sql.Open("mysql", uri)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer conn.Close()

	client = http.Client{
		Timeout: 30 * time.Second,
	}

	log.Println("Getting hash from fuzzworks")

	hash, err := getLatestHash()
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Fuzworks returned hash: %s\n", hash)

	stored, err := getStoredHash(conn)
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Stored hash: %s", stored)

	if stored == hash {
		log.Println("SDE is already up to date")
		return
	}

	log.Printf("Downloading SDE dump")

	written, err := downloadSDE()
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Downloaded %d bytes\n", written)

	log.Println("Decompressing dump")

	err = decompressSDE()
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Executing mysql to load file into database")


	err = loadDatabaseIntoSDE(mysqlAddress, mysqlPort, mysqlUser, mysqlPass, mysqlDB)
	if err != nil {
		log.Fatalln(err)
	}

	err = updateStoredHash(conn, hash)
	if err != nil {
		log.Fatalln(err)
	}
}

func getLatestHash() (string, error) {

	resp, err := client.Get(HashMD5URL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	hash := strings.Split(string(body), " ")[0]
	return hash, nil
}

func getStoredHash(db *sql.DB) (string, error) {

	statement, err := db.Prepare("SELECT hash from SDE_HASH LIMIT 1")
	if err != nil {
		if strings.Contains(err.Error(), "Table") && strings.Contains(err.Error(), "doesn't exist") {
			return "", nil
		} else {
			return "", err
		}
	}

	rows, err := statement.Query()
	if err != nil {
		return "", nil
	}

	var stored string

	for rows.Next() {
		err = rows.Scan(&stored)
	}

	if err != nil {
		return "", nil
	}

	return stored, nil

}

func downloadSDE() (int64, error) {

	resp, err := client.Get(HashURL)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	out, err := os.Create(SDETempFileCompressed)
	if err != nil {
		return 0, err
	}
	defer out.Close()

	b, err := io.Copy(out, resp.Body)
	if err != nil {
		return 0, err
	}

	return b, nil
}

func decompressSDE() error {

	f, err := os.Open(SDETempFileCompressed)
	if err != nil {
		return err
	}
	defer f.Close()

	fstat, err := f.Stat()
	if err != nil{
		return err
	}

	bar := pb.Full.Start64(fstat.Size())

	out, err := os.Create(SDETempFile)
	if err != nil {
		return err
	}
	defer out.Close()

	pr := bar.NewProxyReader(f)

	br := bufio.NewReader(pr)
	cr := bzip2.NewReader(br)

	bw := bufio.NewWriter(out)

	written, err := io.Copy(bw, cr)

	bar.Finish()

	log.Printf("Wrote %d bytes to uncompressed file\n", written)


	return nil

}

func loadDatabaseIntoSDE(address string, port int, user string, pass string, db string) (error) {

	command := fmt.Sprintf("mysql -h %s -P %d -u %s -p%s --binary-mode --force %s < %s", address, port, user, pass, db, SDETempFile)
	_, err := exec.Command("bash", "-c", command).Output()
	fmt.Printf("Executing command: %s", command)
	fmt.Println()
	return err
}

func updateStoredHash(db *sql.DB, hash string) (error) {

	_, err := db.Exec("DROP TABLE IF EXISTS SDE_HASH")

	if err != nil {
		return err
	}

	_, err = db.Exec("CREATE TABLE SDE_HASH(hash varchar(255))")
	if err != nil {
		return err
	}

	_, err = db.Exec("INSERT INTO SDE_HASH (hash) VALUES ($1)", hash)
	if err != nil {
		return err
	}

	return nil

}