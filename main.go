package main

import (
	"database/sql"
	"fmt"
	"log"
	_ "github.com/lib/pq"
)

const (
	dbHost     = "db"
	dbPort     = 5432
	dbUser     = "postgres"
	dbPassword = "postgres"
	dbName     = "testdb"
)

func main() {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("Database unreachable:", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
		id SERIAL PRIMARY KEY,
		name TEXT NOT NULL
	)`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	_, err = db.Exec(`INSERT INTO users (name) VALUES ($1)`, "Alice")
	if err != nil {
		log.Fatal("Failed to insert user:", err)
	}

	rows, err := db.Query(`SELECT id, name FROM users`)
	if err != nil {
		log.Fatal("Failed to query users:", err)
	}
	defer rows.Close()

	fmt.Println("Users:")
	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		fmt.Printf("ID: %d, Name: %s\n", id, name)
	}
}
