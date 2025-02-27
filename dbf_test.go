package dbf

import (
	"os"
	"testing"
	"time"
)

const tempdbf = "temp.dbf"

// TTable test table. Order of struct members is important.
type TTable struct {
	Boolean bool
	Text    string
	Int     int
	Float   float64
}

func TestNew(t *testing.T) {
	db := New()
	db.AddBoolField("boolean")
	db.AddTextField("text", 40)
	db.AddIntField("int", 10)
	db.AddFloatField("float", 8, 6)

	addRecord(t, db)
	checkCount(t, db, 1)
	addStruct(t, db)
	checkCount(t, db, 2)
	addRecord(t, db)
	checkCount(t, db, 3)
	addStruct(t, db)
	checkCount(t, db, 4)
	addStruct(t, db)
	checkCount(t, db, 5)

	updateRecord(t, db, 3)
	checkCount(t, db, 5)
	delRecord(t, db, 4)
	checkCount(t, db, 4)

	if err := db.SaveFile(tempdbf); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempdbf)

	dbload, err := LoadFile(tempdbf)
	if err != nil {
		t.Fatal(err)
	}
	checkCount(t, dbload, 4)
	addStruct(t, db)
	checkCount(t, db, 5)
}

func TestNewStruct(t *testing.T) {
	db := New()
	err := db.Create(TTable{})
	if err != nil {
		t.Fatal(err)
	}

	addRecord(t, db)
	checkCount(t, db, 1)
	addStruct(t, db)
	checkCount(t, db, 2)
	addRecord(t, db)
	checkCount(t, db, 3)
	addStruct(t, db)
	checkCount(t, db, 4)

	updateStruct(t, db, 1)
	checkCount(t, db, 4)
	delRecord(t, db, 2)
	checkCount(t, db, 3)
}

func TestOmitEmpty(t *testing.T) {
	temp, err := os.CreateTemp("", "test_dbf")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(temp.Name())
	temp.Close()

	type TestStruct struct {
		Name      string    `dbf:"NAME"`
		Age       int       `dbf:"AGE,omitempty"`
		Active    bool      `dbf:"ACTIVE,omitempty"`
		Rate      float64   `dbf:"RATE,omitempty"`
		CreatedAt time.Time `dbf:"CREATED,omitempty"`
	}

	// Create a new table
	dt := New()
	if err := dt.Create(TestStruct{}); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test with zero values
	zeroRecord := TestStruct{
		Name: "Zero Values",
		// Leave all omitempty fields as zero values
	}
	if _, err := dt.Append(zeroRecord); err != nil {
		t.Fatalf("Failed to append zero record: %v", err)
	}

	// Test with non-zero values
	fullRecord := TestStruct{
		Name:      "Full Record",
		Age:       30,
		Active:    true,
		Rate:      45.5,
		CreatedAt: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	if _, err := dt.Append(fullRecord); err != nil {
		t.Fatalf("Failed to append full record: %v", err)
	}

	// Test with some non-zero values
	partialRecord := TestStruct{
		Name:   "Partial Record",
		Age:    25,
		Active: true,
		// Rate and CreatedAt are zero values
	}
	if _, err := dt.Append(partialRecord); err != nil {
		t.Fatalf("Failed to append partial record: %v", err)
	}

	// Save the table
	if err := dt.SaveFile(temp.Name()); err != nil {
		t.Fatalf("Failed to save file: %v", err)
	}

	// Load the table back and verify
	loadedTable, err := LoadFile(temp.Name())
	if err != nil {
		t.Fatalf("Failed to load saved file: %v", err)
	}

	// Check values
	it := loadedTable.NewIterator()

	// Verify first record (zero values)
	if it.Next() {
		var record TestStruct
		if err := it.Read(&record); err != nil {
			t.Errorf("Failed to read first record: %v", err)
		}

		if record.Name != "Zero Values" {
			t.Errorf("Expected Name 'Zero Values', got %q", record.Name)
		}
		// Zero values should still be read as zeros
		if record.Age != 0 {
			t.Errorf("Expected Age 0, got %d", record.Age)
		}
		if record.Active != false {
			t.Errorf("Expected Active false, got %t", record.Active)
		}
		if record.Rate != 0 {
			t.Errorf("Expected Rate 0, got %f", record.Rate)
		}
		if !record.CreatedAt.IsZero() {
			t.Errorf("Expected CreatedAt zero, got %v", record.CreatedAt)
		}
	} else {
		t.Error("Failed to iterate to first record")
	}

	// Verify second record (full values)
	if it.Next() {
		var record TestStruct
		if err := it.Read(&record); err != nil {
			t.Errorf("Failed to read second record: %v", err)
		}

		if record.Name != "Full Record" {
			t.Errorf("Expected Name 'Full Record', got %q", record.Name)
		}
		if record.Age != 30 {
			t.Errorf("Expected Age 30, got %d", record.Age)
		}
		if record.Active != true {
			t.Errorf("Expected Active true, got %t", record.Active)
		}
		if record.Rate != 45.5 {
			t.Errorf("Expected Rate 45.5, got %f", record.Rate)
		}
		expectedDate := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
		if record.CreatedAt.Format("20060102") != expectedDate.Format("20060102") {
			t.Errorf("Expected CreatedAt %v, got %v", expectedDate, record.CreatedAt)
		}
	} else {
		t.Error("Failed to iterate to second record")
	}

	// Verify third record (partial values)
	if it.Next() {
		var record TestStruct
		if err := it.Read(&record); err != nil {
			t.Errorf("Failed to read third record: %v", err)
		}

		if record.Name != "Partial Record" {
			t.Errorf("Expected Name 'Partial Record', got %q", record.Name)
		}
		if record.Age != 25 {
			t.Errorf("Expected Age 25, got %d", record.Age)
		}
		if record.Active != true {
			t.Errorf("Expected Active true, got %t", record.Active)
		}
		// These should still be zero as they were omitted
		if record.Rate != 0 {
			t.Errorf("Expected Rate 0, got %f", record.Rate)
		}
		if !record.CreatedAt.IsZero() {
			t.Errorf("Expected CreatedAt zero, got %v", record.CreatedAt)
		}
	} else {
		t.Error("Failed to iterate to third record")
	}
}

func checkCount(t *testing.T, db *DbfTable, count int) {
	c := 0
	iter := db.NewIterator()
	for iter.Next() {
		c++
	}
	if c != count {
		t.Fatal("record count is wrong, expected", count, "found:", c)
	}
}

// delRecord delete row.
func delRecord(t *testing.T, db *DbfTable, row int) {
	db.Delete(row)
	if !db.IsDeleted(row) {
		t.Fatal("record should be deleted but it is not")
	}
}

// addRecord adds and then checks record.
func addRecord(t *testing.T, db *DbfTable) {
	row := db.AddRecord()
	//println("Row: ", row)
	db.SetFieldValueByName(row, "boolean", "t")
	db.SetFieldValueByName(row, "text", "message")
	db.SetFieldValueByName(row, "int", "44")
	db.SetFieldValueByName(row, "float", "44.123")

	arr := db.Row(row)
	if len(arr) != 4 {
		t.Fatal("record length is wrong expected 4 found:", len(arr))
	}
	if arr[0] != "t" {
		t.Fatal("record for boolean field expected 't' found:", arr[0])
	}
	if arr[1] != "message" {
		t.Fatal("expected 'message' found:", arr[1])
	}
	if arr[2] != "44" {
		t.Fatal("expected '44' found:", arr[2])
	}
	if arr[3] != "44.123" {
		t.Fatal("expected '44.123' found:", arr[3])
	}
}

// update one value in record.
func updateRecord(t *testing.T, db *DbfTable, row int) {
	nval := "123"
	db.SetFieldValue(row, 2, nval)
	v := db.FieldValue(row, 2)
	if v != nval {
		t.Fatal("update expected", nval, "found:", v)
	}
}

// update record using struct.
func updateStruct(t *testing.T, db *DbfTable, row int) {
	table := TTable{}
	db.Write(row, &TTable{Boolean: false, Text: "msgupdate", Int: 11, Float: 123.56})
	if err := db.Read(row, &table); err != nil {
		t.Fatal(err)
	}
	if table.Boolean != false {
		t.Fatal("TTable.Boolean must be false")
	}
	if table.Text != "msgupdate" {
		t.Fatal("TTable.Text expected to be 'msgupdate' found:", table.Text)
	}
	if table.Int != 11 {
		t.Fatal("TTable.Int expected to be '11' found:", table.Int)
	}
	if table.Float != 123.56 {
		t.Fatal("TTable.Float expected to be '123.56' found:", table.Float)
	}
}

// addStruct adds record using struct and checks it.
func addStruct(t *testing.T, db *DbfTable) {
	row := db.AddRecord()
	db.Write(row, TTable{Boolean: true, Text: "msg", Int: 33, Float: 44.34})

	table := TTable{}
	if err := db.Read(row, &table); err != nil {
		t.Fatal(err)
	}
	if table.Boolean != true {
		t.Fatal("TTable.Boolean must be true")
	}
	if table.Text != "msg" {
		t.Fatal("TTable.Text expected to be 'msg' found:", table.Text)
	}
	if table.Int != 33 {
		t.Fatal("TTable.Int expected to be '33' found:", table.Int)
	}
	if table.Float != 44.34 {
		t.Fatal("TTable.Float expected to be '44.34' found:", table.Float)
	}
}
