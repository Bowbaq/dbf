/*
Package for working with dBase III plus database files.

1. Package provides both reflection-via-struct interface and direct Row()/FieldValueByName()/AddxxxField() interface.
2. Once table is created and rows added to it, table structure can not be modified.
3. Working with reflection-via-struct interface is easier and produces less verbose code.
4. Use Iterator to iterate over table since it skips deleted rows.

With struct interface, you can use struct tags to customize field mapping:

	type Person struct {
	   Name    string    `dbf:"NAME"`              // Map to NAME field
	   Age     int       `dbf:"AGE,omitempty"`     // Map to AGE field, omit if zero
	   Created time.Time `dbf:"CREATED"`           // Map to CREATED field
	}

The `omitempty` tag option works similarly to the encoding/json package.
When omitempty is specified for a field, that field will only be written
if its value is not the zero value for its type.

TODO: File is loaded and kept in-memory. Not a good design choice if file is huge.
This should be changed to use buffers and keep some of the data on-disk in the future.
Current API structure should allow redesign.

Typical usage
db := dbf.New() or dbf.LoadFile(filename)

then use db.NewIterator() and iterate or db.Append()

do not forget db.SaveFile(filename) if you want changes saved.
*/
package dbf
