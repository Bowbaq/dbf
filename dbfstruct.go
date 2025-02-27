package dbf

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type Iterator struct {
	dt     *DbfTable
	index  int
	last   int
	offset int
}

func (dt *DbfTable) NewIterator() *Iterator {
	return &Iterator{dt: dt, index: -1, offset: -1, last: dt.NumRecords()}
}

func (it *Iterator) Index() int {
	return it.index
}

// Next iterates over records in the table.
func (it *Iterator) Next() bool {
	for it.index++; it.index < it.last; it.index++ {
		if !it.dt.IsDeleted(it.index) {
			return true
		}
	}
	return false // it.index < it.last
}

// Read data into struct.
func (it *Iterator) Read(spec interface{}) error {
	return it.dt.Read(it.index, spec)
}

// Write record where iterator points to.
func (it *Iterator) Write(spec interface{}) (int, error) {
	return it.dt.Write(it.index, spec)
}

// Delete row under iterator. This is possible because rows are marked as deleted
// but are not physically deleted.
func (it *Iterator) Delete() {
	it.dt.Delete(it.index)
}

// Row data as raw slice.
func (it *Iterator) Row() []string {
	return it.dt.Row(it.index)
}

// Create schema based on the spec struct.
func (dt *DbfTable) Create(spec interface{}) error {
	s := reflect.ValueOf(spec)
	if s.Kind() == reflect.Ptr {
		s = s.Elem()
	}
	if s.Kind() != reflect.Struct {
		return errors.New("dbf: spec parameter must be a struct")
	}

	var err error
	typeOfSpec := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		if typeOfSpec.Field(i).PkgPath != "" || typeOfSpec.Field(i).Anonymous {
			continue // ignore unexported or embedded fields
		}

		dbfTag := typeOfSpec.Field(i).Tag.Get("dbf")
		// ignore '-' tags
		if dbfTag == "-" {
			continue
		}

		fieldName := dbfTag
		if fieldName == "" {
			fieldName = typeOfSpec.Field(i).Name
		}

		// If dbf tag has options (like omitempty), extract just the field name
		if options := strings.Split(dbfTag, ","); len(options) > 1 {
			fieldName = options[0]
		}

		var size uint8
		dbfSizeTag := typeOfSpec.Field(i).Tag.Get("size")
		if dbfSizeTag != "" && dbfSizeTag != "-" {
			n, err := strconv.ParseUint(dbfSizeTag, 0, 8)
			if err != nil {
				return errors.Errorf("dbf: invalid struct tag %s", dbfSizeTag)
			}
			size = uint8(n)
		}

		var precision uint8
		dbfPrecTag := typeOfSpec.Field(i).Tag.Get("precision")
		if dbfPrecTag != "" && dbfPrecTag != "-" {
			n, err := strconv.ParseUint(dbfPrecTag, 0, 8)
			if err != nil {
				return errors.Errorf("dbf: invalid struct tag %s", dbfPrecTag)
			}
			precision = uint8(n)
		}

		switch f.Kind() {
		default:
			return errors.Errorf("dbf: unsupported type %s for database table schema, use dash to omit", f.Kind())

		case reflect.String:
			if size == 0 || size > 50 {
				size = 50
			}
			err = dt.AddTextField(fieldName, size)

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if size == 0 || size > 17 {
				size = 17
			}
			err = dt.AddIntField(fieldName, size)

		case reflect.Float32, reflect.Float64:
			if size == 0 || size > 17 {
				size = 17
			}
			if precision == 0 || precision > 8 {
				precision = 8
			}
			err = dt.AddFloatField(fieldName, size, precision)

		case reflect.Bool:
			err = dt.AddBoolField(fieldName)

		case reflect.Struct:
			if f.Type() != reflect.TypeOf(time.Time{}) {
				return errors.Errorf("unsupported type '%s' for database table schema, use dash to omit", f.Type())
			}
			dt.AddDateField(fieldName)
		}

		if err != nil {
			return err
		}
	}
	return nil
}

// Append record to table.
func (dt *DbfTable) Append(spec interface{}) (int, error) {
	return dt.Write(dt.AddRecord(), spec)
}

// Write data into DbfTable from the spec.
func (dt *DbfTable) Write(row int, spec interface{}) (int, error) {
	s := reflect.ValueOf(spec)
	if s.Kind() == reflect.Ptr {
		s = s.Elem()
	}
	if s.Kind() != reflect.Struct {
		return -1, errors.New("dbf: spec parameter must be a struct")
	}

	typeOfSpec := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		if typeOfSpec.Field(i).PkgPath != "" || typeOfSpec.Field(i).Anonymous {
			continue // ignore unexported or embedded fields
		}

		dbfTag := typeOfSpec.Field(i).Tag.Get("dbf")
		// ignore '-' tags
		if dbfTag == "-" {
			continue
		}

		fieldName := dbfTag
		if fieldName == "" {
			fieldName = typeOfSpec.Field(i).Name
		}

		// Check for omitempty tag
		shouldOmitEmpty := false
		options := strings.Split(dbfTag, ",")
		if len(options) > 1 {
			for _, opt := range options[1:] {
				if opt == "omitempty" {
					shouldOmitEmpty = true
					break
				}
			}

			// Update fieldName to just use the name part without options
			fieldName = options[0]
		}

		// Skip this field if it should be omitted when empty and its value is empty
		if shouldOmitEmpty && isEmptyValue(f) {
			continue
		}

		val := ""
		switch f.Kind() {

		default:
			return -1, errors.Errorf("unsupported type '%s' for database table schema, use dash to omit", f.Type())

		case reflect.String:
			val = f.String()

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			val = fmt.Sprintf("%d", f.Int())

		case reflect.Float32, reflect.Float64:
			val = fmt.Sprintf("%f", f.Float())

		case reflect.Bool:
			val = "f"
			if f.Bool() {
				val = "t"
			}

		case reflect.Struct:
			if f.Type() != reflect.TypeOf(time.Time{}) {
				return -1, errors.Errorf("unsupported type '%s' for database table schema, use dash to omit", f.Type())
			}
			val = f.Interface().(time.Time).Format("20060102")
		}

		dt.SetFieldValueByName(row, fieldName, val)
	}

	return row, nil
}

// isEmptyValue returns whether the given value is the zero value for its type.
// Uses reflect.DeepEqual for simplicity.
func isEmptyValue(v reflect.Value) bool {
	// Special case for time.Time since DeepEqual doesn't handle it well
	if v.Type() == reflect.TypeOf(time.Time{}) {
		return v.Interface().(time.Time).IsZero()
	}

	// Create zero value of same type and compare
	zero := reflect.Zero(v.Type())
	return reflect.DeepEqual(v.Interface(), zero.Interface())
}

// Read data into the spec from DbfTable.
func (dt *DbfTable) Read(row int, spec interface{}) error {
	v := reflect.ValueOf(spec)
	if v.Kind() != reflect.Ptr {
		panic("dbf: must be a pointer")
	}
	s := v.Elem()
	if s.Kind() != reflect.Struct {
		panic("dbf: spec parameter must be a struct")
	}

	typeOfSpec := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		if f.CanSet() {
			fieldType := typeOfSpec.Field(i)

			dbfTag := fieldType.Tag.Get("dbf")
			// ignore '-' tags
			if dbfTag == "-" {
				continue
			}

			fieldName := dbfTag
			if fieldName == "" {
				fieldName = fieldType.Name
			}

			// If dbf tag has options (like omitempty), extract just the field name
			if options := strings.Split(dbfTag, ","); len(options) > 1 {
				fieldName = options[0]
			}

			var value string
			if fieldType.Tag.Get("raw") != "" {
				value = dt.RawFieldValueByName(row, fieldName)
			} else {
				value = dt.FieldValueByName(row, fieldName)
			}
			if strings.TrimSpace(value) == "" {
				continue
			}

			switch f.Kind() {
			default:
				return errors.Errorf("unsupported type '%s' for database table schema, use dash to omit", f.Type())

			case reflect.String:
				f.SetString(value)

			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:

				intValue, err := strconv.ParseInt(value, 0, f.Type().Bits())
				if err != nil {
					// Sometimes ints are formatted as floats
					floatValue, err := strconv.ParseFloat(value, 64)
					if err != nil {
						return fmt.Errorf("fail to parse field '%s' type: %s value: %s",
							fieldName, f.Type().String(), value)
					}
					intValue = int64(floatValue)
				}
				f.SetInt(intValue)

			case reflect.Bool:
				if value == "T" || value == "t" || value == "Y" || value == "y" {
					f.SetBool(true)
				} else {
					f.SetBool(false)
				}

			case reflect.Float32, reflect.Float64:
				floatValue, err := strconv.ParseFloat(value, f.Type().Bits())
				if err != nil {
					return errors.Errorf("fail to parse field '%s' type: %s value: %s",
						fieldName, f.Type().String(), value)
				}
				f.SetFloat(floatValue)

			case reflect.Struct:
				if f.Type() != reflect.TypeOf(time.Time{}) {
					return errors.Errorf("unsupported type '%s' for database table schema, use dash to omit", f.Type())
				}
				date, err := time.Parse("20060102", value)
				if err != nil {
					return errors.Errorf("fail to parse field '%s' type: %s value: %s",
						fieldName, f.Type().String(), value)
				}
				f.Set(reflect.ValueOf(date))
			}
		}
	}
	return nil
}
