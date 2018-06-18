package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"

	"github.com/asdine/storm"
)

type FieldType int

const (
	Int64Field FieldType = iota
	StringField
)

type Field struct {
	Name  string
	Type  FieldType
	Value interface{}
	Data  []byte
}

func (f *Field) Encode() ([]byte, error) {
	var err error

	if f.Data == nil {
		switch f.Type {
		case StringField:
			f.Data = []byte(f.Value.(string))
		case Int64Field:
			f.Data, err = EncodeInt64(f.Value.(int64))
		default:
			return nil, errors.New("unsupported type")
		}
	}

	return f.Data, err
}

func EncodeInt64(i int64) ([]byte, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, i)
	return buf[:n], nil
}

func DecodeInt64(v []byte) (int64, error) {
	i, n := binary.Varint(v)
	if n < 0 {
		return 0, errors.New("overflow")
	}

	return i, nil
}

type FieldInfo struct {
	Name string
	Type FieldType
}

type Schema struct {
	fields map[string]*FieldInfo
}

func (c *Schema) Get(name string) *FieldInfo {
	f, _ := c.fields[name]
	return f
}

func (c *Schema) Set(name string, t FieldType) {
	if c.fields == nil {
		c.fields = make(map[string]*FieldInfo)
	}

	c.fields[name] = &FieldInfo{Name: name, Type: t}
}

type Record interface {
	Next() (*Field, error)
	Bytes(field string) ([]byte, error)
}

type FieldBuffer struct {
	fields []*Field
	i      int
	schema *Schema
}

func (b *FieldBuffer) Bytes(field string) ([]byte, error) {
	for _, f := range b.fields {
		if f.Name == field {
			return f.Encode()
		}
	}

	return nil, errors.New("field not found")
}

func (b *FieldBuffer) Next() (*Field, error) {
	if b.i >= len(b.fields) {
		return nil, nil
	}

	f := b.fields[b.i]
	b.i++

	return f, nil
}

func (b *FieldBuffer) Reset() {
	b.i = 0
	b.fields = b.fields[:0]
	b.schema = nil
}

func (b *FieldBuffer) setField(name string, typ FieldType, val interface{}, data []byte) error {
	if b.schema == nil {
		b.schema = new(Schema)
	}

	f := b.schema.Get(name)
	if f != nil {
		return errors.New("field already exists")
	}

	b.schema.Set(name, typ)
	f = b.schema.Get(name)

	fd := Field{
		Name:  f.Name,
		Type:  f.Type,
		Value: val,
		Data:  data,
	}

	b.fields = append(b.fields, &fd)
	return nil
}

func (b *FieldBuffer) SetInt64(name string, i int64) error {
	return b.setField(name, Int64Field, i, nil)
}

func (b *FieldBuffer) SetString(name string, s string) error {
	return b.setField(name, StringField, s, nil)
}

func (b *FieldBuffer) Set(name string, typ FieldType, val interface{}) error {
	return b.setField(name, typ, val, nil)
}

func (b *FieldBuffer) SetData(name string, typ FieldType, data []byte) error {
	return b.setField(name, typ, nil, data)
}

func (b *FieldBuffer) Len() int {
	return len(b.fields)
}

func NewFieldBufferStruct(v interface{}) (*FieldBuffer, error) {
	ref := reflect.ValueOf(v)
	if !ref.IsValid() || ref.Kind() != reflect.Ptr || ref.Elem().Kind() != reflect.Struct {
		return nil, storm.ErrStructPtrNeeded
	}

	typ := ref.Type()
	var fb FieldBuffer

	for i := 0; i < ref.NumField(); i++ {
		r := ref.Field(i)
		t := typ.Field(i)

		var ftyp FieldType
		switch r.Kind() {
		case reflect.Int64:
			ftyp = Int64Field
		case reflect.String:
			ftyp = StringField
		default:
			return nil, fmt.Errorf("unsupported type '%s'", r.Kind())
		}

		err := fb.Set(t.Name, ftyp, r.Interface())
		if err != nil {
			return nil, err
		}
	}

	return &fb, nil
}

type RecordScanner struct {
	Record
}

func (r *RecordScanner) GetString(field string) (string, error) {
	v, err := r.Bytes(field)
	if err != nil {
		return "", err
	}

	return string(v), nil
}

func (r *RecordScanner) GetInt64(field string) (int64, error) {
	v, err := r.Bytes(field)
	if err != nil {
		return 0, err
	}

	return DecodeInt64(v)
}

type RecordBuffer struct {
	records []Record
	i       int
}

func (b *RecordBuffer) Add(rec Record) {
	b.records = append(b.records, rec)
}

func (b *RecordBuffer) Cursor() (Cursor, error) {
	return &cursor{records: b.records}, nil
}

func (b *RecordBuffer) Schema() (*Schema, error) {
	if len(b.records) == 0 {
		return nil, errors.New("can't generate schema from empty record buffer")
	}

	r := b.records[0]
	var s Schema
	for {
		f, err := r.Next()
		if err != nil {
			return nil, err
		}

		if f == nil {
			break
		}

		s.Set(f.Name, f.Type)
	}

	return &s, nil
}

type cursor struct {
	i       int
	records []Record
}

func (c *cursor) Next() (Record, error) {
	if c.i < len(c.records) {
		r := c.records[c.i]
		c.i++
		return r, nil
	}

	return nil, nil
}
