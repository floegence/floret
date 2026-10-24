// Package activityfields reads typed activity values without encoding sibling
// fields. The runtime keeps ownership of each immutable presentation snapshot.
package activityfields

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

var ErrPath = errors.New("activity field is unavailable")
var ErrRange = errors.New("invalid activity field range")

type Field struct {
	Path, Kind   string
	Bytes, Count int
	Value        json.RawMessage
}
type Page struct {
	Field    Field
	Children []Field
	Next     int
	HasMore  bool
}

type child struct {
	name  string
	value reflect.Value
}

func indirect(value reflect.Value) reflect.Value {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	return value
}
func children(value reflect.Value) []child {
	value = indirect(value)
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return nil
	}
	result := make([]child, 0, value.NumField())
	for i := 0; i < value.NumField(); i++ {
		field := value.Type().Field(i)
		if !field.IsExported() {
			continue
		}
		tag := strings.Split(field.Tag.Get("json"), ",")
		if tag[0] == "-" {
			continue
		}
		name := tag[0]
		if name == "" {
			name = field.Name
		}
		item := value.Field(i)
		omit := false
		for _, option := range tag[1:] {
			omit = omit || option == "omitempty"
		}
		if omit && empty(item) {
			continue
		}
		result = append(result, child{name, item})
	}
	return result
}
func empty(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return v.IsZero()
	}
	return false
}
func escape(s string) string { return strings.ReplaceAll(strings.ReplaceAll(s, "~", "~0"), "/", "~1") }
func resolve(root any, path string) (reflect.Value, error) {
	value := reflect.ValueOf(root)
	if len(path) > 4096 || path != "" && !strings.HasPrefix(path, "/") {
		return value, ErrPath
	}
	if path == "" {
		return indirect(value), nil
	}
	for _, part := range strings.Split(path[1:], "/") {
		for i := 0; i < len(part); i++ {
			if part[i] == '~' {
				if i+1 >= len(part) || (part[i+1] != '0' && part[i+1] != '1') {
					return reflect.Value{}, ErrPath
				}
				i++
			}
		}
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		value = indirect(value)
		if !value.IsValid() {
			return value, ErrPath
		}
		switch value.Kind() {
		case reflect.Struct:
			found := false
			for _, field := range children(value) {
				if field.name == part {
					value = field.value
					found = true
					break
				}
			}
			if !found {
				return reflect.Value{}, ErrPath
			}
		case reflect.Array, reflect.Slice:
			if value.Type().Elem().Kind() == reflect.Uint8 {
				return reflect.Value{}, ErrPath
			}
			i, err := strconv.Atoi(part)
			if err != nil || i < 0 || i >= value.Len() || strconv.Itoa(i) != part {
				return reflect.Value{}, ErrPath
			}
			value = value.Index(i)
		default:
			return reflect.Value{}, ErrPath
		}
	}
	return indirect(value), nil
}
func describe(v reflect.Value, path string) (Field, error) {
	v = indirect(v)
	field := Field{Path: path}
	if !v.IsValid() || v.Kind() == reflect.Slice && v.IsNil() {
		field.Kind = "scalar"
		field.Value = json.RawMessage("null")
		return field, nil
	}
	switch v.Kind() {
	case reflect.Struct:
		field.Kind = "object"
		field.Count = len(children(v))
	case reflect.String:
		field.Kind = "string"
		field.Bytes = v.Len()
	case reflect.Array, reflect.Slice:
		if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			field.Kind = "string"
			field.Bytes = base64.StdEncoding.EncodedLen(v.Len())
		} else {
			field.Kind = "array"
			field.Count = v.Len()
		}
	default:
		field.Kind = "scalar"
		raw, err := json.Marshal(v.Interface())
		if err != nil {
			return field, err
		}
		if len(raw) > 64 {
			return field, ErrRange
		}
		field.Value = raw
	}
	return field, nil
}
func Structure(root any, path string, offset, limit int) (Page, error) {
	if offset < 0 || limit < 1 || limit > 64 {
		return Page{}, ErrRange
	}
	value, err := resolve(root, path)
	if err != nil {
		return Page{}, err
	}
	field, err := describe(value, path)
	if err != nil {
		return Page{}, err
	}
	if offset > field.Count {
		return Page{}, ErrRange
	}
	end := offset + min(limit, field.Count-offset)
	page := Page{Field: field, Children: []Field{}, Next: end, HasMore: end < field.Count}
	var fields []child
	if field.Kind == "object" {
		fields = children(value)
	}
	for i := offset; i < end; i++ {
		name := strconv.Itoa(i)
		v := reflect.Value{}
		if field.Kind == "object" {
			name, v = fields[i].name, fields[i].value
		} else {
			v = value.Index(i)
		}
		child, err := describe(v, path+"/"+escape(name))
		if err != nil {
			return Page{}, err
		}
		page.Children = append(page.Children, child)
	}
	return page, nil
}
func Text(root any, path string, offset, limit, through int) (string, error) {
	if offset < 0 || limit < 4 || limit > 16<<10 || through < offset {
		return "", ErrRange
	}
	value, err := resolve(root, path)
	if err != nil {
		return "", err
	}
	if !value.IsValid() {
		return "", ErrPath
	}
	var source string
	if value.Kind() == reflect.String {
		source = value.String()
	} else if value.Kind() == reflect.Slice && value.Type().Elem().Kind() == reflect.Uint8 {
		source = base64.StdEncoding.EncodeToString(value.Bytes())
	} else {
		return "", ErrPath
	}
	if through > len(source) || through < len(source) && !utf8.RuneStart(source[through]) || offset < len(source) && !utf8.RuneStart(source[offset]) {
		return "", ErrRange
	}
	end := min(through, offset+limit)
	for end < len(source) && end > offset && !utf8.RuneStart(source[end]) {
		end--
	}
	return strings.Clone(source[offset:end]), nil
}

// Source returns the immutable selected string without encoding siblings.
func Source(root any, path string) (string, error) {
	value, err := resolve(root, path)
	if err != nil {
		return "", err
	}
	if !value.IsValid() {
		return "", ErrPath
	}
	if value.Kind() == reflect.String {
		return value.String(), nil
	}
	if value.Kind() == reflect.Slice && value.Type().Elem().Kind() == reflect.Uint8 {
		return base64.StdEncoding.EncodeToString(value.Bytes()), nil
	}
	return "", ErrPath
}

// ShapeVersion identifies this node's immediate structure, not sibling text.
func ShapeVersion(root any, path string) (string, error) {
	value, err := resolve(root, path)
	if err != nil {
		return "", err
	}
	field, err := describe(value, path)
	if err != nil {
		return "", err
	}
	fields := []Field{field}
	// Array ordinals are implicit. Their count completely defines the collection.
	if field.Kind == "object" {
		for _, child := range children(value) {
			item, err := describe(child.value, path+"/"+escape(child.name))
			if err != nil {
				return "", err
			}
			fields = append(fields, item)
		}
	}
	for i := range fields {
		fields[i].Bytes = 0
		fields[i].Value = nil
		if i > 0 {
			fields[i].Count = 0
		}
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}
