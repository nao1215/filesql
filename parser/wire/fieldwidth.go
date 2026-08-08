package wire

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"
)

// fieldSuffix is what moov-io/wire names the method that renders a field into
// its fixed-width form: Name is the value, NameField() is what gets written.
const fieldSuffix = "Field"

// validateFieldWidths reports every value in the message that a fixed-width
// field would cut rather than carry.
//
// A Fedwire message is a fixed-width record, and the writer's field formatters
// shorten a value that does not fit instead of refusing it — so an originator
// name past 35 characters was written as its first 35 and the save reported
// success, leaving the file holding a name the session never asked for.
//
// The widths are not listed here, and they could not usefully be: the library
// defines close to three hundred of these fields. Each formatter returns
// exactly the number of characters its field holds, padding a short value and
// cutting a long one, so calling it is the specification's own answer about the
// value it was given.
//
// The walk pairs a formatter with its value by name, which is a guess, so the
// check is written to be safe when the guess is wrong: a value is only refused
// when the formatted form is a strict prefix of it, which is what truncation
// looks like and what an unrelated pairing almost never produces. A wrong guess
// therefore stays silent rather than refusing a save it should have allowed.
func validateFieldWidths(message any) error {
	var errs []error
	visit(reflect.ValueOf(message), "", &errs, make(map[reflect.Value]bool))
	return errors.Join(errs...)
}

// visit walks structs reachable from v, checking each one's field formatters.
func visit(v reflect.Value, path string, errs *[]error, seen map[reflect.Value]bool) {
	if !v.IsValid() {
		return
	}

	if v.Kind() == reflect.Pointer {
		if v.IsNil() || seen[v] {
			return
		}
		seen[v] = true
		checkFormatters(v, path, errs)
		visit(v.Elem(), path, errs, seen)
		return
	}

	if v.Kind() != reflect.Struct {
		return
	}

	t := v.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		child := v.Field(i)
		name := path + field.Name + "."
		switch child.Kind() {
		case reflect.Pointer:
			visit(child, name, errs, seen)
		case reflect.Struct:
			// A nested struct's own formatters are reached through its address
			// when it has one, since the methods are on the pointer type.
			if child.CanAddr() {
				visit(child.Addr(), name, errs, seen)
				continue
			}
			visit(child, name, errs, seen)
		case reflect.Slice:
			for j := range child.Len() {
				visit(child.Index(j), fmt.Sprintf("%s%s[%d].", path, field.Name, j), errs, seen)
			}
		default:
		}
	}
}

// checkFormatters calls every "<Name>Field() string" method on ptr and compares
// what it renders with the value it renders from.
func checkFormatters(ptr reflect.Value, path string, errs *[]error) {
	t := ptr.Type()
	for i := range t.NumMethod() {
		method := t.Method(i)
		if !strings.HasSuffix(method.Name, fieldSuffix) {
			continue
		}
		if method.Type.NumIn() != 1 || method.Type.NumOut() != 1 || method.Type.Out(0).Kind() != reflect.String {
			continue
		}

		source, ok := findStringField(ptr.Elem(), strings.TrimSuffix(method.Name, fieldSuffix))
		if !ok || source == "" {
			continue
		}

		formatted := method.Func.Call([]reflect.Value{ptr})[0].String()
		if utf8.RuneCountInString(source) <= utf8.RuneCountInString(formatted) {
			continue
		}
		if !strings.HasPrefix(source, formatted) {
			// Not the shape truncation leaves, so the pairing is not trusted.
			continue
		}

		*errs = append(*errs, fmt.Errorf(
			"%s%s is %d characters, but the Fedwire record holds %d: %q would be written as %q",
			path, strings.TrimSuffix(method.Name, fieldSuffix),
			utf8.RuneCountInString(source), utf8.RuneCountInString(formatted), source, formatted))
	}
}

// findStringField returns the string field called name, looking one level into
// nested structs because a tag often keeps its value in an embedded record —
// Originator.NameField renders Originator.Personal.Name.
func findStringField(v reflect.Value, name string) (string, bool) {
	if v.Kind() != reflect.Struct {
		return "", false
	}

	if field := v.FieldByName(name); field.IsValid() && field.Kind() == reflect.String {
		return field.String(), true
	}

	t := v.Type()
	for i := range t.NumField() {
		if !t.Field(i).IsExported() || v.Field(i).Kind() != reflect.Struct {
			continue
		}
		if field := v.Field(i).FieldByName(name); field.IsValid() && field.Kind() == reflect.String {
			return field.String(), true
		}
	}
	return "", false
}
