package config

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// Saver implements common functionality useful for ComponentConfigs
type Saver struct {
	save    chan struct{}
	BaseDir string
}

// NotifySave signals the SaveCh() channel in a non-blocking fashion.
func (sv *Saver) NotifySave() {
	if sv.save == nil {
		sv.save = make(chan struct{}, 10)
	}

	// Non blocking, in case no one's listening
	select {
	case sv.save <- struct{}{}:
	default:
		logger.Warning("configuration save channel full")
	}
}

// SaveCh returns a channel which is signaled when a component wants
// to persist its configuration
func (sv *Saver) SaveCh() <-chan struct{} {
	if sv.save == nil {
		sv.save = make(chan struct{})
	}
	return sv.save
}

// SetBaseDir is a setter for BaseDir and implements
// part of the ComponentConfig interface.
func (sv *Saver) SetBaseDir(dir string) {
	sv.BaseDir = dir
}

// DefaultJSONMarshal produces pretty JSON with 2-space indentation
func DefaultJSONMarshal(v interface{}) ([]byte, error) {
	bs, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return bs, nil
}

// SetIfNotDefault sets dest to the value of src if src is not the default
// value of the type.
// dest must be a pointer.
func SetIfNotDefault(src interface{}, dest interface{}) {
	switch src.(type) {
	case time.Duration:
		t := src.(time.Duration)
		if t != 0 {
			*dest.(*time.Duration) = t
		}
	case string:
		str := src.(string)
		if str != "" {
			*dest.(*string) = str
		}
	case uint64:
		n := src.(uint64)
		if n != 0 {
			*dest.(*uint64) = n
		}
	case int:
		n := src.(int)
		if n != 0 {
			*dest.(*int) = n
		}
	case bool:
		b := src.(bool)
		if b {
			*dest.(*bool) = b
		}
	}
}

// DurationOpt provides a datatype to use with ParseDurations
type DurationOpt struct {
	// The duration we need to parse
	Duration string
	// Where to store the result
	Dst *time.Duration
	// A variable name associated to it for helpful errors.
	Name string
}

// ParseDurations takes a time.Duration src and saves it to the given dst.
func ParseDurations(component string, args ...*DurationOpt) error {
	for _, arg := range args {
		if arg.Duration == "" {
			// don't do anything. Let the destination field
			// stay at its default.
			continue
		}
		t, err := time.ParseDuration(arg.Duration)
		if err != nil {
			return fmt.Errorf(
				"error parsing %s.%s: %s",
				component,
				arg.Name,
				err,
			)
		}
		*arg.Dst = t
	}
	return nil
}

// String takes an object and returns it's string form excluding hidden
// elements.
func String(cfg interface{}, hidden []string) string {
	return sType(reflect.ValueOf(cfg), hidden, "")
}

func sType(v reflect.Value, hidden []string, padding string) string {
	var result string

	switch v.Kind() {
	case reflect.Struct:
		result += fmt.Sprintf("{\n")
		childPadding := padding + "  " // padding plus two spaces
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			childName := v.Type().Field(i).Name

			if in(hidden, childName) {
				continue
			}

			switch field.Kind() {
			case reflect.Struct, reflect.Slice, reflect.Map:
				result += fmt.Sprintf("%s%-20s : %s,\n", childPadding, childName, sType(field, hidden, childPadding))
			case reflect.Ptr:
				result += fmt.Sprintf("%s%-20s : %s,\n", childPadding, childName, sType(reflect.Indirect(field), hidden, childPadding))
			default:
				result += fmt.Sprintf("%s%-20s : %s,\n", childPadding, childName, sType(field, nil, ""))
			}
		}
		result += fmt.Sprintf("%s}", padding)
		return result
	case reflect.Slice:
		result += fmt.Sprintf("[\n")
		slicePadding := padding + "  "
		for i := 0; i < v.Len(); i++ {
			result += fmt.Sprintf("%s%s,\n", slicePadding, sType(v.Index(i), nil, ""))
		}
		result += fmt.Sprintf("%s]", padding)
		return result
	case reflect.Map:
		result += fmt.Sprintf("{\n")
		slicePadding := padding + "  "
		for _, key := range v.MapKeys() {
			result += fmt.Sprintf("%s%s,\n", slicePadding, sType(v.MapIndex(key), nil, ""))
		}
		result += fmt.Sprintf("%s}", padding)
	case reflect.Ptr:
		result += sType(reflect.Indirect(v), hidden, padding)
	case reflect.String:
		return fmt.Sprintf("%s", v.String())
	case reflect.Int:
		return fmt.Sprintf("%d", v.Int())
	case reflect.Bool:
		return fmt.Sprintf("%t", v.Bool())
	case reflect.Float64:
		return fmt.Sprintf("%f", v.Float())
	case reflect.Int64:
		return fmt.Sprintf("%d", v.Int())
	case reflect.Uint64:
		return fmt.Sprintf("%d", v.Uint())
	case reflect.Uint32:
		return fmt.Sprintf("%d", v.Uint())
	default:
		return fmt.Sprintf("unexpected kind %s", v.Kind().String())
	}
	return ""
}

func in(arr []string, a string) bool {
	for _, f := range arr {
		if a == f {
			return true
		}
	}

	return false
}
