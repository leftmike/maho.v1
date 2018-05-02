package config

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type boolValue bool

func (b *boolValue) Set(s string) error {
	v, err := strconv.ParseBool(s)
	*b = boolValue(v)
	return err
}

func (b *boolValue) SetValue(v interface{}) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	return b.Set(s)
}

func (b *boolValue) String() string {
	return strconv.FormatBool(bool(*b))
}

type intValue int

func (i *intValue) Set(s string) error {
	v, err := strconv.ParseInt(s, 0, strconv.IntSize)
	*i = intValue(v)
	return err
}

func (i *intValue) SetValue(v interface{}) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	return i.Set(s)
}

func (i *intValue) String() string {
	return strconv.Itoa(int(*i))
}

type int64Value int64

func (i *int64Value) Set(s string) error {
	v, err := strconv.ParseInt(s, 0, 64)
	*i = int64Value(v)
	return err
}

func (i *int64Value) SetValue(v interface{}) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	return i.Set(s)
}

func (i *int64Value) String() string {
	return strconv.FormatInt(int64(*i), 10)
}

type uintValue uint

func (u *uintValue) Set(s string) error {
	v, err := strconv.ParseUint(s, 0, strconv.IntSize)
	*u = uintValue(v)
	return err
}

func (u *uintValue) SetValue(v interface{}) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	return u.Set(s)
}

func (u *uintValue) String() string {
	return strconv.FormatUint(uint64(*u), 10)
}

type uint64Value uint64

func (u *uint64Value) Set(s string) error {
	v, err := strconv.ParseUint(s, 0, 64)
	*u = uint64Value(v)
	return err
}

func (u *uint64Value) SetValue(v interface{}) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	return u.Set(s)
}

func (u *uint64Value) String() string {
	return strconv.FormatUint(uint64(*u), 10)
}

type stringValue string

func (s *stringValue) Set(val string) error {
	*s = stringValue(val)
	return nil
}

func (s *stringValue) SetValue(v interface{}) error {
	sv, ok := v.(string)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	return s.Set(sv)
}

func (s *stringValue) String() string {
	return string(*s)
}

type float64Value float64

func (f *float64Value) Set(s string) error {
	v, err := strconv.ParseFloat(s, 64)
	*f = float64Value(v)
	return err
}

func (f *float64Value) SetValue(v interface{}) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	return f.Set(s)
}

func (f *float64Value) String() string {
	return strconv.FormatFloat(float64(*f), 'g', -1, 64)
}

type durationValue time.Duration

func (d *durationValue) Set(s string) error {
	v, err := time.ParseDuration(s)
	*d = durationValue(v)
	return err
}

func (d *durationValue) SetValue(v interface{}) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	return d.Set(s)
}

func (d *durationValue) String() string {
	return (*time.Duration)(d).String()
}

type Array []interface{} // string, Map, or Array

func (a *Array) Set(s string) error {
	*a = append(*a, s)
	return nil
}

func (a *Array) SetValue(v interface{}) error {
	av, ok := v.(Array)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	*a = av
	return nil
}

func (a *Array) String() string {
	return fmt.Sprintf("%v", *a)
}

type Map map[string]interface{} // string, Map, or Array

func (m Map) SetValue(v interface{}) error {
	mv, ok := v.(Map)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	for k, v := range mv {
		m[k] = v
	}
	return nil
}

func (m Map) String() string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteRune('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteRune(' ')
		}
		fmt.Fprintf(&b, "%s: %s", k, m[k])
	}
	b.WriteRune('}')
	return b.String()
}
