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
	bv, ok := v.(bool)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	*b = boolValue(bv)
	return nil
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
	iv, ok := v.(int)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	*i = intValue(iv)
	return nil
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
	iv, ok := v.(int)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	*i = int64Value(iv)
	return nil
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
	iv, ok := v.(int)
	if !ok || iv < 0 {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	*u = uintValue(iv)
	return nil
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
	iv, ok := v.(int)
	if !ok || iv < 0 {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	*u = uint64Value(iv)
	return nil
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
	fv, ok := v.(float64)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	*f = float64Value(fv)
	return nil
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
	iv, ok := v.(int)
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	*d = durationValue(iv)
	return nil
}

func (d *durationValue) String() string {
	return (*time.Duration)(d).String()
}

type Array []interface{} // int, float64, bool, string, []interface{}, map[string]interface{}

func (a *Array) Set(s string) error {
	*a = append(*a, s)
	return nil
}

func (a *Array) SetValue(v interface{}) error {
	av, ok := v.([]interface{})
	if !ok {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	*a = Array(av)
	return nil
}

func (a *Array) String() string {
	return fmt.Sprintf("%v", *a)
}

type Map map[string]interface{} // int, float64, bool, string, []interface{}, map[string]interface{}

func (m Map) SetValue(v interface{}) error {
	mv, ok := v.([]map[string]interface{})
	if !ok || len(mv) != 1 {
		return fmt.Errorf("parsing %v: invalid syntax", v)
	}
	for k, v := range mv[0] {
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
