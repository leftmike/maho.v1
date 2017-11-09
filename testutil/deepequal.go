package testutil

import (
	"fmt"
	"reflect"
)

func deepValueEqual(v1, v2 reflect.Value) (bool, string) {
	if !v1.IsValid() || !v2.IsValid() {
		return v1.IsValid() == v2.IsValid(), ""
	}
	if v1.Type() != v2.Type() {
		return false, fmt.Sprintf("%#v.Type() != %#v.Type()\n", v1, v2)
	}

	switch v1.Kind() {
	case reflect.Array:
		for i := 0; i < v1.Len(); i++ {
			if ok, err := deepValueEqual(v1.Index(i), v2.Index(i)); !ok {
				return false, fmt.Sprintf("%s%#v[%d] != %#v[%d]\n", err, v1, i, v2, i)
			}
		}
		return true, ""
	case reflect.Slice:
		if v1.IsNil() != v2.IsNil() {
			return false, fmt.Sprintf("%#v != %#v\n", v1, v2)
		}
		if v1.Len() != v2.Len() {
			return false, fmt.Sprintf("%#v != %#v\n", v1, v2)
		}
		if v1.Pointer() == v2.Pointer() {
			return true, ""
		}
		for i := 0; i < v1.Len(); i++ {
			if ok, err := deepValueEqual(v1.Index(i), v2.Index(i)); !ok {
				return false, fmt.Sprintf("%s%#v[%d] != %#v[%d]\n", err, v1, i, v2, i)
			}
		}
		return true, ""
	case reflect.Interface:
		if v1.IsNil() || v2.IsNil() {
			return v1.IsNil() == v2.IsNil(), ""
		}
		return deepValueEqual(v1.Elem(), v2.Elem())
	case reflect.Ptr:
		if v1.Pointer() == v2.Pointer() {
			return true, ""
		}
		return deepValueEqual(v1.Elem(), v2.Elem())
	case reflect.Struct:
		for i, n := 0, v1.NumField(); i < n; i++ {
			if ok, err := deepValueEqual(v1.Field(i), v2.Field(i)); !ok {
				return false, fmt.Sprintf("%s%#v != %#v\n", err, v1, v2)
			}
		}
		return true, ""
	case reflect.Map:
		if v1.IsNil() != v2.IsNil() {
			return false, fmt.Sprintf("%#v != %#v\n", v1, v2)
		}
		if v1.Len() != v2.Len() {
			return false, fmt.Sprintf("%#v != %#v\n", v1, v2)
		}
		if v1.Pointer() == v2.Pointer() {
			return true, ""
		}
		for _, k := range v1.MapKeys() {
			val1 := v1.MapIndex(k)
			val2 := v2.MapIndex(k)
			if !val1.IsValid() || !val2.IsValid() {
				return false, fmt.Sprintf("%#v != %#v\n", v1, v2)
			}
			if ok, err := deepValueEqual(v1.MapIndex(k), v2.MapIndex(k)); !ok {
				return false, fmt.Sprintf("%s%#v != %#v\n", err, v1, v2)
			}
		}
		return true, ""
	case reflect.Func:
		if v1.IsNil() && v2.IsNil() {
			return true, ""
		}
		return false, fmt.Sprintf("%#v != %#v\n", v1, v2)
	default:
		if v1.Interface() != v2.Interface() {
			return false, fmt.Sprintf("%#v != %#v\n", v1, v2)
		}
		return true, ""
	}
}

// DeepEqual is the same as reflect.DeepEqual except that it optionally returns information
// about what was not equal and it does not terminate on cycles.
func DeepEqual(x, y interface{}, trc ...*string) bool {
	if len(trc) > 1 {
		panic("test.DeepEqual: more than one optional argument")
	}

	var eq bool
	var s string
	if x == nil || y == nil {
		if x != y {
			s = fmt.Sprintf("%#v != %#v\n", x, y)
		}
		eq = (x == y)
	} else {
		v1 := reflect.ValueOf(x)
		v2 := reflect.ValueOf(y)
		if v1.Type() != v2.Type() {
			s = fmt.Sprintf("%#v.Type() != %#v.Type()\n", v1, v2)
			eq = false
		} else {
			eq, s = deepValueEqual(v1, v2)
		}
	}

	if len(trc) == 1 && trc[0] != nil {
		*trc[0] = s
	}
	return eq
}
