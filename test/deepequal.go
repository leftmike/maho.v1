package test

import (
	"fmt"
	"reflect"
)

func deepValueEqual(v1, v2 reflect.Value) bool {
	if !v1.IsValid() || !v2.IsValid() {
		return v1.IsValid() == v2.IsValid()
	}
	if v1.Type() != v2.Type() {
		fmt.Printf("%#v.Type() != %#v.Type()\n", v1, v2)
		return false
	}

	switch v1.Kind() {
	case reflect.Array:
		for i := 0; i < v1.Len(); i++ {
			if !deepValueEqual(v1.Index(i), v2.Index(i)) {
				fmt.Printf("%#v[%d] != %#v[%d]\n", v1, i, v2, i)
				return false
			}
		}
		return true
	case reflect.Slice:
		if v1.IsNil() != v2.IsNil() {
			fmt.Printf("%#v != %#v\n", v1, v2)
			return false
		}
		if v1.Len() != v2.Len() {
			fmt.Printf("%#v != %#v\n", v1, v2)
			return false
		}
		if v1.Pointer() == v2.Pointer() {
			return true
		}
		for i := 0; i < v1.Len(); i++ {
			if !deepValueEqual(v1.Index(i), v2.Index(i)) {
				fmt.Printf("%#v[%d] != %#v[%d]\n", v1, i, v2, i)
				return false
			}
		}
		return true
	case reflect.Interface:
		if v1.IsNil() || v2.IsNil() {
			return v1.IsNil() == v2.IsNil()
		}
		return deepValueEqual(v1.Elem(), v2.Elem())
	case reflect.Ptr:
		if v1.Pointer() == v2.Pointer() {
			return true
		}
		return deepValueEqual(v1.Elem(), v2.Elem())
	case reflect.Struct:
		for i, n := 0, v1.NumField(); i < n; i++ {
			if !deepValueEqual(v1.Field(i), v2.Field(i)) {
				fmt.Printf("%#v != %#v\n", v1, v2)
				return false
			}
		}
		return true
	case reflect.Map:
		if v1.IsNil() != v2.IsNil() {
			fmt.Printf("%#v != %#v\n", v1, v2)
			return false
		}
		if v1.Len() != v2.Len() {
			fmt.Printf("%#v != %#v\n", v1, v2)
			return false
		}
		if v1.Pointer() == v2.Pointer() {
			return true
		}
		for _, k := range v1.MapKeys() {
			val1 := v1.MapIndex(k)
			val2 := v2.MapIndex(k)
			if !val1.IsValid() || !val2.IsValid() ||
				!deepValueEqual(v1.MapIndex(k), v2.MapIndex(k)) {
				fmt.Printf("%#v != %#v\n", v1, v2)
				return false
			}
		}
		return true
	case reflect.Func:
		if v1.IsNil() && v2.IsNil() {
			return true
		}
		fmt.Printf("%#v != %#v\n", v1, v2)
		return false
	default:
		if v1.Interface() != v2.Interface() {
			fmt.Printf("%#v != %#v\n", v1, v2)
		}
		return v1.Interface() == v2.Interface()
	}
}

// DeepEqual is the same as reflect.DeepEqual except that it prints information about what was
// not equal and it does not terminate on cycles.
func DeepEqual(x, y interface{}) bool {
	if x == nil || y == nil {
		if x != y {
			fmt.Printf("%#v != %#v\n", x, y)
		}
		return x == y
	}
	v1 := reflect.ValueOf(x)
	v2 := reflect.ValueOf(y)
	if v1.Type() != v2.Type() {
		fmt.Printf("%#v.Type() != %#v.Type()\n", v1, v2)
		return false
	}
	return deepValueEqual(v1, v2)
}
