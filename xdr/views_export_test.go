package xdr

import (
	"fmt"
	"reflect"
)

// views_export_test.go exposes test-only reflection bridges for the views
// conformance harness (xdr_views_conformance_test.go, package xdr_test). Because
// Raw()/Copy()/Validate() are package-level generics (xdr.Raw/Copy/Validate)
// rather than per-type methods, the reflection-driven harness cannot call them
// by method name on an arbitrary view value.
// These helpers accept an `any` (so they are reflection-callable) and route to
// the generic core via the unexported View-satisfying interface, which works
// because every view type is defined in this package. Compiled only in tests.
//
// Why these MIRROR viewRaw/viewCopy/validate rather than delegate to them: the
// production generics are constrained `[T View]`, and View carries a `~[]byte`
// type element, so they require a concrete view type at compile time. A
// reflection-driven harness only has an `any`, which cannot instantiate such a
// generic. The bridges therefore reproduce the generic body exactly — including
// identical error text — so what the harness exercises matches what ships. Keep
// them byte-for-byte equivalent to viewRaw/viewCopy/validate.

// RawValue trims a view value to its exact wire extent — a faithful mirror of
// the generic viewRaw (xdr.Raw), reachable by reflection.
func RawValue(v any) ([]byte, error) {
	s, ok := v.(interface {
		size(depth int) (int, error)
	})
	if !ok {
		return nil, viewErrShortBuffer(0, "value is not a view")
	}
	sz, err := s.size(0)
	if err != nil {
		return nil, err
	}
	b := asBytes(v)
	if sz > len(b) {
		// Identical to viewRaw's error so the harness sees production behavior.
		return nil, viewErrShortBuffer(0, fmt.Sprintf("size %d exceeds data length %d", sz, len(b)))
	}
	return b[:sz], nil
}

// ValidateValue runs whole-subtree validation on a view value (the generic
// xdr.Validate, reachable by reflection).
func ValidateValue(v any) error {
	s, ok := v.(interface {
		valid(depth int) (int, error)
	})
	if !ok {
		return viewErrShortBuffer(0, "value is not a view")
	}
	_, err := s.valid(0)
	return err
}

// CopyValue returns an independent (non-aliasing) copy of a view value, trimmed
// to its exact extent — the generic xdr.Copy, reachable by reflection. The result
// is returned as the same view type as the input (a named []byte). It mirrors
// viewCopy so the conformance harness can exercise Copy's no-panic and
// non-aliasing contract.
func CopyValue(v any) (any, error) {
	raw, err := RawValue(v)
	if err != nil {
		return nil, err
	}
	cp := make([]byte, len(raw))
	copy(cp, raw)
	// Re-wrap the copied bytes in the input's concrete view type.
	out := reflect.ValueOf(cp).Convert(reflect.TypeOf(v))
	return out.Interface(), nil
}

// asBytes returns the underlying []byte of a named-[]byte view value.
func asBytes(v any) []byte {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice || rv.Type().Elem().Kind() != reflect.Uint8 {
		return nil
	}
	return rv.Bytes()
}
