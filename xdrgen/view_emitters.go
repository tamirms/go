package main

import (
	"fmt"
)

// This file contains shared emit helpers and concrete view type emitters.
// We generate per-type code rather than using Go generics because Go's shape
// stenciling compiles all ~[]byte types into a single shared function body with
// indirect dictionary dispatch, adding ~25% overhead on traversal-heavy paths.

// --- Shared emit helpers (used by struct, union, enum, array, opaque, optional emitters) ---

// Raw(), Copy(), and Validate() are not emitted per-type: they are package-level
// generics (xdr.Raw/Copy/Validate over the sealed View constraint; xdr.MustRaw)
// defined in the support file. There is no per-element Must family or generated
// All()/Iter(); arrays are walked with the Scan() cursor. Must is emitted inline
// only on single-valued accessors (struct fields, union arms, Unwrap, leaf
// Value, discriminant).

// emitValueBasedValid emits a valid() that delegates to Value() for schema
// validation, then returns size(). Used by enums, fixed opaque, bounded opaque.
func emitValueBasedValid(f *GeneratedFile, typeName string) {
	f.Use("typeName", typeName).Block(`
		func (v $typeName) valid(_ int) (int, error) {
			if _, err := v.Value(); err != nil { return 0, err }
			return v.size(0)
		}
	`)
}

func emitFixedSizeMethods(f *GeneratedFile, viewTypeName string, size uint32) {
	g := f.Use("viewTypeName", viewTypeName, "size", size)
	g.L("func (v $viewTypeName) size(_ int) (int, error) { return $size, nil }")
}

// emitSizeTraversal, emitLocateTraversal, and emitValidTraversal are three
// struct-field walks (size advances `off`; locate also captures each field's
// extent; valid recurses validation). emitSizeTraversal and emitLocateTraversal
// share the same advance logic — fixed-size fields fold to `off += N` and
// void-case-0 unions are inlined — so a change to that wire traversal must be
// mirrored across both or they will silently drift. emitValidTraversal
// intentionally diverges: it does NOT fold fixed fields or inline void-case-0,
// it recurses valid() into every field unconditionally, so it does not need to
// stay in lockstep with the other two.
// TODO: unify size + locate into one parameterized walk (a per-field visitor) so
// their shared advance logic lives in one place; the conformance harness (tiling
// + valid/size agreement) would catch a drift, but one source is better than two.
//
// emitSizeTraversal emits code that advances `off` past fields [0, end) for size/accessor paths.
// Fixed-size fields emit `off += N` (the compiler folds consecutive additions).
// Void-case-0 unions are inlined for the common extension-point pattern.
func emitSizeTraversal(f *GeneratedFile, fields []FieldPlan, call, errReturn string) {
	g := f.Use("call", call, "errReturn", errReturn)
	for i := range fields {
		vt := fields[i].ViewType
		if fs, ok := vt.FixedSize(); ok {
			g.Set("fs", fs).L("	off += $fs")
			continue
		}
		g.L(`	if off > int64(len(v)) { return $errReturn, viewErrShortBuffer(uint32(off), "field offset exceeds data") }`)
		h := g.Set("fieldType", vt.GoType)
		if fields[i].IsVoidCase0 {
			h.Block(`
					{ d := []byte(v)[off:]
					if len(d) >= 4 && binary.BigEndian.Uint32(d[:4]) == 0 {
						off += 4
					} else {
						sz, err := $fieldType(d).$call
						if err != nil { return $errReturn, err }
						off += int64(sz)
					} }
			`)
			continue
		}
		h.Block(`
				{ sz, err := $fieldType(v[off:]).$call
				if err != nil { return $errReturn, err }
				off += int64(sz)
				if off > int64(len(v)) { return $errReturn, viewErrShortBuffer(uint32(off), "field offset exceeds data") } }
		`)
	}
	g.L(`	if off > int64(len(v)) { return $errReturn, viewErrShortBuffer(uint32(off), "field offset exceeds data") }`)
}

// fieldsBundleFieldName returns the Go field name to use for a struct field
// inside its Fields bundle, escaping the reserved bundle field "View".
func fieldsBundleFieldName(name string) string {
	if name == "View" {
		return "View_"
	}
	return name
}

// emitLocateTraversal emits the body of a variable-size struct's locate helper:
// one walk over all fields capturing each field's trimmed extent into the bundle
// `f`, mirroring emitSizeTraversal's advance logic but keeping the offsets.
// On entry `off` is int64(0); on exit `off` is the node's total extent, and
// every f.<Field> has been set to a trimmed sub-view v[start:end].
func emitLocateTraversal(f *GeneratedFile, fields []FieldPlan) {
	g := f.Use()
	for i := range fields {
		vt := fields[i].ViewType
		bundleName := fieldsBundleFieldName(fields[i].FieldName)
		h := g.Set("bundleName", bundleName).Set("fieldType", vt.GoType)
		if fs, ok := vt.FixedSize(); ok {
			// Fixed-size field: extent is a compile-time constant.
			h.Set("fs", fs).Block(`
					if off+$fs > int64(len(v)) { return f, viewErrShortBuffer(uint32(off), "field offset exceeds data") }
					f.$bundleName = $fieldType(v[off : off+$fs])
					off += $fs
			`)
			continue
		}
		h.L(`	if off > int64(len(v)) { return f, viewErrShortBuffer(uint32(off), "field offset exceeds data") }`)
		if fields[i].IsVoidCase0 {
			h.Block(`
					{ d := []byte(v)[off:]
					var fsz int64
					if len(d) >= 4 && binary.BigEndian.Uint32(d[:4]) == 0 {
						fsz = 4
					} else {
						sz, err := $fieldType(d).size(0)
						if err != nil { return f, err }
						fsz = int64(sz)
					}
					if off+fsz > int64(len(v)) { return f, viewErrShortBuffer(uint32(off), "field offset exceeds data") }
					f.$bundleName = $fieldType(v[off : off+fsz])
					off += fsz }
			`)
			continue
		}
		h.Block(`
				{ sz, err := $fieldType(v[off:]).size(0)
				if err != nil { return f, err }
				fsz := int64(sz)
				if off+fsz > int64(len(v)) { return f, viewErrShortBuffer(uint32(off), "field offset exceeds data") }
				f.$bundleName = $fieldType(v[off : off+fsz])
				off += fsz }
		`)
	}
	g.L(`	if off > int64(len(v)) { return f, viewErrShortBuffer(uint32(off), "field offset exceeds data") }`)
}

// emitValidTraversal emits code that advances `off` past fields [0, end) for the valid() path.
func emitValidTraversal(f *GeneratedFile, fields []FieldPlan) {
	g := f.Use()
	for i := range fields {
		g.Set("fieldType", fields[i].ViewType.GoType).Block(`
				{ sz, err := $fieldType(v[off:]).valid(depth + 1)
				if err != nil { return 0, err }
				off += int64(sz)
				if off > int64(len(v)) { return 0, viewErrShortBuffer(uint32(off), "field offset exceeds data") } }
		`)
	}
}

// --- Concrete view type emitters (arrays, optionals, opaque) ---

// emitConcreteViewType emits a concrete view type (array, opaque, or optional)
// with the given name. The plan phase determines which types need emission;
// this is the dispatch point for the actual code generation.
func emitConcreteViewType(f *GeneratedFile, ip *InlineTypePlan) error {
	switch ip.ViewType.Kind {
	case VKArray:
		return emitArrayType(f, ip)
	case VKOpaque:
		emitOpaqueType(f, ip)
	case VKOptional:
		emitOptionalType(f, ip.Name, ip.ViewType)
	}
	return nil
}

// arrayMinElemW returns the count-validation divisor (the element's clamped
// minimum wire size) for an array view. It returns an error when the element
// computes a zero minimum, which would make the O(1) count bound unsound. Both
// the standalone Count() accessor and the Scan() cursor use this so their
// validation is identical.
func arrayMinElemW(ip *InlineTypePlan) (uint32, error) {
	// For a fixed-size element, minElemWidth already short-circuits minWireSize to
	// the element's exact size (and a non-zero fixed size needs no floor clamp), so
	// ElemMinWidth/ElemMinWidthComputed already equal that size — no override is
	// needed. A zero-size fixed element (e.g. opaque[0]) is rejected loudly upstream
	// at BuildViewType, so it never reaches here with a zero computed minimum.
	if ip.ElemMinWidthComputed == 0 {
		return 0, fmt.Errorf("array view %s: element type %s has zero minimum wire size; "+
			"count validation bound would be unsound", ip.Name, ip.ViewType.Array.Element.GoType)
	}
	return ip.ElemMinWidth, nil
}

// emitArrayType generates a complete concrete array view type.
// Fixed vs variable count is determined by vt.Array.Count (> 0 = fixed).
// Fixed vs variable elements is determined by vt.Array.Element.FixedSize().
func emitArrayType(f *GeneratedFile, ip *InlineTypePlan) error {
	typeName := ip.Name
	vt := ip.ViewType
	elemType := vt.Array.Element.GoType
	elemSize, isFixedElem := vt.Array.Element.FixedSize()
	isVarCount := vt.Array.Count == 0

	startOff := 0
	if isVarCount {
		startOff = 4
	}

	var countExpr any = vt.Array.Count
	if isVarCount {
		countExpr = "count"
	}

	minElemW, err := arrayMinElemW(ip)
	if err != nil {
		return err
	}

	g := f.Use(
		"typeName", typeName,
		"elemType", elemType,
		"elemSize", elemSize,
		"minElemW", minElemW,
		"startOff", startOff,
		"countExpr", countExpr,
		"maxLen", vt.Array.MaxLen,
		"count", vt.Array.Count,
	)

	g.L("type $typeName []byte")

	if isVarCount {
		// Count() validates the wire count against BOTH the schema maximum and the
		// remaining buffer (arrayViewCountChecked), matching the Scan() cursor: a
		// standalone Count() on an unbounded array must not return an unvalidated
		// 4-byte wire count.
		g.L("func (v $typeName) Count() (int, error) { return arrayViewCountChecked([]byte(v), $maxLen, $minElemW) }")
	} else {
		g.L("func (v $typeName) Len() int { return $count }")
	}

	// size — fixed-element arrays use O(1) shortcuts
	if isFixedElem && isVarCount {
		g.Block(`
			func (v $typeName) size(depth int) (int, error) {
				if depth > maxDepth { return 0, viewErrMaxDepth(0) }
				count, err := arrayViewCount([]byte(v), $maxLen)
				if err != nil { return 0, err }
				total := int64(4) + int64(count)*int64($elemSize)
				if total > int64(len(v)) { return 0, viewErrArrayCountExceedsData(4, count, len(v)-4) }
				return int(total), nil
			}
		`)
	} else if isFixedElem {
		// A fixed-count + fixed-element array is itself fixed-size; ResolveViewType
		// already computed elemSize*count and rejected a uint32 overflow when it set
		// vt.fixedSize, so reuse that validated total rather than recomputing it.
		total, _ := vt.FixedSize()
		g = g.Set("totalSize", total)
		g.L("func (v $typeName) size(_ int) (int, error) { return $totalSize, nil }")
	}

	// size (variable-element only) + valid — shared via call parameter
	methods := []struct{ name, call string }{{"valid", "valid(depth + 1)"}}
	if !isFixedElem {
		methods = append([]struct{ name, call string }{{"size", "size(depth + 1)"}}, methods...)
	}
	for _, m := range methods {
		h := g.Set("method", m.name).Set("call", m.call)
		h.L("func (v $typeName) $method(depth int) (int, error) {")
		h.L("	if depth > maxDepth { return 0, viewErrMaxDepth(0) }")
		if isVarCount {
			h.L("	count, err := arrayViewCount([]byte(v), $maxLen)")
			h.L("	if err != nil { return 0, err }")
		}
		h.L("	return arrayTraverse([]byte(v), $countExpr, $startOff, func(d []byte) (int, error) { return $elemType(d).$call })")
		h.L("}")
	}

	// At
	emitArrayAt(g, isVarCount, isFixedElem)

	// The Scan() cursor is the one way to walk an array (there is no All()/Iter()
	// or per-element Must family); Count()/At() remain on the array view;
	// whole-array trimming is xdr.Raw. Materializing a slice is the caller's
	// three-line loop.

	// Scan() cursor.
	if err := emitArrayCursor(f, ip); err != nil {
		return err
	}

	return nil
}

// emitArrayAt emits the At(i) accessor for an array view. The body varies along
// two axes: whether the count is variable (read+validate from the wire) vs fixed
// (a literal), and whether elements are fixed-size (O(1) offset) vs variable
// (walked via arrayTraverse). The printer g must already carry the typeName,
// elemType, elemSize, startOff, count, and maxLen variables.
func emitArrayAt(g Printer, isVarCount, isFixedElem bool) {
	g.L("func (v $typeName) At(i int) ($elemType, error) {")
	g.L("	var zero $elemType")
	if isVarCount {
		g.L("	count, err := arrayViewCount([]byte(v), $maxLen)")
		g.L("	if err != nil { return zero, err }")
		g.L("	if i < 0 || i >= count { return zero, viewErrIndexOutOfRange(0, i, count) }")
	} else {
		g.L("	if i < 0 || i >= $count { return zero, viewErrIndexOutOfRange(0, i, $count) }")
	}
	if isFixedElem {
		g.L("	off64 := int64($startOff) + int64(i)*int64($elemSize)")
		g.L("	if off64+int64($elemSize) > int64(len(v)) { return zero, viewErrShortBuffer(uint32(off64), \"need $elemSize bytes\") }")
		g.L("	return $elemType(v[int(off64):]), nil")
	} else {
		// off, err := ... declares off (and err, when not already declared by the
		// var-count branch above; := is valid as long as one LHS name is new).
		g.L("	off, err := arrayTraverse([]byte(v), i, $startOff, func(d []byte) (int, error) { return $elemType(d).size(0) })")
		g.L("	if err != nil { return zero, err }")
		g.L("	if off >= len(v) { return zero, viewErrShortBuffer(uint32(off), \"element offset exceeds data\") }")
		g.L("	return $elemType(v[off:]), nil")
	}
	g.L("}")
}

// emitArrayCursor emits the by-value Scan() cursor for an array view.
// Next() advances by walking the current element once,
// capturing what Elem()/Bytes() need; for struct elements Elem() returns the
// located Fields bundle, otherwise the plain trimmed element view.
func emitArrayCursor(f *GeneratedFile, ip *InlineTypePlan) error {
	vt := ip.ViewType
	typeName := ip.Name
	elemType := vt.Array.Element.GoType
	elemSize, isFixedElem := vt.Array.Element.FixedSize()
	isVarCount := vt.Array.Count == 0

	// Count validation divisor: the element's clamped minimum wire size, shared
	// with the standalone Count() accessor (arrayMinElemW), with the build-time
	// assert that no array element computes a zero minimum.
	minElemW, err := arrayMinElemW(ip)
	if err != nil {
		return err
	}

	cursorType := typeName + "Cursor"
	// Elem() returns the located Fields bundle for struct elements, the plain
	// trimmed element view otherwise.
	isStructElem := ip.ElemFieldsType != ""
	elemReturnType := elemType
	if isStructElem {
		elemReturnType = ip.ElemFieldsType
	}

	startOff := 0
	if isVarCount {
		startOff = 4
	}
	var countExpr any = vt.Array.Count
	if isVarCount {
		countExpr = "count"
	}

	g := f.Use(
		"typeName", typeName,
		"cursorType", cursorType,
		"elemType", elemType,
		"elemReturnType", elemReturnType,
		"elemSize", elemSize,
		"minElemW", minElemW,
		"maxLen", vt.Array.MaxLen,
		"startOff", startOff,
		"countExpr", countExpr,
	)

	// Cursor struct: buf/off/count/index/err plus the captured current element.
	g.L("// $cursorType is the Scan() cursor over $typeName. It is returned by value: plain loops allocate nothing.")
	g.L("type $cursorType struct {")
	g.L("	buf   []byte")
	g.L("	off   int")
	g.L("	count int")
	g.L("	index int")
	if isStructElem {
		g.L("	elem  $elemReturnType")
	} else {
		g.L("	elem  $elemType")
	}
	// valid is true exactly while the cursor is positioned on an element yielded
	// by a successful Next() (false before the first Next, after exhaustion, and
	// after an error). Elem()/Bytes() guard on it.
	g.L("	valid bool")
	g.L("	err   error")
	g.L("}")

	// Scan() — by-value cursor, count validated at construction.
	g.L("// Scan returns a by-value cursor over the array's elements. The wire count is validated against the buffer at construction.")
	g.L("func (v $typeName) Scan() $cursorType {")
	if isVarCount {
		g.L("	count, err := arrayViewCountChecked([]byte(v), $maxLen, $minElemW)")
		g.L("	return $cursorType{buf: []byte(v), off: $startOff, count: count, index: -1, err: err}")
	} else {
		g.L("	return $cursorType{buf: []byte(v), off: $startOff, count: $countExpr, index: -1}")
	}
	g.L("}")

	// Next()
	emitArrayCursorNext(g, ip, isStructElem, isFixedElem)

	// Read-side accessors: Count/Index/Elem/Bytes/Err/armMisuse.
	emitArrayCursorAccessors(g, isStructElem)

	return nil
}

// emitArrayCursorAccessors emits the cursor's read-side API: Count, Index, Elem,
// Bytes, Err, and the internal armMisuse helper. Elem/Bytes share a sticky
// misuse-error contract when the cursor is not positioned on a valid element.
// The printer g must already carry the cursorType and elemReturnType variables.
func emitArrayCursorAccessors(g Printer, isStructElem bool) {
	// Count() — validated count, safe for preallocation.
	g.L("// Count returns the validated element count, safe for preallocation.")
	g.L("func (c *$cursorType) Count() int { return c.count }")

	// Index()
	g.L("// Index returns the 0-based position of the current element, or -1 before the first Next().")
	g.L("func (c *$cursorType) Index() int { return c.index }")

	// Elem() — located bundle for struct elements, trimmed view otherwise.
	g.L("// Elem returns the current element. Before the first Next(), after Next() returns false,")
	g.L("// or after an error, it returns the zero value and arms a sticky misuse error.")
	g.L("func (c *$cursorType) Elem() $elemReturnType {")
	g.L("	if !c.valid {")
	g.L("		c.armMisuse()")
	g.L("		var zero $elemReturnType")
	g.L("		return zero")
	g.L("	}")
	g.L("	return c.elem")
	g.L("}")

	// Bytes() — current element's exact wire bytes (free).
	g.L("// Bytes returns the current element's exact wire bytes (captured during Next, zero cost). Same misuse contract as Elem().")
	g.L("func (c *$cursorType) Bytes() []byte {")
	g.L("	if !c.valid {")
	g.L("		c.armMisuse()")
	g.L("		return nil")
	g.L("	}")
	if isStructElem {
		g.L("	return []byte(c.elem.View)")
	} else {
		g.L("	return []byte(c.elem)")
	}
	g.L("}")

	// Err()
	g.L("// Err returns the first error encountered, if any.")
	g.L("func (c *$cursorType) Err() error { return c.err }")

	// armMisuse — arms a sticky cursor-misuse error (first error wins).
	g.L("func (c *$cursorType) armMisuse() {")
	g.L("	if c.err == nil { c.err = errCursorMisuse }")
	g.L("}")
}

// emitArrayCursorNext emits the cursor's Next() method, whose advance strategy
// depends on the element shape: a struct element is located in one walk (capturing
// field extents), a fixed-size element advances by a constant offset, and a
// variable-size element is sized once before being trimmed. The printer g must
// already carry the cursorType, elemType, and elemSize variables.
func emitArrayCursorNext(g Printer, ip *InlineTypePlan, isStructElem, isFixedElem bool) {
	g.L("// Next advances to the next element, walking it once and capturing what Elem()/Bytes() need. Returns false at end or on error.")
	g.L("func (c *$cursorType) Next() bool {")
	g.L("	c.valid = false")
	g.L("	if c.err != nil || c.index+1 >= c.count { return false }")
	switch {
	case isStructElem:
		// Struct element: locate the element (one walk, captures field extents).
		// The locate-fn name is carried on the plan (ElemLocateFn), not recovered
		// by string-stripping the Fields bundle type name.
		g.Set("locateFn", ip.ElemLocateFn).Block(`
				if uint(c.off) > uint(len(c.buf)) {
					c.err = viewErrShortBuffer(uint32(c.off), "element offset exceeds data")
					return false
				}
				fields, err := $locateFn($elemType(c.buf[c.off:]))
				if err != nil { c.err = err; return false }
				c.elem = fields
				c.off += len(fields.View)
				c.index++
				c.valid = true
				return true
		`)
	case isFixedElem:
		g.Block(`
				if c.off+$elemSize > len(c.buf) {
					c.err = viewErrShortBuffer(uint32(c.off), "need $elemSize bytes")
					return false
				}
				c.elem = $elemType(c.buf[c.off : c.off+$elemSize])
				c.off += $elemSize
				c.index++
				c.valid = true
				return true
		`)
	default:
		g.Block(`
				if uint(c.off) > uint(len(c.buf)) {
					c.err = viewErrShortBuffer(uint32(c.off), "element offset exceeds data")
					return false
				}
				sz, err := $elemType(c.buf[c.off:]).size(0)
				if err != nil { c.err = err; return false }
				if c.off+sz > len(c.buf) {
					c.err = viewErrShortBuffer(uint32(c.off), "element extends beyond data")
					return false
				}
				c.elem = $elemType(c.buf[c.off : c.off+sz])
				c.off += sz
				c.index++
				c.valid = true
				return true
		`)
	}
	g.L("}")
}

// emitOptionalType generates a concrete optional view type.
func emitOptionalType(f *GeneratedFile, typeName string, vt *ViewType) {
	innerType := vt.Optional.Element.GoType
	g := f.Use("typeName", typeName, "innerType", innerType)
	g.Block(`
		type $typeName []byte

		func (o $typeName) Unwrap() ($innerType, bool, error) {
			var zero $innerType
			if len(o) < 4 { return zero, false, viewErrShortBuffer(0, "need 4 bytes for optional flag") }
			flag := binary.BigEndian.Uint32(o[:4])
			switch flag {
			case 0: return zero, false, nil
			case 1: return $innerType(o[4:]), true, nil
			default: return zero, false, viewErrBadBoolValue(0, flag)
			}
		}
		func (o $typeName) MustUnwrap() ($innerType, bool) { return must2(o.Unwrap()) }
	`)
	for _, m := range []struct{ name, call string }{{"size", "size(depth + 1)"}, {"valid", "valid(depth + 1)"}} {
		g.Set("method", m.name).Set("call", m.call).Block(`
			func (o $typeName) $method(depth int) (int, error) {
				if depth > maxDepth { return 0, viewErrMaxDepth(0) }
				if len(o) < 4 { return 0, viewErrShortBuffer(0, "need 4 bytes for optional flag") }
				flag := binary.BigEndian.Uint32(o[:4])
				switch flag {
				case 0: return 4, nil
				case 1:
					sz, err := $innerType(o[4:]).$call
					if err != nil { return 0, err }
					return 4 + sz, nil
				default: return 0, viewErrBadBoolValue(0, flag)
				}
			}
		`)
	}
}

// emitOpaqueType generates a concrete opaque view type.
// Fixed (RawSize > 0): constant size, padding validation. Value() returns the
// schema Go type the struct decoder produces — a typed [N]byte array (Hash,
// [4]byte, ...) BY VALUE (a copy), not an aliasing []byte.
// Variable bounded (MaxLen > 0): delegates to VarOpaqueView, enforces max length;
// Value() returns an aliasing []byte.
func emitOpaqueType(f *GeneratedFile, ip *InlineTypePlan) {
	typeName := ip.Name
	vt := ip.ViewType
	g := f.Use("typeName", typeName)
	g.L("type $typeName []byte")

	if vt.Opaque.RawSize > 0 {
		paddedSize, _ := vt.FixedSize()
		valGoType := ip.OpaqueValueGoType
		if valGoType == "" {
			valGoType = fmt.Sprintf("[%d]byte", vt.Opaque.RawSize)
		}
		h := g.Set("paddedSize", paddedSize).Set("rawSize", vt.Opaque.RawSize).Set("valGoType", valGoType)
		if vt.Opaque.RawSize%4 != 0 {
			h = h.Set("padLen", paddedSize-vt.Opaque.RawSize)
			h.Block(`
				func (v $typeName) Value() ($valGoType, error) {
					var out $valGoType
					if len(v) < $paddedSize { return out, viewErrShortBuffer(0, "need $paddedSize bytes") }
					if !bytes.Equal([]byte(v)[$rawSize:$paddedSize], zeroPad[:$padLen]) {
						return out, viewErrNonZeroPadding($rawSize)
					}
					copy(out[:], []byte(v)[:$rawSize])
					return out, nil
				}
			`)
		} else {
			h.Block(`
				func (v $typeName) Value() ($valGoType, error) {
					var out $valGoType
					if len(v) < $paddedSize { return out, viewErrShortBuffer(0, "need $paddedSize bytes") }
					copy(out[:], []byte(v)[:$rawSize])
					return out, nil
				}
			`)
		}
		h.L("func (v $typeName) size(_ int) (int, error) { return $paddedSize, nil }")
		emitValueBasedValid(f, typeName)
		h.L("func (v $typeName) MustValue() $valGoType { return must(v.Value()) }")
	} else {
		g = g.Set("maxLen", vt.Opaque.MaxLen)
		g.Block(`
			func (v $typeName) Value() ([]byte, error) {
				val, err := VarOpaqueView(v).Value()
				if err != nil { return nil, err }
				if len(val) > $maxLen { return nil, viewErrOpaqueExceedsMax(0, uint32(len(val)), $maxLen) }
				return val, nil
			}
		`)
		g.L("func (v $typeName) size(depth int) (int, error) { return VarOpaqueView(v).size(depth) }")
		emitValueBasedValid(f, typeName)
		g.L("func (v $typeName) MustValue() []byte { return must(v.Value()) }")
	}
}

// emitEnumViewFromPlan emits an enum view type.
func emitEnumViewFromPlan(f *GeneratedFile, ep *EnumViewPlan) {
	p := f.Use("viewName", ep.ViewTypeName, "enumName", ep.EnumName, "caseNames", joinComma(ep.CaseNames))
	p.Block(`
		type $viewName []byte

		func (v $viewName) Value() ($enumName, error) {
			if len(v) < 4 { return 0, viewErrShortBuffer(0, "need 4 bytes") }
			val := $enumName(int32(binary.BigEndian.Uint32(v[:4])))
			switch val {
			case $caseNames:
				return val, nil
			default:
				return 0, viewErrUnknownDiscriminant(0, int32(val))
			}
		}
		func (v $viewName) size(_ int) (int, error) { return 4, nil }
	`)
	emitValueBasedValid(f, ep.ViewTypeName)
	p.L("func (v $viewName) MustValue() $enumName { return must(v.Value()) }")
}

// emitTypedefViewFromPlan emits a typedef alias.
func emitTypedefViewFromPlan(f *GeneratedFile, tp *TypedefViewPlan) {
	if tp.ViewType.GoType == tp.AliasName {
		return
	}
	p := f.Use("aliasName", tp.AliasName, "goType", tp.ViewType.GoType)
	p.L("type $aliasName = $goType")
}
