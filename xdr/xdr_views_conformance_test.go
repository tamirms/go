package xdr_test

// xdr_views_conformance_test.go is the conformance harness for the XDR views.
// It differentially tests the generated view API
// (Fields()/Fields_(), Scan() cursors, At/Count/Index/Bytes, leaf Value(),
// decoded union discriminants, and the xdr.Raw/Validate package generics —
// reached via the test-only reflection bridges in views_export_test.go)
// against the struct decoder as oracle, across the schema, on a deterministic
// randxdr-seeded corpus of valid values plus byte-level mutations.
//
// Approach: a HAND-WRITTEN reflection/randxdr-driven harness rather than an
// IR-generated one. The randxdr generator produces valid wire bytes for any
// goxdr shape, the SDK struct decoder is the oracle (its MarshalBinary on any
// sub-node yields that node's canonical wire bytes), and the generated view
// methods are uniformly named, so a single reflective lockstep walker descends
// the decoded struct value and the view bytes together and checks every
// invariant at every node. This reaches the full transitive closure of a
// curated set of root types without emitting hundreds of bespoke per-type test
// functions (which would be a large second codegen surface with its own
// correctness story). Coverage is reported honestly via t.Logf: the set of
// distinct view types actually exercised, and the count of nodes skipped with
// reasons.
//
// COVERAGE (honest): the harness drives the transitive closure of the 17 roots
// in conformanceRoots(). It deeply recurses the FIRST element of each array and
// the ACTIVE arm of each union per sample, but the deterministic randxdr corpus
// (perRoot samples, fixed seed) exercises many discriminant values across
// samples, so most union arms are reached over the corpus. It additionally
// drives each union discriminant's STANDALONE enum leaf view (e.g.
// LedgerEntryTypeView), reachable nowhere else — closing a real blind spot where
// a bug in a discriminant enum's Value() would otherwise go uncaught. As of this
// writing that reaches ~394 of the 623 generated view types
// (TestConformance_ValidCorpus logs the live number). The remainder are types
// unreachable from these roots (archive/bucket-detail/result-detail subtrees the
// roots never instantiate) and a few rarely-discriminated union arms. The
// reachable discriminant-enum set is gated by
// TestConformance_DiscriminantLeafViews so a reachable type can never silently
// drop out of the leaf-view checks.
//
// Invariants checked:
//   - Tiling (structs): Fields().View bytes == the node's exact wire bytes
//     (== Raw()); concatenation of trimmed per-field extents == node bytes
//     (pins every internal field boundary).
//   - Yield agreement (arrays): Scan() element sequence matches len/elements of
//     the decoded slice; Bytes() == each element's marshaled bytes; At(i)
//     agrees; Index() increments; Count() matches.
//   - Value agreement (leaves): leaf Value() reached via the view agrees with the
//     decoded struct field (checked directly for comparable scalar/opaque/enum
//     leaves, and universally pinned by Raw()==MarshalBinary()).
//   - Error agreement (mutated/truncated inputs): where the decoder rejects the
//     bytes, the view path returns an error and never panics; on every mutation,
//     Fields()/Scan()/Value()/Raw() never panic.
//   - Count validation: a buffer declaring an impossibly large array count is
//     rejected by the checked-count helper (the O(1) min-element-width bound),
//     not OOM'd.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	goxdr "github.com/xdrpp/goxdr/xdr"

	"github.com/stellar/go-stellar-sdk/gxdr"
	"github.com/stellar/go-stellar-sdk/randxdr"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// conformanceRoot pairs a gxdr shape constructor (for randxdr generation) with
// the SDK struct decoder and the root view type. The transitive closure of
// these roots reaches the overwhelming majority of view types.
type conformanceRoot struct {
	name     string
	newShape func() goxdr.XdrType                              // fresh gxdr shape to populate via randxdr
	newDest  func() interface{ UnmarshalBinary([]byte) error } // fresh SDK struct decoder
	viewType reflect.Type                                      // the root view type (a named []byte)
}

func vt(sample interface{}) reflect.Type { return reflect.TypeOf(sample) }

// conformanceRoots is the curated root set. LedgerCloseMeta alone transitively
// covers most of the schema (envelopes, ledger entries, results, metas, SCP,
// Soroban); the rest broaden coverage into subtrees that LCM does not always
// instantiate (e.g. all union arms of standalone types).
//
// It is a flat, declarative table — one entry per root type, no logic — so the
// funlen budget is waived rather than scattering the curated list across helpers.
//
//nolint:funlen // declarative root table, no logic to extract.
func conformanceRoots() []conformanceRoot {
	return []conformanceRoot{
		{
			name:     "LedgerCloseMeta",
			newShape: func() goxdr.XdrType { return &gxdr.LedgerCloseMeta{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.LedgerCloseMeta{} },
			viewType: vt(xdr.LedgerCloseMetaView(nil)),
		},
		{
			name:     "TransactionEnvelope",
			newShape: func() goxdr.XdrType { return &gxdr.TransactionEnvelope{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.TransactionEnvelope{} },
			viewType: vt(xdr.TransactionEnvelopeView(nil)),
		},
		{
			name:     "LedgerEntry",
			newShape: func() goxdr.XdrType { return &gxdr.LedgerEntry{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.LedgerEntry{} },
			viewType: vt(xdr.LedgerEntryView(nil)),
		},
		{
			name:     "TransactionMeta",
			newShape: func() goxdr.XdrType { return &gxdr.TransactionMeta{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.TransactionMeta{} },
			viewType: vt(xdr.TransactionMetaView(nil)),
		},
		{
			name:     "TransactionResult",
			newShape: func() goxdr.XdrType { return &gxdr.TransactionResult{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.TransactionResult{} },
			viewType: vt(xdr.TransactionResultView(nil)),
		},
		{
			name:     "LedgerHeaderHistoryEntry",
			newShape: func() goxdr.XdrType { return &gxdr.LedgerHeaderHistoryEntry{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.LedgerHeaderHistoryEntry{} },
			viewType: vt(xdr.LedgerHeaderHistoryEntryView(nil)),
		},
		{
			name:     "StellarValue",
			newShape: func() goxdr.XdrType { return &gxdr.StellarValue{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.StellarValue{} },
			viewType: vt(xdr.StellarValueView(nil)),
		},
		{
			name:     "SorobanAuthorizationEntry",
			newShape: func() goxdr.XdrType { return &gxdr.SorobanAuthorizationEntry{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.SorobanAuthorizationEntry{} },
			viewType: vt(xdr.SorobanAuthorizationEntryView(nil)),
		},
		{
			name:     "DiagnosticEvent",
			newShape: func() goxdr.XdrType { return &gxdr.DiagnosticEvent{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.DiagnosticEvent{} },
			viewType: vt(xdr.DiagnosticEventView(nil)),
		},
		{
			name:     "SorobanTransactionData",
			newShape: func() goxdr.XdrType { return &gxdr.SorobanTransactionData{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.SorobanTransactionData{} },
			viewType: vt(xdr.SorobanTransactionDataView(nil)),
		},
		// Broadening roots: standalone types that LedgerCloseMeta does not always
		// instantiate, covering the SCP-history, bucket, ledger-key, operation,
		// operation-result, and SCVal subtrees. (gxdr uses acronym caps —
		// SCVal/SCPHistoryEntry — while the SDK struct and view use GoTypeName
		// normalization — ScVal/ScpHistoryEntry.)
		{
			name:     "ScVal",
			newShape: func() goxdr.XdrType { return &gxdr.SCVal{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.ScVal{} },
			viewType: vt(xdr.ScValView(nil)),
		},
		{
			name:     "Operation",
			newShape: func() goxdr.XdrType { return &gxdr.Operation{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.Operation{} },
			viewType: vt(xdr.OperationView(nil)),
		},
		{
			name:     "OperationResult",
			newShape: func() goxdr.XdrType { return &gxdr.OperationResult{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.OperationResult{} },
			viewType: vt(xdr.OperationResultView(nil)),
		},
		{
			name:     "BucketEntry",
			newShape: func() goxdr.XdrType { return &gxdr.BucketEntry{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.BucketEntry{} },
			viewType: vt(xdr.BucketEntryView(nil)),
		},
		{
			name:     "ScpHistoryEntry",
			newShape: func() goxdr.XdrType { return &gxdr.SCPHistoryEntry{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.ScpHistoryEntry{} },
			viewType: vt(xdr.ScpHistoryEntryView(nil)),
		},
		{
			name:     "LedgerKey",
			newShape: func() goxdr.XdrType { return &gxdr.LedgerKey{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.LedgerKey{} },
			viewType: vt(xdr.LedgerKeyView(nil)),
		},
		{
			name:     "TransactionSignaturePayload",
			newShape: func() goxdr.XdrType { return &gxdr.TransactionSignaturePayload{} },
			newDest:  func() interface{ UnmarshalBinary([]byte) error } { return &xdr.TransactionSignaturePayload{} },
			viewType: vt(xdr.TransactionSignaturePayloadView(nil)),
		},
	}
}

// randxdrPresets collapses the recursive subtrees randxdr would otherwise
// explode. The standard LedgerCloseMetaPresets cover the two recursive XDR
// shapes (nested inner tx sets, deep authorized-invocation trees) that appear
// across all roots.
var randxdrPresets = randxdr.LedgerCloseMetaPresets

// --- coverage tracking -------------------------------------------------------

type coverage struct {
	exercised map[string]int // view type name -> node count checked
	skipped   map[string]int // skip reason -> count
	checks    int            // total invariant assertions made
}

func newCoverage() *coverage {
	return &coverage{exercised: map[string]int{}, skipped: map[string]int{}}
}

func (c *coverage) hit(viewTypeName string) { c.exercised[viewTypeName]++ }
func (c *coverage) skip(reason string)      { c.skipped[reason]++ }
func (c *coverage) check()                  { c.checks++ }

func (c *coverage) report(t *testing.T) {
	t.Helper()
	names := make([]string, 0, len(c.exercised))
	for n := range c.exercised {
		names = append(names, n)
	}
	sort.Strings(names)
	t.Logf("conformance coverage: %d distinct view types exercised, %d total invariant checks", len(names), c.checks)
	reasons := make([]string, 0, len(c.skipped))
	for r := range c.skipped {
		reasons = append(reasons, r)
	}
	sort.Strings(reasons)
	for _, r := range reasons {
		t.Logf("  skipped: %-40s x%d", r, c.skipped[r])
	}
}

// --- the lockstep walker -----------------------------------------------------

// walker descends a decoded struct value (the oracle) and a view in lockstep,
// asserting every invariant. It never panics: panics inside view methods are
// recovered and reported as test failures (a conformance violation), because
// views must error rather than panic.
type walker struct {
	t   *testing.T
	cov *coverage
	// collect, when non-nil, records one valid sample of the trimmed wire bytes
	// for each distinct view type reached, so the mutation sweep can exercise
	// the no-panic contract over the whole transitive type set, not just roots.
	collect map[reflect.Type][]byte
}

// errf records a conformance failure. We keep going so the coverage report and
// other invariants are still produced; one failure does not mask the rest.
func (w *walker) errf(format string, args ...interface{}) {
	w.t.Errorf(format, args...)
}

// callView invokes a no-arg view method by name on a view-typed []byte, with
// panic recovery. Returns the results and a recovered panic value (nil if none).
func callMethod(recv reflect.Value, name string, args ...reflect.Value) (out []reflect.Value, panicked interface{}) {
	defer func() { panicked = recover() }()
	m := recv.MethodByName(name)
	if !m.IsValid() {
		return nil, fmt.Sprintf("method %s not found on %s", name, recv.Type())
	}
	return m.Call(args), nil
}

// viewValueOf wraps wire bytes in a view of the given named-[]byte view type.
func viewValueOf(viewType reflect.Type, data []byte) reflect.Value {
	return reflect.ValueOf(data).Convert(viewType)
}

// marshal returns the canonical wire bytes for an SDK struct value (the oracle).
// sv must implement encoding.BinaryMarshaler via its value or pointer receiver.
// It refuses nil pointers (a nil-receiver MarshalBinary panics) and recovers any
// panic, returning ok=false instead.
func marshal(sv reflect.Value) (data []byte, ok bool) {
	if sv.Kind() == reflect.Ptr && sv.IsNil() {
		return nil, false
	}
	defer func() {
		if recover() != nil {
			data, ok = nil, false
		}
	}()
	m := sv.MethodByName("MarshalBinary")
	if !m.IsValid() && sv.CanAddr() {
		m = sv.Addr().MethodByName("MarshalBinary")
	}
	if !m.IsValid() {
		// Try a pointer copy.
		p := reflect.New(sv.Type())
		p.Elem().Set(sv)
		m = p.MethodByName("MarshalBinary")
	}
	if !m.IsValid() {
		return nil, false
	}
	res := m.Call(nil)
	if !res[1].IsNil() {
		return nil, false
	}
	return res[0].Bytes(), true
}

// oracleFieldBytes returns the wire bytes of an SDK struct field as the oracle
// would encode it within its parent. For XDR optionals (Go pointer fields) the
// wire form is a 4-byte presence flag optionally followed by the inner value's
// bytes; the inner value's own MarshalBinary does not include the flag, so we
// reconstruct it here.
func oracleFieldBytes(fv reflect.Value) ([]byte, bool) {
	if fv.Kind() == reflect.Ptr {
		if fv.IsNil() {
			return []byte{0, 0, 0, 0}, true
		}
		inner, ok := marshal(fv.Elem())
		if !ok {
			return nil, false
		}
		return append([]byte{0, 0, 0, 1}, inner...), true
	}
	return marshal(fv)
}

// isViewType reports whether t is a named []byte view type (our convention:
// underlying kind Slice of uint8, named, in the xdr package).
func isViewType(t reflect.Type) bool {
	return t != nil && t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 && t.Name() != ""
}

// rawOf trims a view to its exact wire extent via the xdr.Raw generic, exposed
// to this reflection harness through the test-only xdr.RawValue bridge (Raw() is
// a package generic, not a per-type method). ok is false on panic.
func rawOf(view reflect.Value) (data []byte, err error, ok bool) {
	defer func() {
		if recover() != nil {
			data, err, ok = nil, nil, false
		}
	}()
	data, err = xdr.RawValue(view.Interface())
	return data, err, true
}

// copyOf returns an independent copy of a view via the xdr.Copy generic (the
// test-only CopyValue bridge). ok is false on panic. Exercises the previously
// untested Copy path (spec 4a).
func copyOf(view reflect.Value) (data []byte, err error, ok bool) {
	defer func() {
		if recover() != nil {
			data, err, ok = nil, nil, false
		}
	}()
	res, cErr := xdr.CopyValue(view.Interface())
	if cErr != nil {
		return nil, cErr, true
	}
	return asViewBytes(res), nil, true
}

// asViewBytes extracts the underlying []byte of a named-[]byte view value.
func asViewBytes(v any) []byte {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice || rv.Type().Elem().Kind() != reflect.Uint8 {
		return nil
	}
	return rv.Bytes()
}

// checkRawAndCopy verifies the whole-node tiling invariants for a view: Raw()
// must equal the oracle's exact extent, and Copy() must return the same bytes
// without aliasing the original buffer. It returns the trimmed node extent that
// child offsets line up against; ok is false (caller should abort) when the
// oracle has no usable extent or a Raw()/equality check failed.
func (w *walker) checkRawAndCopy(sv reflect.Value, data []byte, view reflect.Value, viewType reflect.Type) (node []byte, ok bool) {
	// For an XDR optional (Go pointer node) the wire form includes the 4-byte
	// presence flag, which the inner value's own MarshalBinary omits; use the
	// optional-aware oracle so the bytes line up.
	want, okM := oracleFieldBytes(sv)
	if !okM {
		// Inline/anonymous composite oracles (e.g. []xdr.Operation, [4]xdr.Hash)
		// and leaf scalars do not implement MarshalBinary on their own. Their
		// exact extent is the caller-provided trimmed bytes (`data`), already
		// pinned against the oracle by the parent's per-field boundary check.
		// Trust `data` as the node extent for those.
		if !isLeafKind(sv.Kind(), sv.Type()) && sv.Kind() != reflect.Slice && sv.Kind() != reflect.Array {
			w.cov.skip("oracle MarshalBinary unavailable")
			return nil, false
		}
		want = data
	}
	gotRaw, rawErr, rawOK := rawOf(view)
	if !rawOK {
		w.errf("%s: Raw() panicked on valid input", viewType.Name())
		return nil, false
	}
	w.cov.check()
	if rawErr != nil {
		w.errf("%s: Raw() errored on valid input: %v", viewType.Name(), rawErr)
		return nil, false
	}
	if !bytes.Equal(gotRaw, want) {
		w.errf("%s: Raw() (%d bytes) != oracle/extent (%d bytes) mismatch", viewType.Name(), len(gotRaw), len(want))
		return nil, false
	}

	// Copy() agreement (spec 4a, previously untested): xdr.Copy must return the
	// same trimmed bytes as Raw() and must NOT alias the original buffer.
	if cp, cpErr, cpOk := copyOf(view); cpOk {
		if cpErr != nil {
			w.errf("%s: Copy() errored on valid input: %v", viewType.Name(), cpErr)
		} else {
			if !bytes.Equal(cp, gotRaw) {
				w.errf("%s: Copy() bytes != Raw() (%d vs %d)", viewType.Name(), len(cp), len(gotRaw))
			}
			if len(cp) > 0 && len(gotRaw) > 0 && &cp[0] == &gotRaw[0] {
				w.errf("%s: Copy() aliases the original buffer", viewType.Name())
			}
			w.cov.check()
		}
	} else {
		w.errf("%s: Copy() panicked on valid input", viewType.Name())
	}

	return want, true
}

// walk recurses the (struct value, view bytes, view type) triple.
func (w *walker) walk(sv reflect.Value, data []byte, viewType reflect.Type, depth int) {
	if depth > 40 {
		w.cov.skip("max recursion depth (cycle guard)")
		return
	}
	if !isViewType(viewType) {
		w.cov.skip("non-view type at node")
		return
	}
	w.cov.hit(viewType.Name())

	view := viewValueOf(viewType, data)

	// Raw/Copy whole-node tiling invariant; node is the exact trimmed extent.
	node, ok := w.checkRawAndCopy(sv, data, view, viewType)
	if !ok {
		return
	}

	// Record one valid sample per view type for the mutation sweep.
	if w.collect != nil {
		if _, seen := w.collect[viewType]; !seen {
			cp := make([]byte, len(node))
			copy(cp, node)
			w.collect[viewType] = cp
		}
	}

	// Dispatch on the oracle's kind.
	switch sv.Kind() {
	case reflect.Struct:
		w.walkStruct(sv, node, viewType, depth)
	case reflect.Slice:
		// A []byte oracle field is an XDR opaque or string (a leaf), not an XDR
		// var-array. Only slices of non-byte elements are XDR arrays with a
		// Scan() cursor.
		if sv.Type().Elem().Kind() == reflect.Uint8 {
			w.walkLeaf(sv, node, viewType)
		} else {
			w.walkArray(sv, node, viewType, depth)
		}
	case reflect.Array:
		// [N]byte is a fixed opaque (leaf); [N]T (T non-byte) is an XDR fixed
		// array with At()/Len() but no Scan() cursor.
		if sv.Type().Elem().Kind() == reflect.Uint8 {
			w.walkLeaf(sv, node, viewType)
		} else {
			w.walkFixedArray(sv, node, viewType, depth)
		}
	case reflect.Ptr:
		// Optional.
		w.walkOptional(sv, node, viewType, depth)
	default:
		w.walkLeaf(sv, node, viewType)
	}
}

// isUnionStruct reports whether the SDK struct value is an XDR union (it has a
// SwitchFieldName method generated for unions).
func isUnionStruct(sv reflect.Value) bool {
	m := sv.MethodByName("SwitchFieldName")
	if m.IsValid() {
		return true
	}
	if sv.CanAddr() {
		return sv.Addr().MethodByName("SwitchFieldName").IsValid()
	}
	p := reflect.New(sv.Type())
	p.Elem().Set(sv)
	return p.MethodByName("SwitchFieldName").IsValid()
}

// walkStructField checks one struct field's bundle bytes (bf, captured as
// fieldBytes) against the oracle field (fv) and deep-recurses into it. Fields
// with a standalone MarshalBinary are compared to the oracle's wire bytes;
// inline/anonymous fields trust the bundle's trimmed extent; otherwise only the
// bundle field's own Raw() is validated by the recursion.
func (w *walker) walkStructField(
	fv, bf reflect.Value, viewType reflect.Type, fieldName string, fieldBytes []byte, depth int,
) {
	// Field bytes must equal the oracle field's wire bytes (handling optionals,
	// whose wire form includes the presence flag).
	want, okM := oracleFieldBytes(fv)
	switch {
	case okM:
		if !bytes.Equal(fieldBytes, want) {
			w.errf("%s.%s: Fields() field bytes != oracle (%d vs %d)", viewType.Name(), fieldName, len(fieldBytes), len(want))
		}
		w.cov.check()
		// Recurse with the field's exact bytes (want, the trimmed extent).
		w.walk(fv, want, bf.Type(), depth+1)
	case isLeafKind(fv.Kind(), fv.Type()) || fv.Kind() == reflect.Slice || fv.Kind() == reflect.Array:
		// Inline/anonymous fields (leaf scalars, inline var-arrays like
		// []Operation, inline fixed arrays like [4]Hash) have no standalone
		// MarshalBinary; their exact extent is the bundle field bytes. Recurse with
		// those — walk() trusts the caller-provided extent for these kinds and
		// still checks Raw() == extent and sub-invariants.
		w.walk(fv, fieldBytes, bf.Type(), depth+1)
	default:
		// Oracle can't marshal this field directly (rare for true XDR fields).
		// Fall back to validating the bundle field's own Raw().
		w.cov.skip("oracle field not marshalable; bundle-only check")
	}
}

func (w *walker) walkStruct(sv reflect.Value, node []byte, viewType reflect.Type, depth int) {
	if isUnionStruct(sv) {
		w.walkUnion(sv, node, viewType, depth)
		return
	}

	// Fields() (or Fields_() for the SCSpecUDTStructV0 escape). The bundle's
	// first field is View (the whole node trimmed); the rest correspond 1:1, in
	// order, to the exported XDR struct fields.
	fieldsMethod := "Fields"
	view := viewValueOf(viewType, node)
	if !view.MethodByName("Fields").IsValid() && view.MethodByName("Fields_").IsValid() {
		fieldsMethod = "Fields_"
	}
	res, p := callMethod(view, fieldsMethod)
	if p != nil {
		w.errf("%s: %s() panicked on valid input: %v", viewType.Name(), fieldsMethod, p)
		return
	}
	if len(res) != 2 {
		w.cov.skip("struct without Fields() method")
		return
	}
	if !res[1].IsNil() {
		w.errf("%s: %s() errored on valid input: %v", viewType.Name(), fieldsMethod, res[1].Interface())
		return
	}
	bundle := res[0]
	w.cov.check()

	// bundle.View == node.
	viewField := bundle.FieldByName("View")
	if !viewField.IsValid() {
		w.errf("%s: Fields bundle has no View field", viewType.Name())
		return
	}
	if !bytes.Equal(viewField.Bytes(), node) {
		w.errf("%s: Fields().View != node bytes (%d vs %d)", viewType.Name(), len(viewField.Bytes()), len(node))
	}
	w.cov.check()

	// Walk the XDR struct fields. The oracle struct may carry trailing
	// non-XDR/non-exported helper fields; map by position over exported fields.
	st := sv.Type()
	bundleIdx := 1 // skip View at index 0
	var concat []byte
	for i := 0; i < st.NumField(); i++ {
		sf := st.Field(i)
		if sf.PkgPath != "" {
			continue // unexported
		}
		if bundleIdx >= bundle.NumField() {
			// More struct fields than bundle fields: bundle is generated from
			// the IR, so this should not happen for a real XDR struct. Skip.
			w.cov.skip("oracle field has no bundle counterpart")
			continue
		}
		bf := bundle.Field(bundleIdx)
		bundleIdx++

		fieldBytes := bf.Bytes()
		concat = append(concat, fieldBytes...)
		w.walkStructField(sv.Field(i), bf, viewType, sf.Name, fieldBytes, depth)
	}

	// Concatenation of trimmed field extents == node (pins every boundary).
	if !bytes.Equal(concat, node) {
		w.errf("%s: concatenation of Fields() extents (%d) != node (%d) — internal boundary mismatch", viewType.Name(), len(concat), len(node))
	}
	w.cov.check()
}

// checkUnionDiscriminant verifies the view's discriminant accessor (named after
// the switch field, e.g. V()/Type()/C()) returns the DECODED value matching the
// oracle's discriminant field, and additionally drives the discriminant's
// standalone enum leaf view so a bug in that leaf's
// Value() bites here rather than going uncaught. Compared by integer value to
// cover enum/int/bool uniformly.
func (w *walker) checkUnionDiscriminant(sv reflect.Value, node []byte, viewType reflect.Type, switchName string) {
	discView := viewValueOf(viewType, node)
	res, p := callMethod(discView, switchName)
	if p != nil {
		// The discriminant accessor is the one method this check exists to
		// validate, and switchName names a real generated accessor here, so a
		// recovered panic on valid input is a genuine failure — surface it
		// rather than swallowing it (matches walkUnionArm / checkStruct).
		w.errf("%s.%s(): discriminant accessor panicked on valid input: %v", viewType.Name(), switchName, p)
		return
	}
	if len(res) != 2 {
		w.cov.skip("union discriminant accessor signature unexpected")
		return
	}
	if !res[1].IsNil() {
		w.errf("%s.%s(): discriminant errored on valid input: %v", viewType.Name(), switchName, res[1].Interface())
		return
	}
	oracleDisc := sv.FieldByName(switchName)
	if !oracleDisc.IsValid() {
		return
	}
	if got, want := discAsInt64(res[0]), discAsInt64(oracleDisc); got != want {
		w.errf("%s.%s(): decoded discriminant=%d, oracle=%d", viewType.Name(), switchName, got, want)
	}
	w.cov.check()

	// Standalone enum leaf view (e.g. LedgerEntryTypeView): reached nowhere else
	// for discriminant-only enums.
	leafVT, ok := enumLeafViewFor(oracleDisc)
	if !ok || len(node) < 4 {
		return
	}
	w.cov.hit(leafVT.Name())
	leaf := viewValueOf(leafVT, node[:4])
	lr, lp := callMethod(leaf, "Value")
	switch {
	case lp != nil:
		w.errf("%s discriminant leaf %s.Value() panicked on valid input", viewType.Name(), leafVT.Name())
	case len(lr) == 2 && lr[1].IsNil():
		if discAsInt64(lr[0]) != discAsInt64(oracleDisc) {
			w.errf("%s discriminant leaf %s.Value()=%d, oracle=%d", viewType.Name(), leafVT.Name(), discAsInt64(lr[0]), discAsInt64(oracleDisc))
		}
		w.cov.check()
	case len(lr) == 2:
		w.errf("%s discriminant leaf %s.Value() errored on valid input: %v", viewType.Name(), leafVT.Name(), lr[1].Interface())
	}
}

func (w *walker) walkUnion(sv reflect.Value, node []byte, viewType reflect.Type, depth int) {
	// Unions have no Fields(); Raw() (already checked) pins the whole node.
	// Recurse into the active arm. The SDK union struct stores the
	// discriminant in the field named by SwitchFieldName() and each arm as a
	// pointer field; exactly one (or none, for void arms) is non-nil.
	st := sv.Type()
	switchName := ""
	if m := methodOf(sv, "SwitchFieldName"); m.IsValid() {
		r := m.Call(nil)
		switchName = r[0].String()
	}

	// Decoded-discriminant agreement.
	if switchName != "" {
		w.checkUnionDiscriminant(sv, node, viewType, switchName)
	}

	// Find the matching arm view via the view's arm accessor. We descend into
	// the arm bytes, which on a trimmed union begin at offset 4 (after the
	// 4-byte discriminant). To get the arm view type we look for an active
	// non-nil pointer field and call the same-named view accessor.
	for i := 0; i < st.NumField(); i++ {
		sf := st.Field(i)
		if sf.PkgPath != "" || sf.Name == switchName {
			continue
		}
		fv := sv.Field(i)
		if fv.Kind() != reflect.Ptr || fv.IsNil() {
			continue
		}
		// Active arm found; validate and recurse into it (at most one is non-nil).
		w.walkUnionArm(node, viewType, sf, fv, depth)
		return
	}
	// No active arm pointer found: either a void arm or a primitive arm stored
	// inline. Raw() already validated the whole node; nothing more to recurse.
	w.cov.skip("union void/inline arm (no recursion)")
}

// walkUnionArm validates the active arm of a union (the non-nil pointer field sf)
// against the view's same-named arm accessor: the accessor must not error, must
// yield a view whose Raw() trims to the arm's extent and equals the oracle's arm
// bytes, then deep-recurses into the arm.
func (w *walker) walkUnionArm(node []byte, viewType reflect.Type, sf reflect.StructField, fv reflect.Value, depth int) {
	// The view's arm accessor is named after the field.
	armView := viewValueOf(viewType, node)
	res, p := callMethod(armView, sf.Name)
	if p != nil {
		w.errf("%s: arm accessor %s() panicked on valid input: %v", viewType.Name(), sf.Name, p)
		return
	}
	if len(res) != 2 {
		w.cov.skip("union arm accessor signature unexpected")
		return
	}
	if !res[1].IsNil() {
		w.errf("%s.%s(): errored on valid input: %v", viewType.Name(), sf.Name, res[1].Interface())
		return
	}
	armViewVal := res[0]
	if !isViewType(armViewVal.Type()) {
		w.cov.skip("union arm yields non-view (decoded discriminant or leaf)")
		return
	}
	// The SDK stores each union arm as a pointer to the arm value. An XDR optional
	// arm (e.g. SCVal's vec/map) is a double pointer: the inner pointer is the
	// optional, so its wire form carries the presence flag. oracleFieldBytes
	// handles both: a value inner marshals directly, a pointer inner gets the
	// optional flag prefix.
	armOracle := fv.Elem()
	armBytes, okM := oracleFieldBytes(armOracle)
	// The arm view (fat) Raw() trims to the arm's extent.
	gotRaw, rawErr, ok := rawOf(armViewVal)
	if !ok {
		w.errf("%s.%s(): Raw() panicked", viewType.Name(), sf.Name)
		return
	}
	if rawErr != nil {
		w.errf("%s.%s(): Raw() errored on valid input: %v", viewType.Name(), sf.Name, rawErr)
		return
	}
	switch {
	case okM:
		if !bytes.Equal(gotRaw, armBytes) {
			w.errf("%s.%s(): arm Raw() != oracle (%d vs %d)", viewType.Name(), sf.Name, len(gotRaw), len(armBytes))
		}
		w.cov.check()
		w.walk(armOracle, armBytes, armViewVal.Type(), depth+1)
	case isLeafKind(armOracle.Kind(), armOracle.Type()) || armOracle.Kind() == reflect.Slice || armOracle.Kind() == reflect.Array:
		// Inline arm value (e.g. []OperationResult, bool, string): no standalone
		// MarshalBinary; trust the arm view's own trimmed extent.
		w.cov.check()
		w.walk(armOracle, gotRaw, armViewVal.Type(), depth+1)
	default:
		w.cov.skip("union arm oracle not marshalable")
	}
}

func (w *walker) walkArray(sv reflect.Value, node []byte, viewType reflect.Type, depth int) {
	view := viewValueOf(viewType, node)

	// Count() agreement (the array view's Count, distinct from the cursor's).
	cm := view.MethodByName("Count")
	if cm.IsValid() {
		res, p := callMethod(view, "Count")
		if p != nil {
			w.errf("%s: Count() panicked on valid input", viewType.Name())
		} else if len(res) == 2 && res[1].IsNil() {
			if int(res[0].Int()) != sv.Len() {
				w.errf("%s: Count()=%d, oracle len=%d", viewType.Name(), res[0].Int(), sv.Len())
			}
			w.cov.check()
		}
	}

	w.walkArrayCursor(sv, node, viewType, depth)
}

// cursorElemCheck carries the per-element results walkArrayCursor needs after a
// single element's invariant checks: the cursor's captured Bytes(), the At(idx)
// call result, and the oracle's marshaled element bytes (when marshalable).
type cursorElemCheck struct {
	gotBytes  []byte
	at        []reflect.Value
	elemWant  []byte
	okMarshal bool
}

// checkCursorElement validates one element's cursor invariants against the oracle:
// Index() increments, Bytes() equals the marshaled element, At(idx).Raw() equals
// the same extent, and Elem() bytes agree with Bytes(). It returns the data the
// caller needs for first-element recursion. ok is false only when a cursor method
// panicked (already reported), signaling the caller to abort the walk.
func (w *walker) checkCursorElement(
	viewType reflect.Type, view reflect.Value, cptrAddr reflect.Value, elemVal reflect.Value, idx int,
) (res cursorElemCheck, ok bool) {
	// Index() increments.
	if r, _ := callMethod(cptrAddr, "Index"); len(r) == 1 {
		if int(r[0].Int()) != idx {
			w.errf("%s: cursor.Index()=%d at element %d", viewType.Name(), r[0].Int(), idx)
		}
		w.cov.check()
	}
	// Bytes() == element marshaled.
	res.elemWant, res.okMarshal = oracleFieldBytes(elemVal)
	br, bp := callMethod(cptrAddr, "Bytes")
	if bp != nil {
		w.errf("%s: cursor.Bytes() panicked on valid input", viewType.Name())
		return res, false
	}
	res.gotBytes = br[0].Bytes()
	if res.okMarshal {
		if !bytes.Equal(res.gotBytes, res.elemWant) {
			w.errf("%s: cursor.Bytes() at %d != oracle (%d vs %d)", viewType.Name(), idx, len(res.gotBytes), len(res.elemWant))
		}
		w.cov.check()
	}

	// At(i) yields the same extent as Bytes(). On valid input At() of an in-range
	// index must not panic or error, so surface those rather than swallowing them.
	ar, ap := callMethod(view, "At", reflect.ValueOf(idx))
	res.at = ar
	switch {
	case ap != nil:
		w.errf("%s: At(%d) panicked on valid input: %v", viewType.Name(), idx, ap)
	case len(ar) == 2 && !ar[1].IsNil():
		w.errf("%s: At(%d) errored on valid input: %v", viewType.Name(), idx, ar[1].Interface())
	case len(ar) == 2:
		atRaw, atRawErr, okRaw := rawOf(ar[0])
		switch {
		case !okRaw:
			w.errf("%s: At(%d).Raw() panicked on valid input", viewType.Name(), idx)
		case atRawErr != nil:
			w.errf("%s: At(%d).Raw() errored on valid input: %v", viewType.Name(), idx, atRawErr)
		case res.okMarshal && !bytes.Equal(atRaw, res.elemWant):
			w.errf("%s: At(%d).Raw() != oracle (%d vs %d)", viewType.Name(), idx, len(atRaw), len(res.elemWant))
		}
		w.cov.check()
	}

	// Elem() must agree with Bytes().
	w.checkCursorElemAgreesBytes(cptrAddr, viewType, idx, res.gotBytes)
	return res, true
}

// checkCursorElemAgreesBytes verifies the cursor's Elem() bytes equal Bytes():
// for struct-element cursors Elem() returns a Fields bundle whose View is the
// trimmed element; for other element kinds Elem() returns the plain trimmed
// element view. In both cases those bytes (View field or the view itself) must
// equal gotBytes.
func (w *walker) checkCursorElemAgreesBytes(cptrAddr reflect.Value, viewType reflect.Type, idx int, gotBytes []byte) {
	er, ep := callMethod(cptrAddr, "Elem")
	if ep != nil {
		// Elem() is a real generated cursor method; a recovered panic on valid
		// input is a failure, not something to swallow (matches cursor.Bytes()).
		w.errf("%s: cursor.Elem() panicked on valid input at %d: %v", viewType.Name(), idx, ep)
		return
	}
	if len(er) != 1 {
		return
	}
	ev := er[0]
	var elemBytes []byte
	switch {
	case ev.Kind() == reflect.Struct:
		if vf := ev.FieldByName("View"); vf.IsValid() && vf.Kind() == reflect.Slice {
			elemBytes = vf.Bytes()
		}
	case isViewType(ev.Type()):
		elemBytes = ev.Bytes()
	}
	if elemBytes != nil && !bytes.Equal(elemBytes, gotBytes) {
		w.errf("%s: Elem() bytes at %d != Bytes() (%d vs %d)", viewType.Name(), idx, len(elemBytes), len(gotBytes))
	}
	w.cov.check()
}

// walkArrayCursor drives the Scan() cursor of an array view (variable- OR
// fixed-count: the emitter generates a cursor for both) in lockstep with the
// decoded slice oracle, checking Count/Index/Next/Bytes/At/Elem/Err yield
// agreement and deep-recursing the first element. Shared by walkArray and
// walkFixedArray so the fixed-count cursor is no longer a blind spot (spec 3a).
func (w *walker) walkArrayCursor(sv reflect.Value, node []byte, viewType reflect.Type, depth int) {
	view := viewValueOf(viewType, node)

	// Scan() cursor.
	scanM := view.MethodByName("Scan")
	if !scanM.IsValid() {
		w.cov.skip("array without Scan() cursor")
		return
	}
	res, p := callMethod(view, "Scan")
	if p != nil {
		w.errf("%s: Scan() panicked on valid input", viewType.Name())
		return
	}
	cursor := res[0]
	// Cursor methods take a pointer receiver; make it addressable.
	cptr := reflect.New(cursor.Type())
	cptr.Elem().Set(cursor)
	cptrAddr := cptr.Elem().Addr()

	w.checkCursorPreamble(cptrAddr, viewType, sv.Len())

	idx := 0
	for {
		nres, np := callMethod(cptrAddr, "Next")
		if np != nil {
			w.errf("%s: cursor.Next() panicked on valid input", viewType.Name())
			return
		}
		if !nres[0].Bool() {
			break
		}
		if idx >= sv.Len() {
			w.errf("%s: cursor yielded more elements than oracle len=%d", viewType.Name(), sv.Len())
			break
		}
		elemVal := sv.Index(idx)
		res, ok := w.checkCursorElement(viewType, view, cptrAddr, elemVal, idx)
		if !ok {
			// A method panicked (already reported); abort the walk.
			return
		}
		// Deep-recurse into the FIRST element only: the element type is the same
		// for every element, so one deep walk covers its sub-invariants while the
		// per-element Bytes()/At()/Index() checks above cover yield agreement for
		// all of them. This bounds the otherwise combinatorial recursion.
		if idx == 0 {
			w.recurseCursorFirstElement(elemVal, res, depth)
		}
		idx++
	}
	if r, _ := callMethod(cptrAddr, "Err"); len(r) == 1 {
		if !r[0].IsNil() {
			w.errf("%s: cursor.Err() non-nil on valid input: %v", viewType.Name(), r[0].Interface())
		}
		w.cov.check()
	}
	if idx != sv.Len() {
		w.errf("%s: cursor yielded %d elements, oracle len=%d", viewType.Name(), idx, sv.Len())
	}
	w.cov.check()
}

// checkCursorPreamble verifies the cursor's pre-iteration invariants: Count()
// equals the oracle length and Index() is -1 before the first Next().
func (w *walker) checkCursorPreamble(cptrAddr reflect.Value, viewType reflect.Type, oracleLen int) {
	if r, _ := callMethod(cptrAddr, "Count"); len(r) == 1 {
		if int(r[0].Int()) != oracleLen {
			w.errf("%s: cursor.Count()=%d, oracle len=%d", viewType.Name(), r[0].Int(), oracleLen)
		}
		w.cov.check()
	}
	if r, _ := callMethod(cptrAddr, "Index"); len(r) == 1 {
		if r[0].Int() != -1 {
			w.errf("%s: cursor.Index() before Next()=%d, want -1", viewType.Name(), r[0].Int())
		}
		w.cov.check()
	}
}

// recurseCursorFirstElement deep-walks the first cursor element via At()'s element
// view type, using the element's oracle bytes when marshalable, or the cursor's
// captured wire extent for leaf elements (e.g. []Hash, []uint32).
func (w *walker) recurseCursorFirstElement(elemVal reflect.Value, res cursorElemCheck, depth int) {
	ar := res.at
	if len(ar) != 2 || !ar[1].IsNil() || !isViewType(ar[0].Type()) {
		return
	}
	elemViewType := ar[0].Type()
	switch {
	case res.okMarshal:
		w.walk(elemVal, res.elemWant, elemViewType, depth+1)
	case isLeafKind(elemVal.Kind(), elemVal.Type()):
		w.walk(elemVal, res.gotBytes, elemViewType, depth+1)
	default:
		w.cov.skip("array element oracle not marshalable")
	}
}

// walkFixedArray checks an XDR fixed array of non-byte elements (e.g.
// LedgerHeader.SkipList = [4]Hash). The view has Len()/At() AND a Scan() cursor
// (the emitter generates one for fixed-count arrays too), so we drive both.
func (w *walker) walkFixedArray(sv reflect.Value, node []byte, viewType reflect.Type, depth int) {
	view := viewValueOf(viewType, node)
	if lm := view.MethodByName("Len"); lm.IsValid() {
		if r := lm.Call(nil); len(r) == 1 && int(r[0].Int()) != sv.Len() {
			w.errf("%s: Len()=%d, oracle len=%d", viewType.Name(), r[0].Int(), sv.Len())
		}
		w.cov.check()
	}
	// Drive the Scan() cursor too — previously a blind spot for fixed-count
	// arrays (spec 3a). Reuses walkArray's cursor block via walkArrayCursor.
	w.walkArrayCursor(sv, node, viewType, depth)
	for i := 0; i < sv.Len(); i++ {
		elemVal := sv.Index(i)
		elemWant, okM := oracleFieldBytes(elemVal)
		ar, ap := callMethod(view, "At", reflect.ValueOf(i))
		if ap != nil {
			w.errf("%s: At(%d) panicked on valid input", viewType.Name(), i)
			return
		}
		if len(ar) != 2 || !ar[1].IsNil() || !isViewType(ar[0].Type()) {
			w.cov.skip("fixed-array element view undetermined")
			continue
		}
		atRaw, _, ok := rawOf(ar[0])
		if !ok {
			w.errf("%s: At(%d).Raw() panicked", viewType.Name(), i)
			continue
		}
		if okM && !bytes.Equal(atRaw, elemWant) {
			w.errf("%s: At(%d).Raw() != oracle (%d vs %d)", viewType.Name(), i, len(atRaw), len(elemWant))
		}
		w.cov.check()
		// Deep-recurse into the first element only (same rationale as walkArray).
		if i == 0 {
			if okM {
				w.walk(elemVal, elemWant, ar[0].Type(), depth+1)
			} else if isLeafKind(elemVal.Kind(), elemVal.Type()) {
				w.walk(elemVal, atRaw, ar[0].Type(), depth+1)
			} else {
				w.cov.skip("fixed-array element oracle not marshalable")
			}
		}
	}
}

func (w *walker) walkOptional(sv reflect.Value, node []byte, viewType reflect.Type, depth int) {
	view := viewValueOf(viewType, node)
	res, p := callMethod(view, "Unwrap")
	if p != nil {
		w.errf("%s: Unwrap() panicked on valid input", viewType.Name())
		return
	}
	if len(res) != 3 {
		w.cov.skip("optional without 3-result Unwrap()")
		return
	}
	if !res[2].IsNil() {
		w.errf("%s: Unwrap() errored on valid input: %v", viewType.Name(), res[2].Interface())
		return
	}
	present := res[1].Bool()
	if present != !sv.IsNil() {
		w.errf("%s: Unwrap() presence=%v, oracle present=%v", viewType.Name(), present, !sv.IsNil())
	}
	w.cov.check()
	if present && !sv.IsNil() {
		inner := sv.Elem()
		innerWant, okM := marshal(inner)
		if okM && isViewType(res[0].Type()) {
			w.walk(inner, innerWant, res[0].Type(), depth+1)
		} else {
			w.cov.skip("optional inner not marshalable/view")
		}
	}
}

// walkLeaf checks value agreement for comparable scalar/opaque/enum leaves.
// Raw() (already checked in walk) pins the bytes for every leaf; this adds a
// direct Value() comparison where the value is trivially comparable.
func (w *walker) walkLeaf(sv reflect.Value, node []byte, viewType reflect.Type) {
	view := viewValueOf(viewType, node)
	vm := view.MethodByName("Value")
	if !vm.IsValid() {
		w.cov.skip("leaf without Value() method")
		return
	}
	res, p := callMethod(view, "Value")
	if p != nil {
		w.errf("%s: Value() panicked on valid input", viewType.Name())
		return
	}
	if len(res) != 2 {
		w.cov.skip("leaf Value() signature unexpected")
		return
	}
	if !res[1].IsNil() {
		w.errf("%s: Value() errored on valid input: %v", viewType.Name(), res[1].Interface())
		return
	}
	got := res[0]
	w.cov.check()

	// Compare the view's decoded value to the oracle, where comparable.
	w.compareLeafValue(sv, got, viewType)
}

// compareLeafValue compares a leaf view's decoded Value() (got) against the
// oracle (sv), where the two are directly comparable. Non-comparable shapes
// (e.g. enums surfaced as typed values, non-byte composites) are skipped: their
// bytes are already pinned by the Raw() tiling check in walk().
//
// The complexity is one arm per XDR leaf kind, each a single value comparison;
// the one non-trivial arm (fixed opaque) is already a helper. Splitting the rest
// would scatter trivial comparisons without adding clarity.
//
//nolint:gocyclo // flat per-leaf-kind dispatch; each arm is one comparison.
func (w *walker) compareLeafValue(sv, got reflect.Value, viewType reflect.Type) {
	switch sv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// got.CanInt() is false when the view surfaces a non-integer kind for an
		// integer oracle (e.g. an enum/named int as a typed value); in that case
		// the bytes are already pinned by Raw() and we skip the value compare.
		if got.CanInt() && got.Int() != sv.Int() {
			w.errf("%s: Value()=%d, oracle=%d", viewType.Name(), got.Int(), sv.Int())
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if got.CanUint() && got.Uint() != sv.Uint() {
			w.errf("%s: Value()=%d, oracle=%d", viewType.Name(), got.Uint(), sv.Uint())
		}
	case reflect.Bool:
		if got.Kind() == reflect.Bool && got.Bool() != sv.Bool() {
			w.errf("%s: Value()=%v, oracle=%v", viewType.Name(), got.Bool(), sv.Bool())
		}
	case reflect.Array:
		w.compareLeafFixedOpaque(sv, got, viewType)
	case reflect.Slice:
		// Variable opaque (XDR opaque<>): oracle is []byte, Value() is []byte.
		if sv.Type().Elem().Kind() == reflect.Uint8 && got.Kind() == reflect.Slice {
			oracleBytes := make([]byte, sv.Len())
			reflect.Copy(reflect.ValueOf(oracleBytes), sv)
			if !bytes.Equal(got.Bytes(), oracleBytes) {
				w.errf("%s: var-opaque Value() != oracle (%d vs %d)", viewType.Name(), len(got.Bytes()), len(oracleBytes))
			}
		} else {
			w.cov.skip("leaf Value() not directly comparable (bytes pinned by Raw)")
		}
	case reflect.String:
		// XDR string: oracle is a Go string, Value() is a string.
		if got.Kind() == reflect.String && got.String() != sv.String() {
			w.errf("%s: string Value() != oracle", viewType.Name())
		}
	default:
		// Named ints (enums) and other shapes: Raw() already pinned the bytes.
		w.cov.skip("leaf Value() not directly comparable (bytes pinned by Raw)")
	}
}

// compareLeafFixedOpaque compares a fixed-opaque leaf (e.g. Hash [32]byte) whose
// oracle is a [N]uint8 array. Value() may return []byte or [N]byte; both are
// converted byte-exact via reflectArrayToBytes / Bytes() before comparison.
func (w *walker) compareLeafFixedOpaque(sv, got reflect.Value, viewType reflect.Type) {
	if sv.Type().Elem().Kind() != reflect.Uint8 {
		w.cov.skip("leaf Value() not directly comparable (non-byte array)")
		return
	}
	oracleBytes := reflectArrayToBytes(sv)
	var gotBytes []byte
	switch got.Kind() {
	case reflect.Slice:
		gotBytes = got.Bytes()
	case reflect.Array:
		if got.Type().Elem().Kind() == reflect.Uint8 {
			gotBytes = reflectArrayToBytes(got)
		}
	}
	if gotBytes != nil && !bytes.Equal(gotBytes, oracleBytes) {
		w.errf("%s: fixed-opaque Value() != oracle", viewType.Name())
	}
}

// reflectArrayToBytes copies a [N]uint8 reflect.Value into a fresh byte slice.
// The caller must have verified the element kind is uint8; reflect.Copy is then
// byte-exact (no numeric narrowing). The source is copied into an addressable
// array first, since reflect.Copy requires an addressable array source.
func reflectArrayToBytes(arr reflect.Value) []byte {
	tmp := reflect.New(arr.Type()).Elem()
	tmp.Set(arr)
	out := make([]byte, tmp.Len())
	reflect.Copy(reflect.ValueOf(out), tmp)
	return out
}

// isLeafKind reports whether a reflect kind is an XDR leaf (scalar, enum,
// fixed/var opaque, or string) — i.e. has a Value() and no sub-structure.
func isLeafKind(k reflect.Kind, t reflect.Type) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Bool, reflect.Float32, reflect.Float64, reflect.String:
		return true
	case reflect.Array:
		return t.Elem().Kind() == reflect.Uint8 // fixed opaque
	case reflect.Slice:
		return t.Elem().Kind() == reflect.Uint8 // var opaque
	}
	return false
}

// discAsInt64 normalizes a decoded discriminant (enum named-int, int32, or bool)
// to int64 for cross-comparison with the oracle's switch field.
func discAsInt64(v reflect.Value) int64 {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// XDR discriminants are 32-bit on the wire (enum/uint32/int32), so the
		// unsigned value is <= MaxUint32 and always fits in int64.
		//nolint:gosec // G115: discriminant is wire-bounded to 32 bits.
		return int64(v.Uint())
	case reflect.Bool:
		if v.Bool() {
			return 1
		}
		return 0
	}
	return 0
}

func methodOf(sv reflect.Value, name string) reflect.Value {
	if m := sv.MethodByName(name); m.IsValid() {
		return m
	}
	if sv.CanAddr() {
		if m := sv.Addr().MethodByName(name); m.IsValid() {
			return m
		}
	}
	p := reflect.New(sv.Type())
	p.Elem().Set(sv)
	return p.MethodByName(name)
}

// viewTypeByName indexes every typed-Value view type (typedValueViewTypes, the
// generated list) by its type name, so the harness can construct a discriminant's
// standalone leaf view (e.g. LedgerEntryTypeView) from the decoded discriminant's
// Go type name + "View". This closes the section-3b blind spot: union
// discriminants were only checked through the union's decoded accessor, never the
// standalone enum leaf view, so a bug in that leaf view's Value() went uncaught.
var viewTypeByName = func() map[string]reflect.Type {
	m := make(map[string]reflect.Type, len(typedValueViewTypes))
	for _, t := range typedValueViewTypes {
		m[t.Name()] = t
	}
	return m
}()

// enumLeafViewFor returns the standalone leaf view type for a decoded
// discriminant value (an enum named-int): for a value of Go type Foo it looks up
// FooView. ok is false for non-enum discriminants (int32/bool, which have no
// named leaf view) or an unregistered type.
func enumLeafViewFor(disc reflect.Value) (reflect.Type, bool) {
	tn := disc.Type().Name()
	if tn == "" || tn == "int32" || tn == "bool" {
		return nil, false
	}
	vt, ok := viewTypeByName[tn+"View"]
	return vt, ok
}

// --- the tests ---------------------------------------------------------------

// genValid produces a deterministic corpus of valid wire-byte values for a root,
// using randxdr. It returns the wire bytes and the decoded oracle struct value.
func genValid(t *testing.T, root conformanceRoot, n int) [][]byte {
	t.Helper()
	gen := randxdr.NewGenerator()
	var out [][]byte
	for i := 0; i < n; i++ {
		shape := root.newShape()
		gen.Next(shape, randxdrPresets)
		data := gxdr.Dump(shape)
		// Sanity: the oracle must accept its own generated bytes.
		dest := root.newDest()
		if err := dest.UnmarshalBinary(data); err != nil {
			// randxdr can occasionally produce values the strict decoder
			// rejects (e.g. non-canonical enum); skip those for the valid set.
			continue
		}
		out = append(out, data)
	}
	return out
}

func TestConformance_ValidCorpus(t *testing.T) {
	cov := newCoverage()
	w := &walker{t: t, cov: cov}

	const perRoot = 50
	for _, root := range conformanceRoots() {
		corpus := genValid(t, root, perRoot)
		if len(corpus) == 0 {
			t.Errorf("root %s: empty valid corpus", root.name)
			continue
		}
		for _, data := range corpus {
			dest := root.newDest()
			if err := dest.UnmarshalBinary(data); err != nil {
				continue
			}
			sv := reflect.ValueOf(dest).Elem()
			w.walk(sv, data, root.viewType, 0)
		}
	}
	cov.report(t)
}

// TestConformance_Mutations asserts the no-panic / error-agreement contract:
// for each valid value, apply byte-level mutations (flip, truncate, zero,
// inflate-count) and run Raw()/Fields()/Scan()/Value() through the view; the
// view must never panic, and where the struct decoder rejects the bytes the
// view path must surface an error somewhere.
func TestConformance_Mutations(t *testing.T) {
	cov := newCoverage()
	// Deterministic, fixed-seed PRNG: the mutation corpus must be reproducible
	// across runs; crypto/rand would defeat that and is not a security boundary here.
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // G404: deterministic test fuzzer, not a security context.

	mutate := func(data []byte) [][]byte {
		var muts [][]byte
		// Truncations at several lengths.
		for _, frac := range []float64{0, 0.25, 0.5, 0.75, 0.99} {
			n := int(float64(len(data)) * frac)
			cp := make([]byte, n)
			copy(cp, data)
			muts = append(muts, cp)
		}
		// Single-byte flips at random positions.
		for i := 0; i < 12 && len(data) > 0; i++ {
			cp := make([]byte, len(data))
			copy(cp, data)
			pos := rng.Intn(len(data))
			// rng.Intn(8) is in [0,8), so the bitmask is one of 1..128 — exact in a byte.
			bit := byte(rng.Intn(8)) //nolint:gosec // G115: 0..7, fits a byte by construction.
			cp[pos] ^= 1 << bit
			muts = append(muts, cp)
		}
		// Inflate the leading length/count word to a huge value (count-validation
		// / OOM-safety probe).
		if len(data) >= 4 {
			for _, big := range []uint32{0x7fffffff, 0xffffffff, 500_000_000} {
				cp := make([]byte, len(data))
				copy(cp, data)
				binary.BigEndian.PutUint32(cp[0:4], big)
				muts = append(muts, cp)
			}
		}
		return muts
	}

	for _, root := range conformanceRoots() {
		corpus := genValid(t, root, 12)
		for _, data := range corpus {
			for _, m := range mutate(data) {
				cov.hit(root.viewType.Name())
				decoderRejects := func() bool {
					dest := root.newDest()
					return dest.UnmarshalBinary(m) != nil
				}()
				viewErrored := exerciseNoPanic(t, root.viewType, m, cov)
				if decoderRejects && !viewErrored {
					// The view accepted bytes the decoder rejected. This is only
					// a violation if the bytes are genuinely malformed for the
					// view's contract. The view validates lazily, so we require
					// ValidateFull() to catch it.
					if !validateFullErrors(root.viewType, m) {
						// The ONLY legitimate decoder-rejects/view-accepts/validate-
						// passes divergence is surplus TRAILING bytes (spec 3c).
						justified, extent, rawErr := trailingBytesJustified(root.viewType, m)
						if justified {
							cov.skip("decoder-rejects-but-view-accepts (real trailing bytes)")
						} else {
							t.Errorf("%s: decoder rejected input but view accepted it WITHOUT trailing-bytes justification "+
								"(input=%d bytes, view extent=%d, rawErr=%v): lenient-acceptance divergence",
								root.viewType.Name(), len(m), extent, rawErr)
						}
					}
				}
			}
		}
	}

	// Broad no-panic sweep: collect one valid sample per reachable view type
	// from a valid walk, then run the full view surface over mutations of each.
	// This extends the never-panic guarantee from the 10 roots to the whole
	// transitive type closure (hundreds of types).
	samples := collectViewSamples(t)
	for vtype, sample := range samples {
		for _, m := range mutate(sample) {
			cov.hit(vtype.Name())
			_ = exerciseNoPanic(t, vtype, m, cov)
		}
	}

	cov.report(t)
}

// collectViewSamples runs one valid walk over every root and returns a map of
// each reached view type to a representative valid trimmed-node byte sample.
func collectViewSamples(t *testing.T) map[reflect.Type][]byte {
	t.Helper()
	// The walk's own assertions are redundant with TestConformance_ValidCorpus;
	// here we only need the per-type byte samples. We reuse t so any panic that
	// somehow slips through is still surfaced rather than swallowed.
	w := &walker{t: t, cov: newCoverage(), collect: map[reflect.Type][]byte{}}
	for _, root := range conformanceRoots() {
		for _, data := range genValid(t, root, 5) {
			dest := root.newDest()
			if dest.UnmarshalBinary(data) != nil {
				continue
			}
			w.walk(reflect.ValueOf(dest).Elem(), data, root.viewType, 0)
		}
	}
	return w.collect
}

// exerciseNoPanic runs the view extraction surface over arbitrary bytes and
// asserts no method panics. Returns whether any method surfaced an error.
func exerciseNoPanic(t *testing.T, viewType reflect.Type, data []byte, cov *coverage) (errored bool) {
	view := viewValueOf(viewType, data)
	run := func(name string) {
		res, p := callMethod(view, name)
		cov.check()
		if p != nil {
			// callMethod returns a string for "method not found"; a real panic
			// is any other recovered value. Distinguish by checking the method.
			if view.MethodByName(name).IsValid() {
				t.Errorf("%s.%s() PANICKED on mutated input: %v", viewType.Name(), name, p)
			}
			return
		}
		// Inspect trailing error results.
		for _, r := range res {
			if r.Kind() == reflect.Interface && r.Type().Implements(errorType) && !r.IsNil() {
				errored = true
			}
		}
	}
	// Raw and ValidateFull are now the package generics xdr.Raw/Validate,
	// exercised here via the test-only reflection bridges (no-panic contract).
	runBridge := func(name string, fn func(any) error) {
		cov.check()
		var p interface{}
		func() {
			defer func() { p = recover() }()
			if err := fn(view.Interface()); err != nil {
				errored = true
			}
		}()
		if p != nil {
			t.Errorf("%s.%s PANICKED on mutated input: %v", viewType.Name(), name, p)
		}
	}
	runBridge("Raw", func(v any) error { _, err := xdr.RawValue(v); return err })
	runBridge("Validate", xdr.ValidateValue)
	runBridge("Copy", func(v any) error { _, err := xdr.CopyValue(v); return err })
	run("Fields")
	run("Fields_")
	run("Value")
	run("Unwrap")
	run("Count")
	run("V")
	run("Type")

	// Scan(): drive the cursor to exhaustion; it must never panic and must not
	// OOM on inflated counts (Count is validated; we cap iterations defensively).
	if view.MethodByName("Scan").IsValid() {
		if exerciseScanNoPanic(t, view, viewType, cov) {
			errored = true
		}
	}
	return errored
}

// exerciseScanNoPanic drives a view's Scan() cursor to exhaustion under mutated
// input, asserting no method panics and that the cursor terminates (a runaway
// loop would signal a count-validation hole). It returns whether the cursor
// reported an error via Err().
func exerciseScanNoPanic(t *testing.T, view reflect.Value, viewType reflect.Type, cov *coverage) (errored bool) {
	res, p := callMethod(view, "Scan")
	cov.check()
	if p != nil {
		t.Errorf("%s.Scan() PANICKED on mutated input: %v", viewType.Name(), p)
		return false
	}
	cptr := reflect.New(res[0].Type())
	cptr.Elem().Set(res[0])
	addr := cptr.Elem().Addr()
	const maxIters = 1 << 20
	iters := 0
	for iters < maxIters {
		nres, np := callMethod(addr, "Next")
		if np != nil {
			t.Errorf("%s cursor.Next() PANICKED on mutated input: %v", viewType.Name(), np)
			break
		}
		if !nres[0].Bool() {
			break
		}
		// Touch Elem()/Bytes() (must not panic).
		if _, bp := callMethod(addr, "Bytes"); bp != nil {
			t.Errorf("%s cursor.Bytes() PANICKED: %v", viewType.Name(), bp)
			break
		}
		if _, ep := callMethod(addr, "Elem"); ep != nil {
			t.Errorf("%s cursor.Elem() PANICKED: %v", viewType.Name(), ep)
			break
		}
		iters++
	}
	if iters >= maxIters {
		t.Errorf("%s cursor did not terminate (count validation hole?)", viewType.Name())
	}
	if er, _ := callMethod(addr, "Err"); len(er) == 1 && er[0].Kind() == reflect.Interface && !er[0].IsNil() {
		return true
	}
	return false
}

// trailingBytesJustified reports whether a view's acceptance of decoder-rejected
// bytes is explained by surplus TRAILING bytes: the view trims to the node extent
// (xdr.RawValue), so its accepted extent must be STRICTLY shorter than the input.
// If the view accepted the FULL input (extent == len(data), or Raw failed), the
// acceptance is a genuine lenient divergence, not trailing bytes. This is the
// section-3c decision, factored out so it is directly unit-testable (see
// TestConformance_TrailingBytesJustification).
func trailingBytesJustified(viewType reflect.Type, data []byte) (justified bool, extent int, rawErr error) {
	accepted, err, ok := rawOf(viewValueOf(viewType, data))
	if !ok || err != nil {
		return false, 0, err
	}
	return len(accepted) < len(data), len(accepted), nil
}

func validateFullErrors(viewType reflect.Type, data []byte) (errored bool) {
	defer func() {
		if recover() != nil {
			errored = false
		}
	}()
	// xdr.Validate via the test-only reflection bridge (Validate is a package
	// generic, not a per-type method).
	return xdr.ValidateValue(viewValueOf(viewType, data).Interface()) != nil
}

var errorType = reflect.TypeOf((*error)(nil)).Elem()

// TestConformance_TrailingBytesJustification proves the section-3c decision is
// non-vacuous: it can both PASS (real trailing bytes → justified, the branch
// skips) and FAIL (full-extent acceptance → NOT justified, the branch t.Errorf's).
// Uint32View has a fixed 4-byte extent, so 8 bytes is real trailing bytes
// (extent 4 < 8) while 4 bytes is full acceptance (extent 4 == 4).
func TestConformance_TrailingBytesJustification(t *testing.T) {
	vt := reflect.TypeOf(xdr.Uint32View(nil))

	// Trailing bytes: extent (4) strictly shorter than input (8) -> justified,
	// the harness would SKIP (no failure).
	if just, extent, err := trailingBytesJustified(vt, []byte{0, 0, 0, 1, 9, 9, 9, 9}); !just || extent != 4 || err != nil {
		t.Errorf("trailing-bytes case: justified=%v extent=%d err=%v, want justified=true extent=4", just, extent, err)
	}

	// Full acceptance: extent (4) == input (4) -> NOT justified, the harness
	// would t.Errorf. This is the path that makes section-3c bite a real lenient
	// view that accepts a decoder-rejected full-length input.
	if just, extent, _ := trailingBytesJustified(vt, []byte{0, 0, 0, 1}); just || extent != 4 {
		t.Errorf("full-acceptance case: justified=%v extent=%d, want justified=false extent=4", just, extent)
	}

	// Raw failure (too short to size) -> NOT justified (cannot claim trailing).
	if just, _, _ := trailingBytesJustified(vt, []byte{0, 0}); just {
		t.Errorf("short-input case: justified=true, want false")
	}
}

// TestConformance_CountValidation asserts the count bound directly: a tiny
// buffer declaring an enormous element count is rejected by the checked-count
// helper at cursor construction (or yields zero elements), never OOM'd or spun.
// It targets every array view type reachable from the roots.
// probeCountValidation drives one view type against a 12-byte buffer whose
// leading count word claims 500M elements. For array views, the section-6 bound
// (count > (len-4)/minElemW) must reject: the standalone Count() must error, and
// the Scan() cursor must either start in error or yield nothing — never advance
// 500M times. isArray reports whether the type was an array view; safe reports
// whether it withstood the inflated count without a validation hole.
func probeCountValidation(t *testing.T, viewType reflect.Type) (isArray, safe bool) {
	huge := []byte{
		0x1d, 0xcd, 0x65, 0x00, // 500,000,000
		0, 0, 0, 0,
		0, 0, 0, 0,
	}
	view := viewValueOf(viewType, huge)
	if !view.MethodByName("Scan").IsValid() {
		return false, false
	}
	// Standalone Count() accessor (var-count arrays only): it must reject the
	// inflated 500M wire count, not return it. This is the section-6 hole the
	// cursor's arrayViewCountChecked already closed; Count() now uses the same
	// checked helper. A two-return Count() exists only on var-count arrays.
	if cm := view.MethodByName("Count"); cm.IsValid() && cm.Type().NumOut() == 2 {
		cres, cp := callMethod(view, "Count")
		if cp != nil {
			t.Errorf("%s.Count() panicked on inflated count", viewType.Name())
			return true, false
		}
		if len(cres) == 2 && cres[1].IsNil() {
			t.Errorf("%s: Count()=%d accepted on 12-byte buffer claiming 500M elements "+
				"(standalone Count count-validation hole)", viewType.Name(), cres[0].Int())
			return true, false
		}
	}
	res, p := callMethod(view, "Scan")
	if p != nil {
		t.Errorf("%s.Scan() panicked on inflated count", viewType.Name())
		return true, false
	}
	cptr := reflect.New(res[0].Type())
	cptr.Elem().Set(res[0])
	addr := cptr.Elem().Addr()
	// Either the cursor is already in error (count rejected at construction) or
	// Next() yields nothing — it must never advance 500M times.
	if er, _ := callMethod(addr, "Err"); len(er) == 1 && !er[0].IsNil() {
		return true, true
	}
	nres, np := callMethod(addr, "Next")
	if np != nil {
		t.Errorf("%s cursor.Next() panicked on inflated count", viewType.Name())
		return true, false
	}
	if len(nres) == 1 && nres[0].Bool() {
		t.Errorf("%s: cursor advanced on 12-byte buffer claiming 500M elements (count-validation hole)", viewType.Name())
		return true, false
	}
	return true, true
}

func TestConformance_CountValidation(t *testing.T) {
	// Discover every reachable array view type via a valid walk, then probe each.
	samples := collectViewSamples(t)
	arrays, safe := 0, 0
	for vtype := range samples {
		if isArr, ok := probeCountValidation(t, vtype); isArr {
			arrays++
			if ok {
				safe++
			}
		}
	}

	t.Logf("count-validation: %d/%d array cursor types rejected/zero-yielded an inflated 500M count without OOM", safe, arrays)
	if arrays == 0 {
		t.Fatal("count-validation probed no array types")
	}
}

// TestConformance_DiscriminantLeafViews gates the generated typedValueViewTypes
// registry (xdr_views_registry_generated_test.go, emitted by xdrgen) against the
// live schema: it walks every root, collects every union discriminant whose
// decoded type is a named enum, and asserts each resolves to a registered
// standalone leaf view. The registry regenerates with the views (via `make`), so
// this is a cheap backstop: if a new union enters the schema with an
// unregistered discriminant enum AND the registry was not regenerated, this fails
// loudly — so the discriminant-enum leaf-view coverage can never silently drop a
// reachable type.
// collectDiscriminantEnums recursively walks an oracle value, recording every
// union discriminant field that is a named enum type into discEnums (keyed by
// type name). Byte arrays/slices are skipped; recursion is depth-bounded.
func collectDiscriminantEnums(sv reflect.Value, depth int, discEnums map[string]reflect.Type) {
	if depth > 40 || !sv.IsValid() {
		return
	}
	switch sv.Kind() {
	case reflect.Ptr:
		if !sv.IsNil() {
			collectDiscriminantEnums(sv.Elem(), depth+1, discEnums)
		}
	case reflect.Struct:
		if m := methodOf(sv, "SwitchFieldName"); m.IsValid() {
			sn := m.Call(nil)[0].String()
			if df := sv.FieldByName(sn); df.IsValid() {
				if _, ok := enumDiscType(df); ok {
					discEnums[df.Type().Name()] = df.Type()
				}
			}
		}
		for i := 0; i < sv.NumField(); i++ {
			if sv.Type().Field(i).PkgPath == "" {
				collectDiscriminantEnums(sv.Field(i), depth+1, discEnums)
			}
		}
	case reflect.Slice, reflect.Array:
		if sv.Type().Elem().Kind() != reflect.Uint8 {
			for i := 0; i < sv.Len(); i++ {
				collectDiscriminantEnums(sv.Index(i), depth+1, discEnums)
			}
		}
	}
}

func TestConformance_DiscriminantLeafViews(t *testing.T) {
	discEnums := map[string]reflect.Type{}
	for _, root := range conformanceRoots() {
		for _, data := range genValid(t, root, 10) {
			dest := root.newDest()
			if dest.UnmarshalBinary(data) != nil {
				continue
			}
			collectDiscriminantEnums(reflect.ValueOf(dest).Elem(), 0, discEnums)
		}
	}

	if len(discEnums) == 0 {
		t.Fatal("no union discriminant enums discovered; the gate would be vacuous")
	}
	missing := 0
	for name, typ := range discEnums {
		if _, ok := viewTypeByName[name+"View"]; !ok {
			missing++
			t.Errorf("discriminant enum %s has no entry in typedValueViewTypes; "+
				"regenerate via xdrgen (make) to refresh xdr_views_registry_generated_test.go", name)
		}
		// enumLeafViewFor must resolve a value of this type.
		if _, ok := enumLeafViewFor(reflect.New(typ).Elem()); !ok {
			t.Errorf("enumLeafViewFor failed to resolve discriminant enum %s", name)
		}
	}
	t.Logf("discriminant-leaf gate: %d discriminant enum types, all registered (%d missing)", len(discEnums), missing)
}

// enumDiscType reports whether a decoded union discriminant field is a named enum
// (not a plain int32/bool), returning its type. Enum discriminants in the SDK are
// distinct int32-kinded named types.
func enumDiscType(df reflect.Value) (reflect.Type, bool) {
	if df.Kind() != reflect.Int32 {
		return nil, false
	}
	tn := df.Type().Name()
	if tn == "" || tn == "int32" {
		return nil, false
	}
	return df.Type(), true
}
