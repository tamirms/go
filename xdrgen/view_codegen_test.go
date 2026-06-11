package main

import (
	"reflect"
	"strings"
	"testing"
)

// Codegen edge-case guards. No current IR triggers these, so they are covered
// with synthesized definitions.

// A named case on a NON-ref (bool/int/uint) discriminant must emit the numeric
// literal (int32(<value>)), not int32(GoTypeName(name)) which would be an
// undefined identifier and fail go build.
func TestCaseValueExpr_NonRefNamedCase(t *testing.T) {
	g := testGenerator(nil)

	boolUnion := &UnionDef{
		Name:         "BoolU",
		Discriminant: StructField{Name: "present", Type: TypeRef{Kind: TRBool}},
	}
	// A named bool case ("TRUE", value 1) and an unnamed one (value 0).
	armTrue := &UnionArm{Cases: []UnionCase{{Value: 1, Name: "TRUE"}}}
	got, err := g.caseValueExpr(boolUnion, 0, armTrue)
	if err != nil {
		t.Fatalf("caseValueExpr(bool named): %v", err)
	}
	if got != "int32(1)" {
		t.Errorf("bool named case = %q, want %q (must be the numeric literal, not an identifier)", got, "int32(1)")
	}

	intUnion := &UnionDef{
		Name:         "IntU",
		Discriminant: StructField{Name: "v", Type: TypeRef{Kind: TRUnsignedInt}},
	}
	armNamed := &UnionArm{Cases: []UnionCase{{Value: 7, Name: "SEVEN"}}}
	got, err = g.caseValueExpr(intUnion, 0, armNamed)
	if err != nil {
		t.Fatalf("caseValueExpr(int named): %v", err)
	}
	if got != "int32(7)" {
		t.Errorf("int named case = %q, want %q", got, "int32(7)")
	}
}

// End-to-end: a bool-discriminated union with named TRUE/FALSE cases must
// generate output whose case labels are numeric literals (compilable), never
// the undefined identifier form (int32(True)/int32(False)).
func TestGenerateViews_NamedBoolCasesCompile(t *testing.T) {
	ir := &IR{Definitions: []DefWrap{
		{Kind: DKUnion, Union: &UnionDef{
			Name:         "Flagged",
			Discriminant: StructField{Name: "present", Type: TypeRef{Kind: TRBool}},
			Arms: []UnionArm{
				{Cases: []UnionCase{{Value: 1, Name: "TRUE"}}, Name: "value", Type: &TypeRef{Kind: TRInt}},
				{Cases: []UnionCase{{Value: 0, Name: "FALSE"}}, Name: ""},
			},
		}, FixedSize: nil},
	}}
	gen, err := NewGenerator(ir)
	if err != nil {
		t.Fatalf("NewGenerator: %v", err)
	}
	out, err := gen.GenerateViews()
	if err != nil {
		t.Fatalf("GenerateViews: %v", err)
	}
	src := string(out)
	if !strings.Contains(src, "case int32(1):") {
		t.Errorf("expected numeric case label int32(1) for named bool case TRUE")
	}
	// The undefined-identifier form must NOT appear.
	for _, bad := range []string{"int32(True)", "int32(False)", "int32(TRUE)", "int32(FALSE)"} {
		if strings.Contains(src, bad) {
			t.Errorf("generated output contains undefined identifier case label %q", bad)
		}
	}
}

// assertPlanViewsRejectsInlineElement builds a single-struct IR whose only field
// has the given type and asserts PlanViews rejects it with a "concrete inline
// type" error. Used by the inline-element guards below.
func assertPlanViewsRejectsInlineElement(t *testing.T, fieldType TypeRef) {
	t.Helper()
	ir := &IR{Definitions: []DefWrap{
		{Kind: DKStruct, Struct: &StructDef{
			Name:   "S",
			Fields: []StructField{{Name: "x", Type: fieldType}},
		}},
	}}
	gen, err := NewGenerator(ir)
	if err != nil {
		t.Fatalf("NewGenerator: %v", err)
	}
	if _, err := gen.PlanViews(); err == nil {
		t.Fatal("PlanViews accepted a nested inline element; want a loud codegen error")
	} else if !strings.Contains(err.Error(), "concrete inline type") {
		t.Errorf("unexpected error %v; want one about a concrete inline type", err)
	}
}

// An anonymous-inline array element that itself needs a concrete view type
// (here a nested var array) must be rejected loudly at PlanViews, not silently
// dropped or emitted as non-compiling code. A named/typedef ref element is fine.
func TestPlanViews_RejectsInlineArrayElement(t *testing.T) {
	// struct S { int x<><>; } — a var array whose element is itself a var array.
	assertPlanViewsRejectsInlineElement(t, TypeRef{Kind: TRVarArray, Element: &TypeRef{
		Kind: TRVarArray, Element: &TypeRef{Kind: TRInt},
	}})
}

// An anonymous-inline optional element that needs a concrete type (a nested
// var array under an optional) is likewise rejected.
func TestPlanViews_RejectsInlineOptionalElement(t *testing.T) {
	// struct S { (int<>)* x; } — an optional whose element is an inline var array.
	assertPlanViewsRejectsInlineElement(t, TypeRef{Kind: TROptional, Element: &TypeRef{
		Kind: TRVarArray, Element: &TypeRef{Kind: TRInt},
	}})
}

// Regression guard: a typedef-ref element (e.g. Hash txHashes<>, SCMap*) is
// NOT inline and must continue to be accepted — it resolves to an emitted alias.
func TestPlanViews_AcceptsTypedefRefElements(t *testing.T) {
	ir := &IR{Definitions: []DefWrap{
		{Kind: DKTypedef, Typedef: &TypedefDef{
			Name: "Hash", Type: TypeRef{Kind: TROpaqueFixed, Size: ptrU64(32)},
		}},
		{Kind: DKTypedef, Typedef: &TypedefDef{
			Name: "IntList", Type: TypeRef{Kind: TRVarArray, Element: &TypeRef{Kind: TRInt}},
		}},
		{Kind: DKStruct, Struct: &StructDef{
			Name: "S",
			Fields: []StructField{
				{Name: "hashes", Type: TypeRef{Kind: TRVarArray, Element: &TypeRef{Kind: TRRef, Name: "Hash"}}},
				{Name: "list", Type: TypeRef{Kind: TROptional, Element: &TypeRef{Kind: TRRef, Name: "IntList"}}},
			},
		}},
	}}
	gen, err := NewGenerator(ir)
	if err != nil {
		t.Fatalf("NewGenerator: %v", err)
	}
	if _, err := gen.PlanViews(); err != nil {
		t.Fatalf("PlanViews rejected typedef-ref array/optional elements: %v", err)
	}
}

// A fixed-array size whose elemSize*count overflows uint32 must error in the
// resolver, not silently wrap.
func TestBuildViewType_FixedArrayOverflow(t *testing.T) {
	r := testGenerator(nil).TypeResolver
	// element opaque[65536] (padded 65536 bytes) * count 65536 = 2^32, overflows.
	_, err := r.BuildViewType(&TypeRef{
		Kind:    TRArray,
		Count:   ptrU64(65536),
		Element: &TypeRef{Kind: TROpaqueFixed, Size: ptrU64(65536)},
	})
	if err == nil {
		t.Fatal("BuildViewType accepted an overflowing fixed-array size; want an error")
	}
	if !strings.Contains(err.Error(), "overflow") {
		t.Errorf("unexpected error %v; want an overflow error", err)
	}

	// A just-fitting size (2^32 - padded element) must still resolve.
	if _, err := r.BuildViewType(&TypeRef{
		Kind:    TRArray,
		Count:   ptrU64(1),
		Element: &TypeRef{Kind: TROpaqueFixed, Size: ptrU64(4)},
	}); err != nil {
		t.Errorf("BuildViewType rejected a valid small fixed array: %v", err)
	}
}

// collectTypedValueViewTypes must capture every view whose Value() returns a
// single Go identifier (named type or builtin scalar) and must EXCLUDE composite
// returns ([]byte, [N]byte) — exactly the set the conformance harness needs. It
// must also dedup and sort by (lowercase, original) so the order is deterministic
// and locale-independent.
func TestCollectTypedValueViewTypes(t *testing.T) {
	src := []byte(strings.Join([]string{
		// Included: named enum, builtin scalar, named fixed-opaque typedef, and a
		// struct field accessor that happens to be named Value() returning a view.
		"func (v AssetTypeView) Value() (AssetType, error) {",
		"func (v BoolView) Value() (bool, error) {",
		"func (v HashView) Value() (Hash, error) {",
		"func (v ScpBallotView) Value() (ValueView, error) {",
		// Duplicate of an already-seen accessor must collapse.
		"func (v BoolView) Value() (bool, error) {",
		// Excluded: composite returns (anonymous/variable opaque).
		"func (v Curve25519PublicKeyOpaqueView) Value() ([32]byte, error) {",
		"func (v VarOpaqueView) Value() ([]byte, error) {",
		// Excluded: not a Value() accessor.
		"func (v FooView) Counter() (Uint32View, error) {",
		// Excluded: MustValue has no error return.
		"func (v BoolView) MustValue() bool { return must(v.Value()) }",
	}, "\n"))

	got := collectTypedValueViewTypes(src)
	want := []string{"AssetTypeView", "BoolView", "HashView", "ScpBallotView"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("collectTypedValueViewTypes = %v, want %v", got, want)
	}
}

// The (lowercase, original) sort key must put case-differing prefixes in
// locale-aware order (e.g. ScpBallotView before ScSpecTypeView, because "scp" <
// "scs"), which a plain byte sort would not, so the generated registry matches
// the previously locale-sorted list.
func TestCollectTypedValueViewTypes_SortOrder(t *testing.T) {
	src := []byte(strings.Join([]string{
		"func (v ScValTypeView) Value() (ScValType, error) {",
		"func (v ScpBallotView) Value() (ValueView, error) {",
		"func (v ScSpecTypeView) Value() (ScSpecType, error) {",
		"func (v ScMetaKindView) Value() (ScMetaKind, error) {",
	}, "\n"))

	got := collectTypedValueViewTypes(src)
	want := []string{"ScMetaKindView", "ScpBallotView", "ScSpecTypeView", "ScValTypeView"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sort order = %v, want %v (locale-aware: scm < scp < scs < scv)", got, want)
	}
}

// GenerateViewsRegistry must emit a compilable package xdr_test file with the
// Code-generated header (so linters auto-skip it) and a typedValueViewTypes slice
// listing each collected view type.
func TestGenerateViewsRegistry(t *testing.T) {
	src := []byte(strings.Join([]string{
		"func (v AssetTypeView) Value() (AssetType, error) {",
		"func (v BoolView) Value() (bool, error) {",
	}, "\n"))

	out, err := GenerateViewsRegistry(src)
	if err != nil {
		t.Fatalf("GenerateViewsRegistry: %v", err)
	}
	got := string(out)
	for _, want := range []string{
		"// Code generated by xdrgen. DO NOT EDIT.",
		"package xdr_test",
		`"github.com/stellar/go-stellar-sdk/xdr"`,
		"var typedValueViewTypes = []reflect.Type{",
		"reflect.TypeOf(xdr.AssetTypeView(nil)),",
		"reflect.TypeOf(xdr.BoolView(nil)),",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("generated registry missing %q\n--- got ---\n%s", want, got)
		}
	}

	// Empty input is a loud error, not a silently empty registry.
	if _, err := GenerateViewsRegistry([]byte("func (v FooView) NotValue() (Bar, error) {")); err == nil {
		t.Error("GenerateViewsRegistry accepted source with no typed Value() accessors; want an error")
	}
}
