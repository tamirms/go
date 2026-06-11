package main

import "fmt"

// ViewPlan holds the resolved type information for all view definitions,
// in IR order. It is computed from the IR without any code emission,
// making it testable independently of the emitters.
type ViewPlan struct {
	Entries []ViewPlanEntry
}

// ViewPlanEntry is implemented by each plan type.
type ViewPlanEntry interface {
	planEntry()
}

type StructViewPlan struct {
	ViewTypeName  string
	FixedWireSize *uint32
	Fields        []FieldPlan
	// XDRName is the original XDR type name, used to derive the locate helper
	// name and the Fields bundle type name.
	XDRName string
}

func (*StructViewPlan) planEntry() {}

type FieldPlan struct {
	FieldName   string
	ViewType    *ViewType
	IsVoidCase0 bool
}

// DiscKind classifies a union discriminant for decoded-discriminant emission.
// The discriminant accessor returns the decoded value, not
// a leaf view: enum discriminants return the enum Go type with known-value
// validation, int discriminants return int32 with no validation (so default
// arms stay reachable), bool discriminants return bool.
type DiscKind string

const (
	DiscEnum DiscKind = "enum"
	DiscInt  DiscKind = "int"
	DiscBool DiscKind = "bool"
)

type UnionViewPlan struct {
	ViewTypeName  string
	FixedWireSize *uint32
	DiscName      string
	DiscViewType  *ViewType
	Arms          []UnionArmPlan
	// Decoded-discriminant fields.
	DiscKind      DiscKind // enum/int/bool
	DiscGoType    string   // decoded Go type: enum name, "int32", or "bool"
	DiscCaseNames []string // enum case constant names (DiscEnum only), for known-value validation
}

func (*UnionViewPlan) planEntry() {}

type UnionArmPlan struct {
	ArmName   string
	ViewType  *ViewType
	CaseExprs []string
}

type EnumViewPlan struct {
	ViewTypeName string
	EnumName     string
	CaseNames    []string
}

func (*EnumViewPlan) planEntry() {}

type TypedefViewPlan struct {
	AliasName string
	ViewType  *ViewType
}

func (*TypedefViewPlan) planEntry() {}

type InlineTypePlan struct {
	Name     string
	ViewType *ViewType
	// Array-only fields, populated for VKArray view types (zero otherwise).
	// They drive Scan() cursor emission.
	ElemMinWidth         uint32 // clamped (floor 1) minimum element wire size
	ElemMinWidthComputed uint32 // unclamped minimum, for the build-time zero assert
	// ElemFieldsType is the element's Fields bundle type name when the element
	// is a named struct (Elem() returns the located bundle); empty otherwise
	// (Elem() returns the plain trimmed element view).
	ElemFieldsType string
	// ElemLocateFn is the element's locate-helper function name, carried directly
	// (set alongside ElemFieldsType) so the cursor emitter need not string-strip
	// "Fields" and re-prepend "locate". Empty when ElemFieldsType is empty.
	ElemLocateFn string
	// OpaqueValueGoType is the Go type a fixed-opaque view's Value() returns,
	// matching what the struct decoder produces: the named
	// schema type for a typedef opaque (e.g. "Hash"), or "[N]byte" for an
	// anonymous inline opaque. Empty for non-fixed-opaque view types.
	OpaqueValueGoType string
}

func (*InlineTypePlan) planEntry() {}

// PlanViews computes the ViewPlan for the entire IR.
func (g *Generator) PlanViews() (*ViewPlan, error) {
	plan := &ViewPlan{}

	for _, def := range g.allDefs {
		switch def.Kind {
		case DKStruct:
			if err := g.planStruct(plan, def.Struct); err != nil {
				return nil, err
			}
		case DKUnion:
			if err := g.planUnion(plan, def.Union); err != nil {
				return nil, err
			}
		case DKEnum:
			g.planEnum(plan, def.Enum)
		case DKTypedef:
			if err := g.planTypedef(plan, def.Typedef); err != nil {
				return nil, err
			}
		case DKConst:
		}
	}
	return plan, nil
}

func inlineTypeName(containerName, fieldName string, vt *ViewType) string {
	suffix := "View"
	if vt.Kind == VKOpaque {
		suffix = "OpaqueView"
	}
	if vt.Kind == VKOptional {
		suffix = "OptView"
	}
	return GoTypeName(containerName) + GoTypeName(fieldName) + suffix
}

func nameInlineType(containerName, fieldName string, vt *ViewType) (*ViewType, *InlineTypePlan) {
	if !vt.NeedsConcreteType() {
		return vt, nil
	}
	name := inlineTypeName(containerName, fieldName, vt)
	result := *vt
	result.GoType = name
	ip := &InlineTypePlan{Name: name, ViewType: vt}
	// Anonymous inline fixed opaque: the struct decoder produces a [N]byte
	// array, so Value() returns [N]byte.
	if vt.Kind == VKOpaque && vt.Opaque.RawSize > 0 {
		ip.OpaqueValueGoType = fmt.Sprintf("[%d]byte", vt.Opaque.RawSize)
	}
	return &result, ip
}

// elementNeedsInlineType reports whether an array/optional ELEMENT requires a
// dedicated inline view type that the planner does not currently emit, which
// would leave the element's GoType as a stand-in with no view methods (or a
// silently-dropped bound). The planner only names the TOP-LEVEL inline type of a
// field/arm (nameInlineType), never an element nested inside it.
//
// The element is fine when it came from a named/typedef ref (elementInline ==
// false): such a ref always resolves to an emitted view type (e.g. Hash ->
// HashView, SCMap -> SCMapView). It is unhandled only when it is anonymous-inline
// AND structurally needs a concrete type (a nested anonymous array/optional/fixed
// or bounded opaque element), which the planner never emits as an element.
func elementNeedsInlineType(elem *ViewType, elementInline bool) bool {
	return elementInline && elem.NeedsConcreteType()
}

// fillInlineArrayPlan populates the array-specific fields of an InlineTypePlan
// (no-op for non-array view types). It computes the element's minimum wire size
// for count validation and, when the element is a named struct, the element's
// Fields bundle type name so the cursor's Elem() can yield the located form.
//
// It returns an error when an array OR optional element itself needs a concrete
// inline type the planner does not emit (elementNeedsInlineType): emitting would
// reference an undefined/method-less type or drop a bound. Until inline-element
// naming is implemented, fail loudly at codegen — mirroring the default-arm
// rejection in planUnion — rather than at go build / silently. No current IR
// triggers this.
func (g *Generator) fillInlineArrayPlan(ip *InlineTypePlan) error {
	if ip == nil {
		return nil
	}
	switch ip.ViewType.Kind {
	case VKArray:
		elem := ip.ViewType.Array.Element
		if elementNeedsInlineType(elem, ip.ViewType.Array.ElementInline) {
			return fmt.Errorf("array view %s: element type needs a concrete inline type "+
				"(nested inline array/optional/opaque element); inline-element naming is not yet implemented", ip.Name)
		}
		ip.ElemMinWidth, ip.ElemMinWidthComputed = g.minElemWidth(elem)
		if fieldsType, locateFn, ok := g.structElemFieldsType(elem); ok {
			ip.ElemFieldsType = fieldsType
			ip.ElemLocateFn = locateFn
		}
	case VKOptional:
		opt := ip.ViewType.Optional
		if elementNeedsInlineType(opt.Element, opt.ElementInline) {
			return fmt.Errorf("optional view %s: element type needs a concrete inline type "+
				"(nested inline array/optional/opaque element); inline-element naming is not yet implemented", ip.Name)
		}
	}
	return nil
}

// structElemFieldsType reports whether a view type is a named struct and, if so,
// returns its Fields bundle type name and its locate-helper function name (the
// same names emitStructFields emits: GoTypeName+"Fields" and "locate"+GoTypeName).
func (g *Generator) structElemFieldsType(vt *ViewType) (fieldsType, locateFn string, ok bool) {
	if vt == nil || vt.Kind != VKNamed || vt.Named == nil {
		return "", "", false
	}
	def, dok := g.TypeResolver[vt.Named.XDRName]
	if !dok || def.Kind != DKStruct {
		return "", "", false
	}
	name := GoTypeName(vt.Named.XDRName)
	return name + "Fields", "locate" + name, true
}

func (g *Generator) planStruct(plan *ViewPlan, s *StructDef) error {
	sp := StructViewPlan{
		ViewTypeName:  GoTypeName(s.Name) + "View",
		FixedWireSize: g.TypeResolver[s.Name].FixedSize,
		XDRName:       s.Name,
	}

	for _, f := range s.Fields {
		vt, err := g.ResolveViewType(&f.Type)
		if err != nil {
			return fmt.Errorf("struct %s field %s: %w", s.Name, f.Name, err)
		}
		isInline := f.Type.Kind != TRRef
		if isInline {
			var inlinePlan *InlineTypePlan
			vt, inlinePlan = nameInlineType(s.Name, f.Name, vt)
			if inlinePlan != nil {
				if err := g.fillInlineArrayPlan(inlinePlan); err != nil {
					return fmt.Errorf("struct %s field %s: %w", s.Name, f.Name, err)
				}
				plan.Entries = append(plan.Entries, inlinePlan)
			}
		}
		sp.Fields = append(sp.Fields, FieldPlan{
			FieldName:   GoTypeName(f.Name),
			ViewType:    vt,
			IsVoidCase0: g.isVoidCase0Union(vt),
		})
	}

	plan.Entries = append(plan.Entries, &sp)
	return nil
}

func (g *Generator) planUnion(plan *ViewPlan, u *UnionDef) error {
	for _, arm := range u.Arms {
		if len(arm.Cases) == 0 {
			return fmt.Errorf("union %s: XDR default arms not yet supported", u.Name)
		}
	}

	xdrName := GoTypeName(u.Name)

	discVT, err := g.ResolveViewType(&u.Discriminant.Type)
	if err != nil {
		return fmt.Errorf("union %s discriminant: %w", u.Name, err)
	}

	up := UnionViewPlan{
		ViewTypeName:  xdrName + "View",
		FixedWireSize: g.TypeResolver[u.Name].FixedSize,
		DiscName:      GoTypeName(u.Discriminant.Name),
		DiscViewType:  discVT,
	}

	if err := g.fillDiscriminant(&up, u); err != nil {
		return err
	}

	for i, arm := range u.Arms {
		var caseExprs []string
		for j := range arm.Cases {
			expr, ceErr := g.caseValueExpr(u, j, &arm)
			if ceErr != nil {
				return ceErr
			}
			caseExprs = append(caseExprs, expr)
		}

		armName := GoTypeName(arm.Name)
		if armName == "" {
			armName = fmt.Sprintf("Arm%d", i)
		}

		var vt *ViewType
		if arm.Type != nil {
			vt, err = g.ResolveViewType(arm.Type)
			if err != nil {
				return fmt.Errorf("union %s arm %s: %w", u.Name, armName, err)
			}
			if arm.Type.Kind != TRRef {
				var inlinePlan *InlineTypePlan
				vt, inlinePlan = nameInlineType(xdrName, armName, vt)
				if inlinePlan != nil {
					if err := g.fillInlineArrayPlan(inlinePlan); err != nil {
						return fmt.Errorf("union %s arm %s: %w", u.Name, armName, err)
					}
					plan.Entries = append(plan.Entries, inlinePlan)
				}
			}
		}

		up.Arms = append(up.Arms, UnionArmPlan{
			ArmName:   armName,
			ViewType:  vt,
			CaseExprs: caseExprs,
		})
	}

	plan.Entries = append(plan.Entries, &up)
	return nil
}

// fillDiscriminant classifies the union's discriminant and records the decoded
// Go type (and, for enums, the valid case names) so the emitter can generate a
// decoded-discriminant accessor.
func (g *Generator) fillDiscriminant(up *UnionViewPlan, u *UnionDef) error {
	resolved, err := g.resolveTypeRef(&u.Discriminant.Type)
	if err != nil {
		return fmt.Errorf("union %s discriminant: %w", u.Name, err)
	}
	switch resolved.Kind {
	case TRBool:
		up.DiscKind = DiscBool
		up.DiscGoType = "bool"
	case TRInt, TRUnsignedInt:
		// Int-discriminated: decode to int32, NO known-value validation so
		// default arms stay reachable for forward compatibility.
		up.DiscKind = DiscInt
		up.DiscGoType = "int32"
	case TRRef:
		def, ok := g.TypeResolver[resolved.Name]
		if !ok {
			return fmt.Errorf("union %s discriminant: unknown type %q", u.Name, resolved.Name)
		}
		if def.Kind != DKEnum {
			return fmt.Errorf("union %s discriminant: unsupported ref kind %q (%s)", u.Name, def.Kind, resolved.Name)
		}
		up.DiscKind = DiscEnum
		enumName := GoTypeName(resolved.Name)
		up.DiscGoType = enumName
		for _, m := range def.Enum.Members {
			up.DiscCaseNames = append(up.DiscCaseNames, enumName+GoTypeName(m.Name))
		}
	default:
		return fmt.Errorf("union %s discriminant: unsupported kind %q", u.Name, resolved.Kind)
	}
	return nil
}

func (g *Generator) planEnum(plan *ViewPlan, e *EnumDef) {
	enumName := GoTypeName(e.Name)
	caseNames := make([]string, len(e.Members))
	for i, m := range e.Members {
		caseNames[i] = enumName + GoTypeName(m.Name)
	}
	plan.Entries = append(plan.Entries, &EnumViewPlan{
		ViewTypeName: enumName + "View",
		EnumName:     enumName,
		CaseNames:    caseNames,
	})
}

func (g *Generator) planTypedef(plan *ViewPlan, td *TypedefDef) error {
	vt, err := g.ResolveViewType(&td.Type)
	if err != nil {
		return fmt.Errorf("typedef %s: %w", td.Name, err)
	}
	aliasName := GoTypeName(td.Name) + "View"

	if vt.NeedsConcreteType() && aliasName != vt.GoType {
		ip := &InlineTypePlan{
			Name:     aliasName,
			ViewType: vt,
		}
		if err := g.fillInlineArrayPlan(ip); err != nil {
			return fmt.Errorf("typedef %s: %w", td.Name, err)
		}
		// A typedef fixed opaque (e.g. Hash = opaque[32]) decodes to the named
		// schema type the struct decoder produces.
		if vt.Kind == VKOpaque && vt.Opaque.RawSize > 0 {
			ip.OpaqueValueGoType = GoTypeName(td.Name)
		}
		plan.Entries = append(plan.Entries, ip)
		result := *vt
		result.GoType = aliasName
		vt = &result
	}

	plan.Entries = append(plan.Entries, &TypedefViewPlan{
		AliasName: aliasName,
		ViewType:  vt,
	})
	return nil
}

// isVoidCase0Union checks if a ViewType is a union whose case 0 is void.
func (g *Generator) isVoidCase0Union(vt *ViewType) bool {
	if vt.Named == nil {
		return false
	}
	def, ok := g.TypeResolver[vt.Named.XDRName]
	if !ok || def.Kind != DKUnion {
		return false
	}
	for _, arm := range def.Union.Arms {
		for _, c := range arm.Cases {
			if c.Value == 0 {
				return arm.Type == nil
			}
		}
	}
	return false
}

// caseValueExpr returns a Go expression for a union case value, cast to int32.
func (g *Generator) caseValueExpr(u *UnionDef, caseIdx int, arm *UnionArm) (string, error) {
	c := arm.Cases[caseIdx]
	if c.Name == "" {
		return fmt.Sprintf("int32(%d)", c.Value), nil
	}
	discType, err := g.resolveTypeRef(&u.Discriminant.Type)
	if err != nil {
		return "", err
	}
	// Only enum (ref) discriminants have a Go identifier for a named case
	// (the enum member constant). For non-ref discriminants (bool/int/uint), a
	// named case such as TRUE/FALSE has no Go constant — emit the numeric literal
	// from c.Value, not GoTypeName(c.Name) (which would be an undefined identifier
	// and fail go build).
	if discType.Kind == TRRef {
		return fmt.Sprintf("int32(%s%s)", GoTypeName(discType.Name), GoTypeName(c.Name)), nil
	}
	return fmt.Sprintf("int32(%d)", c.Value), nil
}
