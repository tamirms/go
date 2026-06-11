# XDR Views

## The Problem

Reading any field from a `LedgerCloseMeta` normally requires decoding the entire message into Go structs — every transaction, every operation, every ledger change. A typical pubnet ledger is ~1.5MB of XDR (median). Decoding it allocates ~8.5MB across ~107,000 Go objects (the decoded representation is larger than the wire format due to pointers, slice headers, etc.), even if you only need one transaction hash.

## The Idea

XDR's wire format is prefix-deterministic — given the schema, you can compute the byte offset of any field by reading length prefixes and discriminants, without decoding the full message. Views provide this: typed, read-only windows into raw XDR bytes that parse lazily on access.

```go
// Full decode: decode everything, use one field
var data []byte = getXDRBytes()
var lcm xdr.LedgerCloseMeta
err := lcm.UnmarshalBinary(data)          // ~8.5MB allocated, ~107K objects
seq := lcm.MustV1().LedgerHeader.Header.LedgerSeq

// View: navigate directly to the field
view := xdr.LedgerCloseMetaView(data)     // zero cost — just a type cast
seq := view.MustV1().MustLedgerHeader().MustHeader().MustLedgerSeq().MustValue()

// Or with error handling:
v1, err := view.V1()                        // read 4-byte discriminant, return sub-view at V1 arm
hdr, err := v1.LedgerHeader()               // read preceding field sizes to find offset, return sub-view
header, err := hdr.Header()                 // read preceding field sizes to find offset, return sub-view
seqView, err := header.LedgerSeq()          // read preceding field sizes to find offset, return sub-view
seq, err := seqView.Value()                 // read 4 bytes, decode as uint32
```

The view path reads the union discriminant (4 bytes), reads a few length prefixes to skip past preceding struct fields, then reads the 4-byte sequence number. Only a small fraction of the buffer is touched. Everything else is skipped entirely.

## How It Works

A view is a named `[]byte` type. Creating one is a type cast — no copies, no allocations:

```go
type LedgerCloseMetaView []byte

var data []byte = getXDRBytes()
view := LedgerCloseMetaView(data)
```

Every XDR struct, union, enum, and typedef has a corresponding view type. Each field of a struct becomes a method that returns a sub-view — a `[]byte` slice starting at that field's byte offset. Sub-views are "fat slices": they extend to the end of the parent buffer, not just the field's own extent. This avoids computing the field's size during navigation. The exact bytes for a view can be extracted later with `xdr.Raw()`.

```go
v1, err := view.V1()                  // LedgerCloseMetaV1View
if err != nil { return err }
hdr, err := v1.LedgerHeader()         // LedgerHeaderHistoryEntryView
if err != nil { return err }
header, err := hdr.Header()           // LedgerHeaderView
if err != nil { return err }
seqView, err := header.LedgerSeq()    // Uint32View
if err != nil { return err }
```

Each accessor returns `(T, error)`. The error is non-nil if the data is truncated or malformed. Each call computes the byte offset of the requested field and returns a sub-view starting there. No intermediate Go structs are created. No heap allocations occur.

At the leaves of the type hierarchy are primitive views like `Uint32View`, `Int64View`, `BoolView`. These have no sub-fields to navigate into. Instead, they expose `Value()` which decodes the raw bytes into a Go type:

```go
seq, err := seqView.Value()           // uint32
```

## Navigating Structs

Struct fields become typed methods. Each returns a sub-view that you can navigate further or extract a value from. Error checks omitted for brevity in the remaining examples:

```go
// Given a LedgerEntryView:
entryData, err := ledgerEntry.Data()       // LedgerEntryDataView (a union)
account, err := entryData.Account()        // AccountEntryView (a struct)
balance, err := account.Balance()          // Int64View (a leaf)
val, err := balance.Value()                // int64
```

### Fields(): every field of a node in one walk

A field accessor locates one field by walking the bytes of the fields that precede it. When you need *several* fields of the same struct, calling each accessor separately re-walks the same prefix each time. `Fields()` locates every field of a struct in a single pass and hands back a bundle of trimmed sub-views:

```go
// Generated for every struct view:
type TransactionResultMetaV1Fields struct {
    View                     TransactionResultMetaV1View // the whole node, trimmed to its exact extent
    Ext                      ExtensionPointView          // every field, trimmed
    Result                   TransactionResultPairView
    FeeProcessing            LedgerEntryChangesView
    TxApplyProcessing        TransactionMetaView
    PostTxApplyFeeProcessing LedgerEntryChangesView
}

fields, err := meta.Fields()
result := fields.Result            // already trimmed: []byte(result) is its exact wire bytes
feeProc := fields.FeeProcessing
```

Each field in the bundle is trimmed to its exact wire extent, so `[]byte(field)` is the field's raw bytes with no further sizing walk. The bundle also carries the whole node as `View` (trimmed for free, since the walk already computed the node's total extent), which makes `Fields()` the natural way to trim a fat struct view. A `Fields` bundle is a handful of slice headers on the stack — nothing is allocated and nothing is copied.

`Fields()` is generated for structs only. Unions and optionals do not need it: their only variable-size component is also their last one (the arm of a union starts at a fixed offset and runs to the end; the inner value of an optional does the same after the presence flag), so on a trimmed view their arm accessor and `Unwrap()` already return trimmed results.

If a struct has a field whose accessor is already named `Fields`, the bundle method is emitted as `Fields_` to avoid a method-set collision.

## Navigating Unions

A union has a discriminant accessor (named after its switch field, e.g. `V()`, `Type()`, `C()`) and one method per arm. The discriminant accessor returns the **decoded** discriminant value directly, not a leaf view:

```go
// Given a LedgerEntryDataView:
discVal, err := entryData.Type()           // LedgerEntryType (the decoded enum value)

account, err := entryData.Account()        // works if disc == ACCOUNT
trustline, err := entryData.TrustLine()    // works if disc == TRUSTLINE
// calling the wrong arm returns ViewErrWrongDiscriminant
```

Enum-discriminated unions return the Go enum type, with the same known-value validation an enum leaf view performs. Int-discriminated unions return `int32` with no value validation, so `default:` arms stay reachable for unknown protocol versions and the unknown value remains printable in error messages. Bool-discriminated unions return `bool`. This mirrors how optionals already decode their presence flag inline rather than returning a `BoolView`.

## Leaf Types

Leaf views have no sub-fields. Instead of returning sub-views, they expose `Value()` which decodes the raw bytes into a Go type:

| View Type | `Value()` returns |
|-----------|-------------------|
| `Int32View` | `int32` |
| `Uint32View` | `uint32` |
| `Int64View` | `int64` |
| `Uint64View` | `uint64` |
| `BoolView` | `bool` (strict 0 or 1) |
| `Float32View` | `float32` |
| `Float64View` | `float64` |
| Enum views (e.g., `LedgerEntryTypeView`) | The Go enum type (e.g., `LedgerEntryType`) |
| Fixed opaque views (e.g., `HashView`) | The schema array type (e.g., `Hash`, or `[N]byte`) — a copy, not aliasing |
| Variable opaque / string views (e.g., `VarOpaqueView`) | `[]byte` (variable length, aliasing) |
| Bounded opaque / string views (e.g., `String32View`) | `[]byte` (enforces max length, aliasing) |

Fixed-size opaque `Value()` returns the same Go array type the struct decoder produces (`Hash`, `[4]byte`, …) **by value**. The returned array is a copy, so it is safe to retain past the source buffer's lifetime. Variable and bounded opaques/strings return an aliasing `[]byte`; copy them if you need to keep them past the source buffer.

```go
// Given a TransactionResultPairView:
hashView, err := txResultPair.TransactionHash() // HashView (fixed opaque[32])
hash, err := hashView.Value()                   // Hash ([32]byte), a copy

// Given an AccountEntryView:
domainView, err := account.HomeDomain()         // String32View (bounded string<32>)
domainBytes, err := domainView.Value()          // []byte, up to 32 bytes (aliasing)
```

## Arrays

Arrays expose `Count()`/`Len()` and random access via `At(i)`. The cursor returned by `Scan()` is the one way to walk an array sequentially.

```go
count, err := arr.Count()            // (int, error) — reads + validates count from wire (variable arrays)
n := arr.Len()                       // int — compile-time constant, never fails (fixed arrays)

elem, err := arr.At(5)               // random access to element 5
```

For variable arrays, `Count()` reads the 4-byte wire count and validates it against both the schema maximum and the remaining buffer, so it is safe to use for preallocation. Fixed arrays use `Len()`, whose count comes from the schema. Bounded arrays (`T<100>`) enforce their max count in `Count()` and `At()`.

Random access via `At(i)` is O(i) for variable-size elements, because preceding elements must be walked to compute offsets. Prefer the `Scan()` cursor for sequential access; on variable-size-element arrays an `At(i)` loop over the index is accidental O(n²).

### Scan() cursors

`Scan()` returns a cursor that walks the array one element at a time, in a `bufio.Scanner`-style loop:

```go
c := arr.Scan()
for c.Next() {
    elem := c.Elem()      // current element (located bundle for structs; trimmed view otherwise)
    raw := c.Bytes()      // current element's exact wire bytes (zero cost)
    idx := c.Index()      // 0-based position of the current element
    // ... transform elem into a domain type
}
if err := c.Err(); err != nil {
    return err
}
```

The cursor API:

| Method | Returns |
|--------|---------|
| `Next() bool` | advances; `false` at end or on error |
| `Count() int` | validated element count (safe for preallocation) |
| `Index() int` | 0-based position; `-1` before the first `Next()` |
| `Elem()` | the current element, located |
| `Bytes() []byte` | the current element's exact wire bytes |
| `Err() error` | the first error encountered, if any |

`Next()` advances by walking the current element exactly once, capturing the field extents it needs along the way. For **struct-element** arrays, `Elem()` returns the element's `Fields` bundle (whose `View` field is the trimmed element), so the fields are already located — `[]byte(field)` is free. For arrays of primitives, opaques, unions, optionals, and nested arrays, `Elem()` returns the plain trimmed element view. `Bytes()` returns the element's exact wire bytes at zero cost, since `Next()` already computed the extent — useful for the "store every element's raw bytes" pattern. `At(i)`, unlike `Elem()`, always returns the plain trimmed element view, even for struct elements — it does not hand back a `Fields` bundle. To get the located fields of a struct element obtained via `At(i)`, call `.Fields()` on it.

For **fixed-size elements** the cursor carries no capture state: `Next()` is an offset increment plus a bounds check (O(1) per element).

**Cursor contract:**

- `Elem()`, `Bytes()`, and a `Fields` bundle accessed before the first `Next()`, after `Next()` returns false, or after a sticky error return zero values and arm a sticky `ViewErrCursorMisuse`, surfaced at `Err()`. The first error wins.
- **Retention is allowed.** Yielded views and bundles alias the immutable input buffer, not cursor-internal state; they remain valid after further `Next()` calls and after the cursor is discarded. No defensive copying is needed.
- A cursor is a plain struct. Copying one is a cheap checkpoint: the copy resumes independently from the same position. A single cursor must not be used from multiple goroutines concurrently, but distinct cursors over the same buffer are safe.
- `Scan()` returns the cursor **by value**, so plain loops allocate nothing, independent of escape analysis.

There is no generated `All()`/`Iter()` or per-element `Must` family. Materializing a slice is a three-line cursor loop; in practice consumers transform each element into a domain type inside the loop rather than holding a slice of views.

## Optionals

```go
inner, present, err := opt.Unwrap()
if present {
    // use inner (another view)
}
```

## Package-level helpers: Raw, Copy, Validate

`Raw`, `Copy`, and `Validate` are package-level generic functions over the sealed `View` constraint, callable on any view value.

### Extracting raw bytes

To get the exact XDR wire bytes for any view, use `xdr.Raw`:

```go
raw, err := xdr.Raw(txResult)   // the exact bytes, no trailing data
```

This is how you extract a sub-message for storage or forwarding without decoding it. Do not use `[]byte(v)` — views are fat slices that include trailing bytes. `xdr.Raw` trims to the exact wire extent. (For trimmed values you already have — a `Fields` bundle field or a cursor's `Elem()`/`Bytes()` — `[]byte(v)` is already exact, so `xdr.Raw` is unnecessary there.)

### Copying

Views alias the original buffer. If you need an independent copy that outlives the original:

```go
copied, err := xdr.Copy(view)   // new allocation, safe to use after the original is freed
```

### Validation

Views validate incrementally during navigation — every field accessor checks bounds before reading and returns `(T, error)`. There is no way to get a value from a view without error checking, so navigating a view on well-formed data always succeeds, and navigating on malformed data returns errors at the point of access.

For an upfront guarantee, `xdr.Validate` traverses the **entire** structure checking bounds, schema constraints (max lengths, known enum values, bool 0/1, zero padding bytes), and nesting depth:

```go
err := xdr.Validate(view)
```

After `xdr.Validate` succeeds, all field accessors on that view are guaranteed to succeed, provided the underlying buffer is not modified. The normal navigation path already validates incrementally; `xdr.Validate` just does it exhaustively and upfront. This follows the same pattern as [Cap'n Proto](https://capnproto.org/encoding.html#security-considerations), which validates lazily on each pointer traversal rather than upfront.

For trusted input (e.g., from captive core or a verified ledger archive), `xdr.Validate` is not necessary — the per-access validation is sufficient, and the full traversal cost (~730µs per 1.5 MB ledger) can be avoided.

## Errors

All accessors return `(T, error)`. Errors are `*ViewError`:

```go
type ViewError struct {
    Kind   ViewErrorKind
    Offset uint32
    Detail string
}
```

| Kind | Meaning |
|------|---------|
| `ViewErrShortBuffer` | Data truncated |
| `ViewErrWrongDiscriminant` | Accessed wrong union arm |
| `ViewErrUnknownDiscriminant` | Discriminant not in schema |
| `ViewErrIndexOutOfRange` | Array index out of bounds |
| `ViewErrArrayCountExceedsData` | Array count exceeds remaining data |
| `ViewErrArrayCountExceedsMax` | Array count exceeds schema bound |
| `ViewErrOpaqueExceedsMax` | Opaque/string exceeds schema max length |
| `ViewErrBadBoolValue` | Bool is not 0 or 1 |
| `ViewErrMaxDepth` | Nesting depth exceeded internal limit |
| `ViewErrNonZeroPadding` | Padding byte is not zero |
| `ViewErrCursorMisuse` | Cursor accessed before first `Next`, after exhaustion, or after an error |

### Must methods

Single-valued accessors — struct fields, union arms, `Unwrap`, and leaf `Value()` — have a `Must` variant that panics on error instead of returning it. `xdr.MustRaw` is the package-level companion for dives that terminate in bytes rather than a scalar.

```go
// Error-checked:
seqView, err := header.LedgerSeq()
seq, err := seqView.Value()

// Must (panics on error):
seq := header.MustLedgerSeq().MustValue()

// MustRaw for a dive ending in bytes (e.g. envelope hashing):
raw := xdr.MustRaw(tx.MustResult())
```

`Must` is confined to single-valued navigation, which is the deep-scalar use case; array iteration uses the `Scan()` cursor, which owns its own error handling via `Err()`. Must methods are safe after `xdr.Validate` succeeds, or on trusted input. They also work inside `Try` blocks.

### Try / TryVoid

`xdr.Try` and `xdr.TryVoid` recover panics from Must methods and return them as errors, enabling clean navigation without per-field error checks:

```go
result, err := xdr.Try(func() uint32 {
    view := xdr.LedgerCloseMetaView(data)
    return view.MustV1().MustLedgerHeader().MustHeader().MustLedgerSeq().MustValue()
})

err := xdr.TryVoid(func() {
    c := view.MustV1().MustTxProcessing().Scan()
    for c.Next() {
        hash := c.Elem().Result.MustTransactionHash().MustValue()
        // ...
    }
    if e := c.Err(); e != nil {
        panic(e)
    }
})
```

Only `*ViewError` panics are caught — other panics propagate normally. Must methods must be called in the same goroutine as `Try`.

## Performance

Benchmarked on a real pubnet ledger (`xdr/testdata/ledger_58752000.bin`: ~1.3 MB, 249 transactions, 18 contract events) on Apple M1 Max, Go 1.25, comparing full struct decode (`SafeUnmarshal` + re-marshal) against the view path. Numbers are the median of 3 runs (`-benchtime 2s -count 3`).

| Workload | Full decode | View | Speedup |
|---|---:|---:|---:|
| Find tx by hash (early match) | 4,895µs | 32µs | **151x** |
| Find tx by hash (mid match) | 4,948µs | 99µs | **50x** |
| Find tx by hash (late match) | 4,917µs | 251µs | **20x** |
| Extract events by tx hash | 5,218µs | 133µs | **39x** |
| Extract all tx hashes | 5,224µs | 267µs | **20x** |
| Extract all events | 5,635µs | 432µs | **13x** |
| Extract all transactions | 7,615µs | 273µs | **28x** |

Allocations per ledger:

| Workload | Full decode | View |
|---|---|---|
| Extract all events | 7.2 MB / 91,621 allocs | 20 KB / 40 allocs |
| Extract events by tx hash | 7.2 MB / 91,307 allocs | 2.2 KB / 6 allocs |
| Extract all transactions | 10.7 MB / 106,697 allocs | 27 KB / 1 alloc |
| Extract all tx hashes | 7.2 MB / 91,239 allocs | 8 KB / 1 alloc |
| Find tx by hash | ~7.2 MB / ~91,258 allocs | 0 B / 0 allocs |

Full decode allocates ~7–11 MB across ~91,000–107,000 objects per ledger and its time is constant regardless of which fields are accessed — it always decodes everything. Views perform 0 heap allocations for navigation; the only allocations are the caller's own output slice (and only if it chooses to materialize). `xdr.Raw` returns a subslice of the original buffer (zero allocation); allocations occur only on `xdr.Copy`.

View time scales with how much data is touched: finding a transaction by hash near the start of the array (32µs) is ~8x faster than scanning to the end (251µs).

The largest win is **extract all transactions** (28x faster, and a single allocation versus ~107,000): each element yields three whole-field extents (Result, FeeProcessing, TxApplyProcessing) that the cursor locates in a single `Next()` walk; the field accessors are then free slice expressions. Single-field-per-element workloads (extract all hashes, find by hash) still walk each element once to advance, so they sit closer to XDR's inherent sizing-walk floor, but at a fraction of full decode's allocations.

## Design notes

A few design choices worth calling out for readers extending the views.

**Views are named `[]byte`, not structs.** A struct view (`type FooView struct { buf []byte; size int32 }`) would fix the `[]byte` footgun in the type system and allow size memoization, but a named slice is exactly the shape Go optimizes best — scalar replacement in SSA, register passing, sub-views as bare slice expressions. A larger struct with extra bookkeeping degrades at every non-inlinable boundary and pushes accessors over inlining budgets. The representation stays a named `[]byte`; the sticky error therefore lives in the `Scan()` cursor (a per-loop struct, where the cost does not multiply per value) rather than on every view.

**Cursors, not iterators.** With trimmed yields a cursor and a trimmed `iter.Seq2` cover the same element access, but the cursor differs in three ways. First, error shape: `iter.Seq2[E, error]` forces every loop body to handle an error per iteration, whereas a cursor checks `Err()` once. Second, advance byproducts: `Next()`'s walk computes the element's field extents, and because the cursor has state it keeps them — that is what makes the located `Elem()`, `Bytes()`, and `Index()` free; a view obtained any other way is a bare slice whose `Fields()` must re-walk. Third, predictable allocation: a by-value concrete cursor's behavior does not depend on escape analysis, while closures crossing generic boundaries heap-allocate.

**Field accessors stay fat; yields are trimmed.** Trimming a field accessor would require sizing the field's entire subtree, which is exactly the work lazy navigation exists to avoid — trimming `V1()` on a `LedgerCloseMeta` would walk the whole message to read one header field. Array element yields, by contrast, are trimmed for free: advancing past element *i* already requires `size(i)`, so the only case that pays anything is a search that breaks at an element and never reads its extent (one `size()` call); for fixed-size elements even that is free.

**Counts are validated up front.** xdrgen computes a minimum wire size for every type (clamped to a floor of 1) and the shared count helper rejects, in O(1), any wire count whose elements cannot fit the remaining buffer. This makes `Count()` safe for preallocation and bounds memory to O(payload), closing an allocation-amplification hole where a tiny buffer declaring a huge count could drive a multi-gigabyte allocation. The check runs once per array, never per element. For trusted input, preallocate from `Count()`; for untrusted input, prefer append-growth, which self-limits to the true element count.

## Security

Views are designed to safely handle untrusted input. Here is what the implementation guarantees, what callers should be aware of, and known failure modes.

### Guaranteed by the implementation

**No panics on malformed input (error-returning API).** Every slice operation is preceded by a bounds check. All error-returning accessors (`Field()`, `Value()`, `At()`, `Fields()`, the cursor methods, etc.) never panic, even on truncated, corrupt, or adversarial data. Must methods (`MustField()`, `MustValue()`, `xdr.MustRaw`, etc.) panic on error by design — use them inside `Try` blocks or after `xdr.Validate` succeeds.

**No unbounded memory allocation.** View construction is a zero-cost type cast. Navigation allocates nothing on the heap. `xdr.Raw` returns a subslice of the original buffer (zero allocation). `xdr.Copy` allocates exactly the bytes needed. Wire array counts are validated against the remaining buffer before use, so a small buffer cannot drive a large allocation.

**Nesting depth limit.** XDR allows recursive types (e.g., `ClaimPredicate`, `SCVal`). All view operations — field navigation, `xdr.Raw`, `xdr.Validate`, cursor advance — enforce a fixed recursion depth limit of 1,500, matching stellar-core's `xdr::marshaling_stack_limit`. Real-world Stellar XDR nests under 20 levels; 1,500 provides ample headroom for future schema evolution.

**Padding byte validation.** Both `xdr.Validate` and `Value()` reject non-zero XDR padding bytes with `ViewErrNonZeroPadding`, matching the behavior of the `go-xdr` decoder used by `SafeUnmarshal`.

**Integer overflow safety.** Struct field traversal, size computation, and validation accumulate offsets in `int64`, so those walks cannot overflow on either 32-bit or 64-bit platforms. Array iteration accumulates its running offset in `int`, but every element advance is preceded by a bounds check against the buffer length, so the offset stays within one element's size of the buffer and cannot run away. Wire-level element counts are validated as signed 32-bit integers (max 2,147,483,647), and the up-front count check rejects any array whose declared elements cannot fit the remaining buffer before iteration begins.

**No amplification attacks.** Processing time is proportional to the data actually present in the buffer, not to wire-declared counts. A small buffer with a large declared array count is rejected in O(1) for fixed-size elements and in O(data size) for variable-size elements. **Limiting the input payload size is sufficient to bound both CPU and memory usage** — views allocate nothing during navigation, and `xdr.Copy` allocates at most the payload size.

### Known failure modes

**Extremely deep nesting is rejected.** The recursion depth limit is 1,500, matching stellar-core's `xdr::marshaling_stack_limit`. XDR data nested deeper than this returns `ViewErrMaxDepth`. This limit is fixed and not configurable. Current real-world Stellar XDR nests under 20 levels.

**All mutation of the underlying buffer is unsafe.** Views alias the underlying buffer and assume the bytes are immutable for the view's lifetime. Any modification to the buffer (serial or concurrent) may cause views to return corrupt data or errors. Views are safe for concurrent reads from multiple goroutines.

### Caller responsibilities

**Check errors, use Try, or call `xdr.Validate`.** Views validate incrementally — every accessor returns `(T, error)` and checks bounds before reading. Three styles:
1. Check each error individually.
2. Use Must methods inside `Try`/`TryVoid` for clean chaining.
3. Call `xdr.Validate` once upfront, then use Must methods freely.

For trusted input (e.g., from captive core or a verified ledger archive), `xdr.Validate` is not necessary — the per-access validation is sufficient, and the full traversal cost can be avoided.

**Read `Err()` after a cursor loop.** On truncated input a cursor's `Next()` returns false early with no other signal, so a missing `Err()` check is silent data loss. Always check `c.Err()` after the loop.

**Use `xdr.Raw`, not `[]byte(v)`, on fat views.** Field accessors return fat slices that extend beyond the value's wire extent. Converting such a view to `[]byte` directly includes trailing bytes from sibling fields. Use `xdr.Raw` to extract the exact wire bytes. (Values that are already trimmed — `Fields` bundle fields, cursor `Elem()`/`Bytes()` — are exact, so `[]byte(v)` is fine there.)
