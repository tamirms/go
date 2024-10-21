package server

import (
	"fmt"
	"net/http"

	"github.com/stellar/go/support/render/problem"
)

const (
	// DefaultPageSize is the default page size for db queries
	DefaultPageSize = 10
	// MaxPageSize is the max page size for db queries
	MaxPageSize = 200

	// OrderAscending is used to indicate an ascending order in request params
	OrderAscending = "asc"

	// OrderDescending is used to indicate an descending order in request params
	OrderDescending = "desc"

	// DefaultPairSep is the default separator used to separate two numbers for CursorInt64Pair
	DefaultPairSep = "-"

	// ParamCursor is a query string param name
	ParamCursor = "cursor"
	// ParamOrder is a query string param name
	ParamOrder = "order"
	// ParamLimit is a query string param name
	ParamLimit = "limit"
)

var (
	// ErrInvalidOrder is an error that occurs when a user-provided order string
	// is invalid
	ErrInvalidOrder = &InvalidFieldError{"order"}
	// ErrInvalidLimit is an error that occurs when a user-provided limit num
	// is invalid
	ErrInvalidLimit = &InvalidFieldError{"limit"}
	// ErrInvalidCursor is an error that occurs when a user-provided cursor string
	// is invalid
	ErrInvalidCursor = &InvalidFieldError{"cursor"}
)

type InvalidFieldError struct {
	Name string
}

func (e *InvalidFieldError) Error() string {
	return fmt.Sprintf("%s: invalid value", e.Name)
}

type PageQuery struct {
	Order string
	Limit uint64
}

// Invert returns a new PageQuery whose order is reversed
func (p PageQuery) Invert() PageQuery {
	switch p.Order {
	case OrderAscending:
		p.Order = OrderDescending
	case OrderDescending:
		p.Order = OrderAscending
	}

	return p
}

// NewPageQuery creates a new PageQuery struct, ensuring the order, limit, and
// cursor are set to the appropriate defaults and are valid.
func NewPageQuery(
	order string,
	limit uint64,
) (result PageQuery, err error) {

	// Set order
	switch order {
	case "":
		result.Order = OrderAscending
	case OrderAscending, OrderDescending:
		result.Order = order
	default:
		err = ErrInvalidOrder
		return
	}

	// Set limit
	switch {
	case limit == 0:
		err = ErrInvalidLimit
		return
	case limit > MaxPageSize:
		err = ErrInvalidLimit
		return
	default:
		result.Limit = limit
	}

	return
}

// GetPageQuery is a helper that returns a new db.PageQuery struct initialized
// using the results from a call to GetPagingParams()
func GetPageQuery(r *http.Request) (PageQuery, error) {
	order, err := getString(r, ParamOrder)
	if err != nil {
		return PageQuery{}, err
	}
	limit, err := getLimit(r, ParamLimit, DefaultPageSize, MaxPageSize)
	if err != nil {
		return PageQuery{}, err
	}

	pageQuery, err := NewPageQuery(order, limit)
	if err != nil {
		if invalidFieldError, ok := err.(*InvalidFieldError); ok {
			err = problem.MakeInvalidFieldProblem(
				invalidFieldError.Name,
				err,
			)
		} else {
			err = problem.BadRequest
		}

		return PageQuery{}, err
	}

	return pageQuery, nil
}
