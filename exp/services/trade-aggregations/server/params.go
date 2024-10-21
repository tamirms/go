package server

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"unicode/utf8"

	"github.com/asaskevich/govalidator"
	"github.com/go-chi/chi"
	"github.com/gorilla/schema"

	"github.com/stellar/go/support/render/problem"
)

func checkUTF8(name, value string) error {
	if !utf8.ValidString(value) {
		return problem.MakeInvalidFieldProblem(name, fmt.Errorf("invalid value"))
	}
	return nil
}

// getURLParam returns the corresponding URL parameter value from the request
// routing context and an additional boolean reflecting whether or not the
// param was found. This is ported from Chi since the Chi version returns ""
// for params not found. This is undesirable since "" also is a valid url param.
// Ref: https://github.com/go-chi/chi/blob/d132b31857e5922a2cc7963f4fcfd8f46b3f2e97/context.go#L69
func getURLParam(r *http.Request, key string) (string, bool) {
	rctx := chi.RouteContext(r.Context())

	if rctx == nil {
		return "", false
	}

	// Return immediately if keys does not match Values
	// This can happen when a named param is not specified.
	// This is a bug in chi: https://github.com/go-chi/chi/issues/426
	if len(rctx.URLParams.Keys) != len(rctx.URLParams.Values) {
		return "", false
	}

	for k := len(rctx.URLParams.Keys) - 1; k >= 0; k-- {
		if rctx.URLParams.Keys[k] == key {
			return rctx.URLParams.Values[k], true
		}
	}

	return "", false
}

// getString retrieves a string from either the URLParams, form or query string.
// This method uses the priority (URLParams, Form, Query).
func getString(r *http.Request, name string) (string, error) {
	fromURL, ok := getURLParam(r, name)
	if ok {
		ret, err := url.PathUnescape(fromURL)
		if err != nil {
			return "", problem.MakeInvalidFieldProblem(name, err)
		}

		if err := checkUTF8(name, ret); err != nil {
			return "", err
		}

		return ret, nil
	}

	fromForm := r.FormValue(name)
	if fromForm != "" {
		if err := checkUTF8(name, fromForm); err != nil {
			return "", err
		}
		return fromForm, nil
	}

	value := r.URL.Query().Get(name)
	if err := checkUTF8(name, value); err != nil {
		return "", err
	}

	return value, nil
}

// Note from chi: it is a good idea to set a Decoder instance as a package
// global, because it caches meta-data about structs, and an instance can be
// shared safely:
var decoder = schema.NewDecoder()

// getParams fills a struct with values read from a request's query parameters.
func getParams(dst interface{}, r *http.Request) error {
	query := r.URL.Query()

	// Merge chi's URLParams with URL Query Params. Given
	// `/accounts/{account_id}/transactions?foo=bar`, chi's URLParams will
	// contain `account_id` and URL Query params will contain `foo`.
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		for _, key := range rctx.URLParams.Keys {
			if key == "*" {
				continue
			}
			param, _ := getURLParam(r, key)
			query.Set(key, param)
		}
	}

	if err := decoder.Decode(dst, query); err != nil {
		for k, e := range err.(schema.MultiError) {
			return problem.NewProblemWithInvalidField(
				problem.BadRequest,
				k,
				getSchemaErrorFieldMessage(k, e),
			)
		}
	}

	if _, err := govalidator.ValidateStruct(dst); err != nil {
		field, message := getErrorFieldMessage(err)
		err = problem.MakeInvalidFieldProblem(
			getSchemaTag(dst, field),
			fmt.Errorf(message),
		)

		return err
	}

	if v, ok := dst.(Validateable); ok {
		if err := v.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func getSchemaTag(params interface{}, field string) string {
	v := reflect.ValueOf(params).Elem()
	qt := v.Type()
	f, _ := qt.FieldByName(field)
	return f.Tag.Get("schema")
}

// getCursor retrieves a string from either the URLParams, form or query string.
// This method uses the priority (URLParams, Form, Query).
func getCursor(r *http.Request, name string) (string, error) {
	cursor, err := getString(r, name)

	if err != nil {
		return "", err
	}

	//if cursor == "now" {
	//	tid := toid.AfterLedger(ledgerState.CurrentStatus().HistoryLatest)
	//	cursor = tid.String()
	//}

	if lastEventID := r.Header.Get("Last-Event-ID"); lastEventID != "" {
		cursor = lastEventID
	}

	// In case cursor is negative value, return InvalidField error
	cursorInt, err := strconv.Atoi(cursor)
	if err == nil && cursorInt < 0 {
		msg := fmt.Sprintf("the cursor %d is a negative number: ", cursorInt)

		return "", problem.MakeInvalidFieldProblem(
			name,
			fmt.Errorf(msg),
		)
	}

	return cursor, nil
}

// getLimit retrieves a uint64 limit from the action parameter of the given
// name. Populates err if the value is not a valid limit.  Uses the provided
// default value if the limit parameter is a blank string.
func getLimit(r *http.Request, name string, def uint64, max uint64) (uint64, error) {
	limit, err := getString(r, name)

	if err != nil {
		return 0, err
	}
	if limit == "" {
		return def, nil
	}

	asI64, err := strconv.ParseInt(limit, 10, 64)
	if err != nil {

		return 0, problem.MakeInvalidFieldProblem(name, fmt.Errorf("unparseable value"))
	}

	if asI64 <= 0 {
		err = fmt.Errorf("invalid limit: non-positive value provided")
	} else if asI64 > int64(max) {
		err = fmt.Errorf("invalid limit: value provided that is over limit max of %d", max)
	}

	if err != nil {
		return 0, problem.MakeInvalidFieldProblem(name, err)
	}

	return uint64(asI64), nil
}

func init() {
	decoder.IgnoreUnknownKeys(true)
}
