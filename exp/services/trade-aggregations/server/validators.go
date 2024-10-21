package server

import (
	"fmt"
	"strings"

	"github.com/asaskevich/govalidator"
	"github.com/gorilla/schema"

	"github.com/stellar/go/xdr"
)

// Validateable allow structs to define their own custom validations.
type Validateable interface {
	Validate() error
}

func init() {
	govalidator.TagMap["accountID"] = isAccountID
	govalidator.TagMap["assetType"] = isAssetType
	govalidator.TagMap["asset"] = isAsset
}

var customTagsErrorMessages = map[string]string{
	"accountID": "Account ID must start with `G` and contain 56 alphanum characters",
	"asset":     "Asset must be the string \"native\" or a string of the form \"Code:IssuerAccountID\" for issued assets.",
	"assetType": "Asset type must be native, credit_alphanum4 or credit_alphanum12",
	"bool":      "Filter should be true or false",
}

// isAsset validates if string contains a valid SEP11 asset
func isAsset(assetString string) bool {
	var asset xdr.Asset

	if strings.ToLower(assetString) == "native" {
		if err := asset.SetNative(); err != nil {
			return false
		}
	} else {

		parts := strings.Split(assetString, ":")
		if len(parts) != 2 {
			return false
		}

		code := parts[0]
		if !xdr.ValidAssetCode.MatchString(code) {
			return false
		}

		issuer, err := xdr.AddressToAccountId(parts[1])
		if err != nil {
			return false
		}

		if err := asset.SetCredit(code, issuer); err != nil {
			return false
		}
	}

	return true
}

func isAccountID(str string) bool {
	if _, err := xdr.AddressToAccountId(str); err != nil {
		return false
	}

	return true
}

func getSchemaErrorFieldMessage(field string, err error) error {
	if customMessage, ok := customTagsErrorMessages[field]; ok {
		return fmt.Errorf(customMessage)
	}

	if ce, ok := err.(schema.ConversionError); ok {
		customMessage, ok := customTagsErrorMessages[ce.Type.String()]
		if ok {
			return fmt.Errorf(customMessage)
		}
	}

	return err
}

func getErrorFieldMessage(err error) (string, string) {
	var field, message string

	switch err := err.(type) {
	case govalidator.Error:
		field = err.Name
		validator := err.Validator
		m, ok := customTagsErrorMessages[validator]
		// Give priority to inline custom error messages.
		// CustomErrorMessageExists when the validator is defined like:
		// `validatorName~custom message`
		if !ok || err.CustomErrorMessageExists {
			m = err.Err.Error()
		}
		message = m
	case govalidator.Errors:
		for _, item := range err.Errors() {
			field, message = getErrorFieldMessage(item)
			break
		}
	}

	return field, message
}

// AssetTypeMap is the read-only (i.e. don't modify it) map from string names to
// xdr.AssetType values
var AssetTypeMap = map[string]xdr.AssetType{
	"native":            xdr.AssetTypeAssetTypeNative,
	"credit_alphanum4":  xdr.AssetTypeAssetTypeCreditAlphanum4,
	"credit_alphanum12": xdr.AssetTypeAssetTypeCreditAlphanum12,
}

func isAssetType(str string) bool {
	_, ok := AssetTypeMap[str]
	return ok
}
