package xdr

// Successful returns true if the transaction succeeded
func (r TransactionResult) Successful() bool {
	return r.Result.Code == TransactionResultCodeTxSuccess ||
		r.Result.Code == TransactionResultCodeTxFeeBumpInnerSuccess
}

// Successful returns true if the transaction succeeded
func (r TransactionResultPair) Successful() bool {
	return r.Result.Successful()
}

// OperationResults returns the operation results for the transaction
func (r TransactionResultPair) OperationResults() ([]OperationResult, bool) {
	innerResults, ok := r.Result.Result.GetInnerResultPair()
	if ok {
		return innerResults.Result.Result.GetResults()
	}
	return r.Result.Result.GetResults()
}

// InnerHash returns the hash of the inner transaction,
// if there exists an inner transaction
func (r TransactionResultPair) InnerHash() (Hash, bool) {
	innerResults, ok := r.Result.Result.GetInnerResultPair()
	if !ok {
		return Hash{}, false
	}
	return innerResults.TransactionHash, true
}
