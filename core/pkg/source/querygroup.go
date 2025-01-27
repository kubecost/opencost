package source

type QueryGroup struct {
	errorCollector *QueryErrorCollector
}

type QueryGroupAsyncResult struct {
	errorCollector *QueryErrorCollector
	resultsChan    QueryResultsChan
}

func NewQueryGroup() *QueryGroup {
	var errorCollector QueryErrorCollector

	return &QueryGroup{
		errorCollector: &errorCollector,
	}
}

func (qg *QueryGroup) With(resultsChan QueryResultsChan) *QueryGroupAsyncResult {
	return newQueryGroupAsyncResult(qg.errorCollector, resultsChan)
}

func (qg *QueryGroup) HasErrors() bool {
	return qg.errorCollector.IsError()
}

func (qg *QueryGroup) Error() error {
	return qg.errorCollector
}

func (qg *QueryGroup) Errors() []*QueryError {
	return qg.errorCollector.Errors()
}

func newQueryGroupAsyncResult(collector *QueryErrorCollector, resultsChan QueryResultsChan) *QueryGroupAsyncResult {
	return &QueryGroupAsyncResult{
		errorCollector: collector,
		resultsChan:    resultsChan,
	}
}

func (qgar *QueryGroupAsyncResult) Await() ([]*QueryResult, error) {
	defer close(qgar.resultsChan)
	result := <-qgar.resultsChan

	q := result.Query
	err := result.Error

	if err != nil {
		qgar.errorCollector.AppendError(&QueryError{Query: q, Error: err})
		return nil, err
	}

	return result.Results, nil
}

type QueryResultCollection []*QueryResults

func (qrc *QueryResultCollection) HasErrors() bool {
	for _, qr := range *qrc {
		if qr.Error != nil {
			return true
		}
	}
	return false
}

func (qrc *QueryResultCollection) Error() error {
	var errCollection QueryErrorCollector

	for _, qr := range *qrc {
		q := qr.Query
		e := qr.Error

		if e != nil {
			if IsErrorCollection(e) {
				if errs, ok := e.(QueryErrorCollection); ok {
					for _, qErr := range errs.Errors() {
						errCollection.AppendError(qErr)
					}
					for _, qWarn := range errs.Warnings() {
						errCollection.AppendWarning(qWarn)
					}
				} else {
					errCollection.AppendError(&QueryError{Query: q, Error: e})
				}
			} else {
				errCollection.AppendError(&QueryError{Query: q, Error: e})
			}
		}
	}

	return &errCollection
}
