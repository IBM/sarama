package mocks

import (
	"errors"
)

// A simple interafce that includes the testing.T methods we use to report
// expectation violations when using the mock objects.
type ExpectationViolationReporter interface {
	Errorf(string, ...interface{})
}

var (
	errProduceSuccess    error = nil
	errOutOfExpectations       = errors.New("No more expectations set on mock producer")
)

type producerExpectation struct {
	Result error
}
