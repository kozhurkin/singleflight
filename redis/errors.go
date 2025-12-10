package redisflight

import (
	"errors"
	"fmt"
)

// BackendError оборачивает ошибку, возникшую при работе с Backend'ом.
// С помощью IsBackendError можно отличить её от ошибок, возвращённых fn.
type BackendError struct {
	Op  string // операция Backend'а, в которой произошла ошибка (например, "GetResultWithTTL", "TryLock")
	Err error  // исходная ошибка Backend'а
}

func (e *BackendError) Error() string {
	return fmt.Sprintf("redisflight backend %s: %v", e.Op, e.Err)
}

func (e *BackendError) Unwrap() error {
	return e.Err
}

// IsBackendError возвращает true, если err была вызвана работой с Backend'ом
// (и, соответственно, завернута в *BackendError).
func IsBackendError(err error) bool {
	var be *BackendError
	return errors.As(err, &be)
}


