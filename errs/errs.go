package errs

import "net/http"

type AppError struct {
	Code    int
	Message string
}

func (e AppError) Error() string {
	return e.Message
}

func NewNotFoundError(msg string) error {
	return AppError{Code: http.StatusNotFound, Message: msg}
}

func NewValidationError(msg string) error {
	return AppError{Code: http.StatusUnprocessableEntity, Message: msg}
}

func NewBadRequestError(msg string) error {
	return AppError{Code: http.StatusBadRequest, Message: msg}
}

func NewConflictError(msg string) error {
	return AppError{Code: http.StatusConflict, Message: msg}
}

func NewUnexpectedError() error {
	return AppError{Code: http.StatusInternalServerError, Message: "unexpected error"}
}
