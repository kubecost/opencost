package protocol

import (
	"fmt"
	"net/http"

	"github.com/opencost/opencost/core/pkg/util/json"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// HTTPProtocol is a struct used as a selector for request/response protocol utility methods
type HTTPProtocol struct{}

// HTTPError represents an http error response
type HTTPError struct {
	StatusCode int
	Body       string
}

// Error returns the error string
func (he HTTPError) Error() string {
	return string(he.Body)
}

// BadRequest creates a BadRequest HTTPError
func (hp HTTPProtocol) BadRequest(message string) HTTPError {
	return HTTPError{
		StatusCode: http.StatusBadRequest,
		Body:       message,
	}
}

// UnprocessableEntity creates an UnprocessableEntity HTTPError
func (hp HTTPProtocol) UnprocessableEntity(message string) HTTPError {
	if message == "" {
		message = "Unprocessable Entity"
	}
	return HTTPError{
		StatusCode: http.StatusUnprocessableEntity,
		Body:       message,
	}
}

// InternalServerError creates an InternalServerError HTTPError
func (hp HTTPProtocol) InternalServerError(message string) HTTPError {
	if message == "" {
		message = "Internal Server Error"
	}
	return HTTPError{
		StatusCode: http.StatusInternalServerError,
		Body:       message,
	}
}

// NotFound creates a NotFound HTTPError
func (hp HTTPProtocol) NotFound() HTTPError {
	return HTTPError{
		StatusCode: http.StatusNotFound,
		Body:       "Not Found",
	}
}

// HTTPResponse represents a data envelope for our HTTP messaging
type HTTPResponse struct {
	Code    int         `json:"code"`
	Data    interface{} `json:"data"`
	Message string      `json:"message,omitempty"`
	Warning string      `json:"warning,omitempty"`
}

// ToResponse accepts a data payload and/or error to encode into a new HTTPResponse instance. Responses
// which should not contain an error should pass nil for err.
func (hp HTTPProtocol) ToResponse(data interface{}, err error) *HTTPResponse {
	if err != nil {
		return &HTTPResponse{
			Code:    http.StatusInternalServerError,
			Data:    data,
			Message: err.Error(),
		}
	}

	return &HTTPResponse{
		Code: http.StatusOK,
		Data: data,
	}
}

// WriteData wraps the data payload in an HTTPResponse and writes the resulting response using the
// http.ResponseWriter
func (hp HTTPProtocol) WriteData(w http.ResponseWriter, data interface{}) {
	status := http.StatusOK
	resp, err := json.Marshal(&HTTPResponse{
		Code: status,
		Data: data,
	})
	if err != nil {
		status = http.StatusInternalServerError
		resp, _ = json.Marshal(&HTTPResponse{
			Code:    status,
			Message: fmt.Sprintf("Error: %s", err),
		})
	}

	w.WriteHeader(status)
	w.Write(resp)
}

// WriteDataWithWarning writes the data payload similiar to WriteData except it provides an additional warning message.
func (hp HTTPProtocol) WriteDataWithWarning(w http.ResponseWriter, data interface{}, warning string) {
	status := http.StatusOK
	resp, err := json.Marshal(&HTTPResponse{
		Code:    status,
		Data:    data,
		Warning: warning,
	})
	if err != nil {
		status = http.StatusInternalServerError
		resp, _ = json.Marshal(&HTTPResponse{
			Code:    status,
			Message: fmt.Sprintf("Error: %s", err),
		})
	}

	w.WriteHeader(status)
	w.Write(resp)
}

// WriteDataWithMessage writes the data payload similiar to WriteData except it provides an additional string message.
func (hp HTTPProtocol) WriteDataWithMessage(w http.ResponseWriter, data interface{}, message string) {
	status := http.StatusOK
	resp, err := json.Marshal(&HTTPResponse{
		Code:    status,
		Data:    data,
		Message: message,
	})
	if err != nil {
		status = http.StatusInternalServerError
		resp, _ = json.Marshal(&HTTPResponse{
			Code:    status,
			Message: fmt.Sprintf("Error: %s", err),
		})
	}

	w.WriteHeader(status)
	w.Write(resp)
}

// WriteProtoWithMessage uses the protojson package to convert proto3 response to json response and
// return it to the requester. Proto3 drops messages with default values but overriding the param
// EmitUnpopulated to true it returns default values in the Json response payload. If error is
// encountered it sent InternalServerError and the error why the json conversion failed.
func (hp HTTPProtocol) WriteProtoWithMessage(w http.ResponseWriter, data proto.Message) {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	status := http.StatusOK
	resp, err := m.Marshal(data)
	if err != nil {
		status = http.StatusInternalServerError
		resp, _ = json.Marshal(&HTTPResponse{
			Message: fmt.Sprintf("Error: %s", err),
		})
	}

	w.WriteHeader(status)
	w.Write(resp)
}

// WriteDataWithMessageAndWarning writes the data payload similiar to WriteData except it provides a warning and additional message string.
func (hp HTTPProtocol) WriteDataWithMessageAndWarning(w http.ResponseWriter, data interface{}, message string, warning string) {
	status := http.StatusOK
	resp, err := json.Marshal(&HTTPResponse{
		Code:    status,
		Data:    data,
		Message: message,
		Warning: warning,
	})
	if err != nil {
		status = http.StatusInternalServerError
		resp, _ = json.Marshal(&HTTPResponse{
			Code:    status,
			Message: fmt.Sprintf("Error: %s", err),
		})
	}

	w.WriteHeader(status)
	w.Write(resp)
}

// WriteError wraps the HTTPError in a HTTPResponse and writes it via http.ResponseWriter
func (hp HTTPProtocol) WriteError(w http.ResponseWriter, err HTTPError) {
	status := err.StatusCode
	if status == 0 {
		status = http.StatusInternalServerError
	}
	w.WriteHeader(status)

	resp, _ := json.Marshal(&HTTPResponse{
		Code:    status,
		Message: err.Body,
	})
	w.Write(resp)
}

// WriteResponse writes the provided HTTPResponse instance via http.ResponseWriter
func (hp HTTPProtocol) WriteResponse(w http.ResponseWriter, r *HTTPResponse) {
	status := r.Code
	resp, err := json.Marshal(r)
	if err != nil {
		status = http.StatusInternalServerError
		resp, _ = json.Marshal(&HTTPResponse{
			Code:    status,
			Message: fmt.Sprintf("Error: %s", err),
		})
	}

	w.WriteHeader(status)
	w.Write(resp)
}
