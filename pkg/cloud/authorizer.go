package cloud

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/util/json"
)

// AuthorizerTypeProperty is the property where the id of an Authorizer should be placed in its custom MarshalJSON function
const AuthorizerTypeProperty = "authorizerType"

type Authorizer interface {
	Config
	json.Marshaler
}

// AuthorizerSelectorFn implementations of this function should be a simple switch
// and acts as a register for the Authorizer types, returned Authorizer should be empty
// except for its default type property and will have other values marshalled into it
type AuthorizerSelectorFn[T Authorizer] func(string) (T, error)

// AuthorizerFromInterface this generic function provides Authorizer unmarshalling for all providers
func AuthorizerFromInterface[T Authorizer](f any, authSelectFn AuthorizerSelectorFn[T]) (T, error) {
	var emptyAuth T
	if f == nil {
		return emptyAuth, nil
	}
	fmap, ok := f.(map[string]interface{})
	if !ok {
		return emptyAuth, fmt.Errorf("AuthorizerFromInterface: could not cast interface as map")
	}

	authType, err := GetInterfaceValue[string](fmap, AuthorizerTypeProperty)
	if err != nil {
		return emptyAuth, fmt.Errorf("AuthorizerFromInterface: could not retrieve type property: %w", err)
	}
	authorizer, err := authSelectFn(authType)
	if err != nil {
		return emptyAuth, fmt.Errorf("AuthorizerFromInterface: %w", err)
	}

	// convert the interface back to a []Byte so that it can be unmarshalled into the correct type
	fBin, err := json.Marshal(f)
	if err != nil {
		return emptyAuth, fmt.Errorf("AuthorizerFromInterface: could not marshal value %v: %w", f, err)
	}

	err = json.Unmarshal(fBin, authorizer)
	if err != nil {
		return emptyAuth, fmt.Errorf("AuthorizerFromInterface: failed to unmarshal into Authorizer type %T from value %v: %w", authorizer, f, err)
	}
	return authorizer, nil
}
