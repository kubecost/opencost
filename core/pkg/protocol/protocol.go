package protocol

////////////////////////////////////////////////////////////////////////////////
//
//  The purpose of this package is to provide a general set of utilities for
//  writing responses in networked communication. Since go often uses the basic
//  protocol names (ie: "net/http") for their packages, keeping protocol utilities
//  in their own packages can be a bit annoying with respect to building an API.
//  To provide a "static" set of utilities, we can utilize method selectors on
//  structs allowing callers to use proto.<protocol>() to access the utility methods
//  with package-like syntax. We can also expand on the supported protocols as needed.
//
////////////////////////////////////////////////////////////////////////////////

var (
	httpProtocol HTTPProtocol
)

// HTTP returns the HTTPProtocol utilities.
func HTTP() HTTPProtocol {
	return httpProtocol
}
