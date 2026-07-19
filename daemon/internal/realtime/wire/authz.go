package wire

import "fmt"

// Role is the realtime-protocol role of a connecting device (SPEC.md
// section 9.2) — coarser than and distinct from the HTTP-layer roles used
// for pairing/REST.
type Role string

const (
	RoleSource Role = "source"
	RoleViewer Role = "viewer"
)

// Authorize checks a client-originated envelope against the section 10.1
// authorization matrix, plus the two rules that apply regardless of the
// declared role:
//   - stage:"accept" belongs exclusively to the server (section 9.1); any
//     client-sent accept is a handshake violation, answered with
//     error{code: hello_required} rather than the ordinary
//     error{code: unauthorized} the matrix would otherwise suggest.
//   - command{origin: "server"} can never be sent by a client, regardless
//     of the sender's role or the action named (section 8).
//
// body must be the already-decoded, already-Validate()'d body value
// returned by DecodeBody for the same envelope. Authorize does not itself
// re-run schema validation.
func Authorize(role Role, envType string, body any) error {
	switch envType {
	case TypeHello:
		hb, ok := body.(HelloBody)
		if !ok {
			return fmt.Errorf("authorize: expected HelloBody, got %T", body)
		}
		if hb.Accept != nil {
			return fmt.Errorf("%s: stage \"accept\" may only be sent by the server", ErrHelloRequired)
		}
		// stage "offer" is permitted from both source and viewer.
		return nil
	case TypeResume, TypeSubscribe, TypeUnsubscribe, TypeInterestRenew:
		if role != RoleViewer {
			return fmt.Errorf("%s: %q may only be sent by a viewer", ErrUnauthorized, envType)
		}
		return nil
	case TypeSnapshot, TypeDelta, TypeConfigEvent:
		return fmt.Errorf("%s: %q is server-only, a client MUST NOT send it", ErrUnauthorized, envType)
	case TypeTaskEvent, TypeMessageEvent, TypeMetricFrame:
		if role != RoleSource {
			return fmt.Errorf("%s: %q may only be sent by a source", ErrUnauthorized, envType)
		}
		return nil
	case TypeAck:
		if role != RoleSource {
			return fmt.Errorf("%s: only a source may send ack (optional command-delivery ack)", ErrUnauthorized)
		}
		return nil
	case TypeCommand:
		cb, ok := body.(CommandBody)
		if !ok {
			return fmt.Errorf("authorize: expected CommandBody, got %T", body)
		}
		// This rule applies regardless of role: a client can never
		// originate a server-side command, whatever action it names.
		if cb.Origin == "server" {
			return fmt.Errorf("%s: a client may never send command with origin \"server\"", ErrUnauthorized)
		}
		if role != RoleViewer {
			return fmt.Errorf("%s: command may only be sent by a viewer (with origin viewer)", ErrUnauthorized)
		}
		return nil
	case TypeError:
		return nil // either role may send error
	default:
		// Unrecognized type: SPEC.md section 15 says ignore, not reject.
		return nil
	}
}
