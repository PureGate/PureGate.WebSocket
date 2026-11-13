namespace PureGate.WebSocket.Protocol;

// The flags byte of docs/PROTOCOL.md. Bits 5–7 are reserved.
[Flags]
enum FrameFlags : byte
{
  None = 0,

  // The payload is raw bytes, not JSON.
  Binary = 1 << 0,

  // A timeout field follows the name. Request only.
  HasTimeout = 1 << 1,

  // An error field follows the id: the exchange failed. Response and StreamEnd only.
  HasError = 1 << 2,

  // A trace field (a W3C traceparent) follows. Request, Event and StreamOpen only.
  HasTrace = 1 << 3,

  // The client produces this stream's items. StreamOpen only.
  Upload = 1 << 4,
}
