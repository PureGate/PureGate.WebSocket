namespace PureGate.WebSocket.Protocol;

// The frame kinds of docs/PROTOCOL.md. Kinds 0 and 17–255 are reserved.
enum FrameKind : byte
{
  Hello = 1,
  Welcome = 2,
  Request = 3,
  Response = 4,
  Event = 5,
  StreamOpen = 6,
  StreamItem = 7,
  StreamEnd = 8,
  Credit = 9,
  Cancel = 10,
  Ack = 11,
  Ping = 12,
  Pong = 13,
  Authenticate = 14,
  Authenticated = 15,
  GoAway = 16,
}
