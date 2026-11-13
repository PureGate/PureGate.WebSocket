namespace PureGate.WebSocket.Protocol;

// The two ends of a connection. Some frame kinds may only be sent by one of them.
enum Side : byte
{
  Client,
  Server,
}
