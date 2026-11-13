namespace PureGate.WebSocket.Protocol;

// Why a peer's bytes broke the protocol. test-vectors/frames.json names these in snake_case, and the TypeScript
// client reports the same ones, so both implementations reject malformed input the same way.
enum ProtocolViolation
{
  Truncated,
  VarintTooLong,
  VarintOverflow,
  EmptyMessage,
  EmptyFrame,
  FrameOverrun,
  ReservedKind,
  WrongDirection,
  ReservedFlag,
  InvalidFlag,
  InvalidId,
  InvalidName,
  InvalidString,
  InvalidTrace,
  InvalidErrorCode,
  InvalidCustomCode,
  InvalidCredit,
  TrailingBytes,
  NameOutOfOrder,
  NameUndefined,
  NameTableFull,
}
