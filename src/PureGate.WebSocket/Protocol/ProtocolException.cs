using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace PureGate.WebSocket.Protocol;

// Thrown when a peer breaks the protocol. The connection then closes with 4000 (protocol_error).
sealed class ProtocolException(ProtocolViolation violation) : Exception(Describe(violation))
{
  public ProtocolViolation Violation { get; } = violation;

  // Kept out of line, so the checks that call it stay small enough to inline.
  [DoesNotReturn]
  [MethodImpl(MethodImplOptions.NoInlining)]
  public static void Throw(ProtocolViolation violation) => throw new ProtocolException(violation);

  static string Describe(ProtocolViolation violation) => violation switch
  {
    ProtocolViolation.Truncated => "A field runs past the end of its frame.",
    ProtocolViolation.VarintTooLong => "A varint is longer than 5 bytes.",
    ProtocolViolation.VarintOverflow => "A varint is 2^32 or more.",
    ProtocolViolation.EmptyMessage => "A message has no frames.",
    ProtocolViolation.EmptyFrame => "A frame's length is 0.",
    ProtocolViolation.FrameOverrun => "A frame is longer than the rest of its message.",
    ProtocolViolation.ReservedKind => "A frame has a reserved kind.",
    ProtocolViolation.WrongDirection => "A frame kind was sent by the side that may not send it.",
    ProtocolViolation.ReservedFlag => "A frame sets a reserved flag.",
    ProtocolViolation.InvalidFlag => "A frame sets a flag its kind does not allow.",
    ProtocolViolation.InvalidId => "A frame has the id 0.",
    ProtocolViolation.InvalidName => "A name is not 1 to 256 bytes of dot-separated identifiers.",
    ProtocolViolation.InvalidString => "A string is not valid UTF-8.",
    ProtocolViolation.InvalidTrace => "A trace is not 1 to 256 bytes of printable ASCII.",
    ProtocolViolation.InvalidErrorCode => "An error code is above 11.",
    ProtocolViolation.InvalidCustomCode => "A custom error code is not 1 to 64 bytes of [a-z0-9_].",
    ProtocolViolation.InvalidCredit => "A credit is 0 where it must be positive, or set on an upload.",
    ProtocolViolation.TrailingBytes => "A frame without a payload has bytes after its last field.",
    ProtocolViolation.NameOutOfOrder => "A name definition does not take the next number.",
    ProtocolViolation.NameUndefined => "A name reference is to a number not yet defined.",
    ProtocolViolation.NameTableFull => "A name definition is past the table's limit.",
    _ => "The peer broke the protocol.",
  };
}
