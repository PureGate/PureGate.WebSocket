namespace PureGate.WebSocket.Protocol;

// Unsigned LEB128 below 2^32 (docs/PROTOCOL.md, Encoding primitives): 7 bits per byte, least significant group
// first, at most 5 bytes. Writes use the shortest form.
static class Varint
{
  public const int MaxLength = 5;

  public static int GetLength(uint value) =>
    value < 1u << 7 ? 1
    : value < 1u << 14 ? 2
    : value < 1u << 21 ? 3
    : value < 1u << 28 ? 4
    : 5;

  // Writes value at the start of destination and returns the bytes written.
  public static int Write(Span<byte> destination, uint value)
  {
    var length = 0;
    while (value >= 0x80)
    {
      destination[length++] = (byte)(value | 0x80);
      value >>= 7;
    }

    destination[length++] = (byte)value;
    return length;
  }

  // Reads the varint at offset in source, and moves offset past it.
  public static uint Read(ReadOnlySpan<byte> source, ref int offset)
  {
    uint value = 0;
    for (var shift = 0; shift < 28; shift += 7)
    {
      if ((uint)offset >= (uint)source.Length)
        ProtocolException.Throw(ProtocolViolation.Truncated);

      uint b = source[offset++];
      value |= (b & 0x7F) << shift;
      if (b < 0x80)
        return value;
    }

    // The fifth byte holds bits 28 to 31: only its low 4 bits can be set, and it must be the last.
    if ((uint)offset >= (uint)source.Length)
      ProtocolException.Throw(ProtocolViolation.Truncated);

    uint last = source[offset++];
    if (last >= 0x80)
      ProtocolException.Throw(ProtocolViolation.VarintTooLong);
    if (last > 0x0F)
      ProtocolException.Throw(ProtocolViolation.VarintOverflow);

    return value | (last << 28);
  }
}
