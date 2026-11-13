using System.Buffers;
using System.Diagnostics;

namespace PureGate.WebSocket.Protocol;

// Writes frames with their length prefixes (docs/PROTOCOL.md, Messages and frames), each in one piece: the
// length is computed first, so nothing is moved afterwards. The caller makes every frame well-formed: Flags say
// which optional fields are set, and names, traces and codes follow the protocol's rules. Only the reader
// validates.
static class FrameWriter
{
  // The bytes Write takes for frame, its length prefix included.
  public static int GetLength(scoped in Frame frame)
  {
    var bodyLength = GetBodyLength(in frame);
    return Varint.GetLength((uint)bodyLength) + bodyLength;
  }

  public static void Write(IBufferWriter<byte> output, scoped in Frame frame)
  {
    var bodyLength = GetBodyLength(in frame);
    var length = Varint.GetLength((uint)bodyLength) + bodyLength;
    var span = output.GetSpan(length);

    var offset = Varint.Write(span, (uint)bodyLength);
    span[offset++] = (byte)frame.Kind;
    if (FrameLayout.HasFlags(frame.Kind))
      span[offset++] = (byte)frame.Flags;

    switch (frame.Kind)
    {
      case FrameKind.Request:
        offset = WriteVarint(span, offset, frame.Id);
        offset = WriteName(span, offset, in frame);
        if (frame.Has(FrameFlags.HasTimeout))
          offset = WriteVarint(span, offset, frame.Timeout);
        if (frame.Has(FrameFlags.HasTrace))
          offset = WriteString(span, offset, frame.Trace);
        break;

      case FrameKind.Response:
      case FrameKind.StreamEnd:
        offset = WriteVarint(span, offset, frame.Id);
        if (frame.Has(FrameFlags.HasError))
          offset = WriteError(span, offset, in frame);
        break;

      case FrameKind.Event:
        offset = WriteName(span, offset, in frame);
        if (frame.Has(FrameFlags.HasTrace))
          offset = WriteString(span, offset, frame.Trace);
        break;

      case FrameKind.StreamOpen:
        offset = WriteVarint(span, offset, frame.Id);
        offset = WriteName(span, offset, in frame);
        offset = WriteVarint(span, offset, frame.Credit);
        if (frame.Has(FrameFlags.HasTrace))
          offset = WriteString(span, offset, frame.Trace);
        break;

      case FrameKind.StreamItem:
      case FrameKind.Cancel:
        offset = WriteVarint(span, offset, frame.Id);
        break;

      case FrameKind.Credit:
        offset = WriteVarint(span, offset, frame.Id);
        offset = WriteVarint(span, offset, frame.Amount);
        break;

      case FrameKind.Ack:
        offset = WriteVarint(span, offset, frame.Count);
        break;
    }

    if (FrameLayout.HasPayload(frame.Kind))
    {
      frame.Payload.CopyTo(span[offset..]);
      offset += frame.Payload.Length;
    }

    Debug.Assert(offset == length, "The frame's length was computed wrong.");
    output.Advance(length);
  }

  // Bytes after the length prefix: the kind, the fields and the payload.
  static int GetBodyLength(scoped in Frame frame)
  {
    var length = FrameLayout.HasFlags(frame.Kind) ? 2 : 1;
    switch (frame.Kind)
    {
      case FrameKind.Request:
        length += Varint.GetLength(frame.Id) + GetNameLength(in frame);
        if (frame.Has(FrameFlags.HasTimeout))
          length += Varint.GetLength(frame.Timeout);
        if (frame.Has(FrameFlags.HasTrace))
          length += GetStringLength(frame.Trace);
        break;

      case FrameKind.Response:
      case FrameKind.StreamEnd:
        length += Varint.GetLength(frame.Id);
        if (frame.Has(FrameFlags.HasError))
          length += GetErrorLength(in frame);
        break;

      case FrameKind.Event:
        length += GetNameLength(in frame);
        if (frame.Has(FrameFlags.HasTrace))
          length += GetStringLength(frame.Trace);
        break;

      case FrameKind.StreamOpen:
        length += Varint.GetLength(frame.Id) + GetNameLength(in frame) + Varint.GetLength(frame.Credit);
        if (frame.Has(FrameFlags.HasTrace))
          length += GetStringLength(frame.Trace);
        break;

      case FrameKind.StreamItem:
      case FrameKind.Cancel:
        length += Varint.GetLength(frame.Id);
        break;

      case FrameKind.Credit:
        length += Varint.GetLength(frame.Id) + Varint.GetLength(frame.Amount);
        break;

      case FrameKind.Ack:
        length += Varint.GetLength(frame.Count);
        break;
    }

    if (FrameLayout.HasPayload(frame.Kind))
      length += frame.Payload.Length;

    return length;
  }

  static int GetStringLength(ReadOnlySpan<byte> value) => Varint.GetLength((uint)value.Length) + value.Length;

  static int GetNameLength(scoped in Frame frame) =>
    Varint.GetLength(frame.NameCode) + (FrameLayout.NameHasText(frame.NameCode) ? GetStringLength(frame.NameText) : 0);

  static int GetErrorLength(scoped in Frame frame) =>
    Varint.GetLength((uint)frame.ErrorCode)
    + (frame.ErrorCode == ErrorCode.Custom ? GetStringLength(frame.ErrorCustom) : 0)
    + GetStringLength(frame.ErrorMessage);

  static int WriteVarint(Span<byte> span, int offset, uint value) => offset + Varint.Write(span[offset..], value);

  static int WriteString(Span<byte> span, int offset, ReadOnlySpan<byte> value)
  {
    offset = WriteVarint(span, offset, (uint)value.Length);
    value.CopyTo(span[offset..]);
    return offset + value.Length;
  }

  static int WriteName(Span<byte> span, int offset, scoped in Frame frame)
  {
    offset = WriteVarint(span, offset, frame.NameCode);
    return FrameLayout.NameHasText(frame.NameCode) ? WriteString(span, offset, frame.NameText) : offset;
  }

  static int WriteError(Span<byte> span, int offset, scoped in Frame frame)
  {
    offset = WriteVarint(span, offset, (uint)frame.ErrorCode);
    if (frame.ErrorCode == ErrorCode.Custom)
      offset = WriteString(span, offset, frame.ErrorCustom);

    return WriteString(span, offset, frame.ErrorMessage);
  }
}
