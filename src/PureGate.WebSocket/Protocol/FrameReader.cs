using System.Text.Unicode;

namespace PureGate.WebSocket.Protocol;

// Reads the frames of one message (docs/PROTOCOL.md, Messages and frames) and validates every field, so the
// layers above only ever see well-formed frames. Malformed input throws ProtocolException, after the frames
// before it have been read.
ref struct FrameReader
{
  const int MaxTraceLength = 256;

  readonly ReadOnlySpan<byte> Message;
  readonly Side From;
  int Offset;

  // from is the side that sent the message.
  public FrameReader(ReadOnlySpan<byte> message, Side from)
  {
    if (message.IsEmpty)
      ProtocolException.Throw(ProtocolViolation.EmptyMessage);

    Message = message;
    From = from;
  }

  public bool TryRead(out Frame frame)
  {
    if (Offset == Message.Length)
    {
      frame = default;
      return false;
    }

    var length = Varint.Read(Message, ref Offset);
    if (length == 0)
      ProtocolException.Throw(ProtocolViolation.EmptyFrame);
    if (length > (uint)(Message.Length - Offset))
      ProtocolException.Throw(ProtocolViolation.FrameOverrun);

    frame = Decode(Message.Slice(Offset, (int)length), From);
    Offset += (int)length;
    return true;
  }

  static Frame Decode(ReadOnlySpan<byte> body, Side from)
  {
    var kind = (FrameKind)body[0];
    if (kind == 0 || kind > FrameLayout.LastKind)
      ProtocolException.Throw(ProtocolViolation.ReservedKind);
    if (!FrameLayout.CanSend(kind, from))
      ProtocolException.Throw(ProtocolViolation.WrongDirection);

    var frame = new Frame { Kind = kind };
    var offset = 1;
    if (FrameLayout.HasFlags(kind))
      frame.Flags = ReadFlags(body, ref offset, kind);

    switch (kind)
    {
      case FrameKind.Request:
        frame.Id = ReadId(body, ref offset);
        frame.NameText = ReadName(body, ref offset, out frame.NameCode);
        if (frame.Has(FrameFlags.HasTimeout))
          frame.Timeout = Varint.Read(body, ref offset);
        if (frame.Has(FrameFlags.HasTrace))
          frame.Trace = ReadTrace(body, ref offset);
        break;

      case FrameKind.Response:
      case FrameKind.StreamEnd:
        frame.Id = ReadId(body, ref offset);
        if (frame.Has(FrameFlags.HasError))
          frame.ErrorMessage = ReadError(body, ref offset, out frame.ErrorCode, out frame.ErrorCustom);
        break;

      case FrameKind.Event:
        frame.NameText = ReadName(body, ref offset, out frame.NameCode);
        if (frame.Has(FrameFlags.HasTrace))
          frame.Trace = ReadTrace(body, ref offset);
        break;

      case FrameKind.StreamOpen:
        frame.Id = ReadId(body, ref offset);
        frame.NameText = ReadName(body, ref offset, out frame.NameCode);
        frame.Credit = Varint.Read(body, ref offset);
        // A download needs credit to start with; an upload starts with none, and the server grants it.
        if (frame.Has(FrameFlags.Upload) ? frame.Credit != 0 : frame.Credit == 0)
          ProtocolException.Throw(ProtocolViolation.InvalidCredit);
        if (frame.Has(FrameFlags.HasTrace))
          frame.Trace = ReadTrace(body, ref offset);
        break;

      case FrameKind.StreamItem:
      case FrameKind.Cancel:
        frame.Id = ReadId(body, ref offset);
        break;

      case FrameKind.Credit:
        // Id 0 is the event window, not a stream.
        frame.Id = Varint.Read(body, ref offset);
        frame.Amount = Varint.Read(body, ref offset);
        if (frame.Amount == 0)
          ProtocolException.Throw(ProtocolViolation.InvalidCredit);
        break;

      case FrameKind.Ack:
        frame.Count = Varint.Read(body, ref offset);
        break;
    }

    if (FrameLayout.HasPayload(kind))
      frame.Payload = body[offset..];
    else if (offset != body.Length)
      ProtocolException.Throw(ProtocolViolation.TrailingBytes);

    return frame;
  }

  static FrameFlags ReadFlags(ReadOnlySpan<byte> body, scoped ref int offset, FrameKind kind)
  {
    if ((uint)offset >= (uint)body.Length)
      ProtocolException.Throw(ProtocolViolation.Truncated);

    var flags = body[offset++];
    if ((flags & FrameLayout.ReservedFlags) != 0)
      ProtocolException.Throw(ProtocolViolation.ReservedFlag);
    if ((flags & ~(int)FrameLayout.AllowedFlags(kind)) != 0)
      ProtocolException.Throw(ProtocolViolation.InvalidFlag);

    return (FrameFlags)flags;
  }

  static uint ReadId(ReadOnlySpan<byte> body, scoped ref int offset)
  {
    var id = Varint.Read(body, ref offset);
    if (id == 0)
      ProtocolException.Throw(ProtocolViolation.InvalidId);

    return id;
  }

  static ReadOnlySpan<byte> ReadString(ReadOnlySpan<byte> body, scoped ref int offset)
  {
    var length = Varint.Read(body, ref offset);
    if (length > (uint)(body.Length - offset))
      ProtocolException.Throw(ProtocolViolation.Truncated);

    var value = body.Slice(offset, (int)length);
    offset += (int)length;
    return value;
  }

  static ReadOnlySpan<byte> ReadName(ReadOnlySpan<byte> body, scoped ref int offset, out uint code)
  {
    code = Varint.Read(body, ref offset);
    if (!FrameLayout.NameHasText(code))
      return default;

    var text = ReadString(body, ref offset);
    if (!NameRules.IsValid(text))
      ProtocolException.Throw(ProtocolViolation.InvalidName);

    return text;
  }

  static ReadOnlySpan<byte> ReadTrace(ReadOnlySpan<byte> body, scoped ref int offset)
  {
    var trace = ReadString(body, ref offset);
    if (trace.IsEmpty || trace.Length > MaxTraceLength || trace.ContainsAnyExceptInRange((byte)0x21, (byte)0x7E))
      ProtocolException.Throw(ProtocolViolation.InvalidTrace);

    return trace;
  }

  // Returns the error's message; code and custom receive the rest of the field.
  static ReadOnlySpan<byte> ReadError(
      ReadOnlySpan<byte> body,
      scoped ref int offset,
      out ErrorCode code,
      out ReadOnlySpan<byte> custom)
  {
    var value = Varint.Read(body, ref offset);
    if (value > (uint)ErrorCodes.Last)
      ProtocolException.Throw(ProtocolViolation.InvalidErrorCode);

    code = (ErrorCode)value;
    custom = default;
    if (code == ErrorCode.Custom)
    {
      custom = ReadString(body, ref offset);
      if (!ErrorCodes.IsValidCustom(custom))
        ProtocolException.Throw(ProtocolViolation.InvalidCustomCode);
    }

    var message = ReadString(body, ref offset);
    if (!Utf8.IsValid(message))
      ProtocolException.Throw(ProtocolViolation.InvalidString);

    return message;
  }
}
