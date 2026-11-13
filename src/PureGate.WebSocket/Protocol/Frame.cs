namespace PureGate.WebSocket.Protocol;

// One frame, as read from a message or about to be written to one. Which fields mean anything depends on Kind
// and Flags (see FrameLayout). The spans point into the message, or into the caller's buffers when writing,
// and are valid only as long as those are.
ref struct Frame
{
  public FrameKind Kind;
  public FrameFlags Flags;
  public uint Id;

  // The name field: a code, and the name's text when the code carries it (FrameLayout.NameHasText).
  public uint NameCode;
  public ReadOnlySpan<byte> NameText;

  // Milliseconds the caller still waits. Set when Flags has HasTimeout.
  public uint Timeout;

  // A W3C traceparent. Set when Flags has HasTrace.
  public ReadOnlySpan<byte> Trace;

  // StreamOpen: the client's initial credit, in items. 0 for an upload.
  public uint Credit;

  // Credit: how many items, or events when Id is 0, the receiver grants.
  public uint Amount;

  // Ack: how many sequenced frames the sender has processed.
  public uint Count;

  // The error field. Set when Flags has HasError; ErrorCustom only when ErrorCode is Custom.
  public ErrorCode ErrorCode;
  public ReadOnlySpan<byte> ErrorCustom;
  public ReadOnlySpan<byte> ErrorMessage;

  // Everything after the fields, for the kinds that have a payload.
  public ReadOnlySpan<byte> Payload;

  public readonly bool Has(FrameFlags flag) => (Flags & flag) != 0;
}
