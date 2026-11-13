namespace PureGate.WebSocket.Protocol;

// Which fields each frame kind carries (docs/PROTOCOL.md, Frame kinds). The reader, the writer and the tests
// all decide layouts here, so they cannot disagree.
static class FrameLayout
{
  public const FrameKind LastKind = FrameKind.GoAway;

  // Flag bits no version 1 frame may set.
  public const byte ReservedFlags = 0b1110_0000;

  public static bool HasFlags(FrameKind kind) =>
    kind is FrameKind.Request
      or FrameKind.Response
      or FrameKind.Event
      or FrameKind.StreamOpen
      or FrameKind.StreamItem
      or FrameKind.StreamEnd;

  public static FrameFlags AllowedFlags(FrameKind kind) => kind switch
  {
    FrameKind.Request => FrameFlags.Binary | FrameFlags.HasTimeout | FrameFlags.HasTrace,
    FrameKind.Response => FrameFlags.Binary | FrameFlags.HasError,
    FrameKind.Event => FrameFlags.Binary | FrameFlags.HasTrace,
    FrameKind.StreamOpen => FrameFlags.Binary | FrameFlags.HasTrace | FrameFlags.Upload,
    FrameKind.StreamItem => FrameFlags.Binary,
    FrameKind.StreamEnd => FrameFlags.Binary | FrameFlags.HasError,
    _ => FrameFlags.None,
  };

  public static bool CanSend(FrameKind kind, Side from) => kind switch
  {
    FrameKind.Hello or FrameKind.StreamOpen or FrameKind.Authenticate => from == Side.Client,
    FrameKind.Welcome or FrameKind.Authenticated or FrameKind.GoAway => from == Side.Server,
    _ => true,
  };

  public static bool HasId(FrameKind kind) =>
    kind is FrameKind.Request
      or FrameKind.Response
      or FrameKind.StreamOpen
      or FrameKind.StreamItem
      or FrameKind.StreamEnd
      or FrameKind.Credit
      or FrameKind.Cancel;

  public static bool HasName(FrameKind kind) =>
    kind is FrameKind.Request or FrameKind.Event or FrameKind.StreamOpen;

  // Kinds whose layout ends in a payload. The others end at their last field.
  public static bool HasPayload(FrameKind kind) =>
    kind is FrameKind.Hello
      or FrameKind.Welcome
      or FrameKind.Request
      or FrameKind.Response
      or FrameKind.Event
      or FrameKind.StreamOpen
      or FrameKind.StreamItem
      or FrameKind.StreamEnd
      or FrameKind.Authenticate
      or FrameKind.Authenticated;

  // Frames counted for acknowledgement and resume (docs/PROTOCOL.md, Acknowledgement and resume).
  public static bool IsSequenced(FrameKind kind) => kind is >= FrameKind.Request and <= FrameKind.Cancel;

  // Name codes 0 (literal) and 2n + 1 (definition) are followed by the name's text; 2n refers to it.
  public static bool NameHasText(uint code) => code == 0 || (code & 1) == 1;
}
