namespace PureGate.WebSocket.Protocol;

// The error codes of docs/PROTOCOL.md, as sent in an error field. Custom means an application code follows
// as text.
enum ErrorCode : byte
{
  Custom = 0,
  Timeout = 1,
  Cancelled = 2,
  Unauthorized = 3,
  Forbidden = 4,
  UnknownOperation = 5,
  InvalidPayload = 6,
  PayloadTooLarge = 7,
  Overloaded = 8,
  HandlerFailed = 9,
  ConnectionClosed = 10,
  ProtocolError = 11,
}

static class ErrorCodes
{
  public const ErrorCode Last = ErrorCode.ProtocolError;
  public const int MaxCustomLength = 64;

  // The code's name as applications see it, such as "timeout".
  public static string GetName(ErrorCode code) => code switch
  {
    ErrorCode.Timeout => "timeout",
    ErrorCode.Cancelled => "cancelled",
    ErrorCode.Unauthorized => "unauthorized",
    ErrorCode.Forbidden => "forbidden",
    ErrorCode.UnknownOperation => "unknown_operation",
    ErrorCode.InvalidPayload => "invalid_payload",
    ErrorCode.PayloadTooLarge => "payload_too_large",
    ErrorCode.Overloaded => "overloaded",
    ErrorCode.HandlerFailed => "handler_failed",
    ErrorCode.ConnectionClosed => "connection_closed",
    ErrorCode.ProtocolError => "protocol_error",
    _ => throw new ArgumentOutOfRangeException(nameof(code), code, "Custom codes have no fixed name."),
  };

  // The library code with this name. False for anything else, which travels as a custom code.
  public static bool TryGetCode(string name, out ErrorCode code)
  {
    code = name switch
    {
      "timeout" => ErrorCode.Timeout,
      "cancelled" => ErrorCode.Cancelled,
      "unauthorized" => ErrorCode.Unauthorized,
      "forbidden" => ErrorCode.Forbidden,
      "unknown_operation" => ErrorCode.UnknownOperation,
      "invalid_payload" => ErrorCode.InvalidPayload,
      "payload_too_large" => ErrorCode.PayloadTooLarge,
      "overloaded" => ErrorCode.Overloaded,
      "handler_failed" => ErrorCode.HandlerFailed,
      "connection_closed" => ErrorCode.ConnectionClosed,
      "protocol_error" => ErrorCode.ProtocolError,
      _ => ErrorCode.Custom,
    };
    return code != ErrorCode.Custom;
  }

  // An application code: 1 to 64 bytes of [a-z0-9_].
  public static bool IsValidCustom(ReadOnlySpan<byte> code)
  {
    if (code.IsEmpty || code.Length > MaxCustomLength)
      return false;

    foreach (var b in code)
    {
      if (!(b is >= (byte)'a' and <= (byte)'z' or >= (byte)'0' and <= (byte)'9' or (byte)'_'))
        return false;
    }

    return true;
  }
}
