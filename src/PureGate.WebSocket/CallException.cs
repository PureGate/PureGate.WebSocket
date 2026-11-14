using System.Text;
using System.Text.Json;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket;

/// <summary>
/// A call that failed with an error code. A handler throws it to send the browser its own code, such as
/// "not_found", with optional details. InvokeAsync throws it when the browser's handler failed, or the call
/// timed out or could not be sent.
/// </summary>
public sealed class CallException : Exception
{
  /// <summary>
  /// A failure with code, one of the protocol's code names or the application's own of 1 to 64 characters of a-z, 0-9
  /// and _, and message. details, when set, goes to the browser as JSON.
  /// </summary>
  public CallException(string code, string message, object details = null)
    : base(message)
  {
    ArgumentNullException.ThrowIfNull(code);
    if (!ErrorCodes.TryGetCode(code, out _) && !ErrorCodes.IsValidCustom(Encoding.UTF8.GetBytes(code)))
      throw new ArgumentException($"'{code}' is not an error code: use 1 to 64 characters of a-z, 0-9 and _.", nameof(code));

    Code = code;
    Details = details;
  }

  /// <summary>
  /// The protocol's code name, such as "timeout", or the application's own code.
  /// </summary>
  public string Code { get; }

  /// <summary>
  /// Thrown: anything the endpoint's serializer options can write as JSON. Received from a browser: a
  /// JsonElement, or null when there were none.
  /// </summary>
  public object Details { get; }

  // The error of a failed Response or StreamEnd.
  internal static CallException FromFrame(scoped in Frame frame)
  {
    var code = frame.ErrorCode == ErrorCode.Custom
      ? Encoding.ASCII.GetString(frame.ErrorCustom)
      : ErrorCodes.GetName(frame.ErrorCode);
    object details = null;
    if (!frame.Payload.IsEmpty)
    {
      try
      {
        var reader = new Utf8JsonReader(frame.Payload);
        details = JsonElement.ParseValue(ref reader);
      }
      catch (JsonException)
      {
      }
    }

    return new CallException(code, Encoding.UTF8.GetString(frame.ErrorMessage), details);
  }
}
