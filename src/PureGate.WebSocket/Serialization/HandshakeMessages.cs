using System.Text.Json;
using System.Text.Json.Serialization;

namespace PureGate.WebSocket.Serialization;

// The JSON payloads of the handshake (docs/PROTOCOL.md, Handshake). Source-generated, so they work under Native AOT.

sealed class HelloMessage
{
  public string Token { get; set; }
  public string Contract { get; set; }

  // The payload format the browser writes and reads, or null for JSON.
  public string Format { get; set; }

  public ResumeRequest Resume { get; set; }
  public int Calls { get; set; }
  public int MaxMessage { get; set; }
}

sealed class ResumeRequest
{
  public string Session { get; set; }
  public long Received { get; set; }
}

sealed class WelcomeMessage
{
  public string Connection { get; set; }
  public string Session { get; set; }
  public bool Resumed { get; set; }
  public long Received { get; set; }
  public int Calls { get; set; }
  public int Streams { get; set; }
  public int Events { get; set; }
  public int MaxMessage { get; set; }
  public long Heartbeat { get; set; }
  public long ResumeWindow { get; set; }
  public string Contract { get; set; }

  // The format Hello asked for, which the server has; null for JSON.
  public string Format { get; set; }

  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
  public long Expires { get; set; }

  public string Route { get; set; }
}

// A refreshed token (docs/PROTOCOL.md, Re-authentication).
sealed class AuthenticateMessage
{
  public string Token { get; set; }
}

sealed class AuthenticatedMessage
{
  [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
  public long Expires { get; set; }
}

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(HelloMessage))]
[JsonSerializable(typeof(WelcomeMessage))]
[JsonSerializable(typeof(AuthenticateMessage))]
[JsonSerializable(typeof(AuthenticatedMessage))]
[JsonSerializable(typeof(string))]
partial class HandshakeJson : JsonSerializerContext;
