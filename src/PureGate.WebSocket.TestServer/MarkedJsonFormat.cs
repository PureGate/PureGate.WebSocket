using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace PureGate.WebSocket.TestServer;

// The tests' payload format: JSON after a marker byte, so a test can tell which format a payload travelled in. The
// TypeScript tests have the same one (test/formats.ts).
public sealed class MarkedJsonFormat : IPayloadFormat
{
  public const string FormatName = "marked-json";

  // Never the first byte of UTF-8 JSON.
  public const byte Marker = 0xFE;

  public string Name => FormatName;

  public void Write<T>(IBufferWriter<byte> output, T value)
  {
    output.GetSpan(1)[0] = Marker;
    output.Advance(1);
    using var writer = new Utf8JsonWriter(output);
    JsonSerializer.Serialize(writer, value, TypeInfo<T>());
  }

  public T Read<T>(ReadOnlyMemory<byte> payload) =>
    payload.Span[0] == Marker
      ? JsonSerializer.Deserialize(payload.Span[1..], TypeInfo<T>())
      : throw new FormatException("The payload has no marker.");

  static JsonTypeInfo<T> TypeInfo<T>() =>
    TestJson.Default.GetTypeInfo(typeof(T)) as JsonTypeInfo<T> ?? throw new NotSupportedException($"TestJson has no {typeof(T).Name}.");
}
