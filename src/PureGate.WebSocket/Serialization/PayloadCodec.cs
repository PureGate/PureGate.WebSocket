using System.Buffers;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace PureGate.WebSocket.Serialization;

// No value: the request of an operation that takes none, or the result of one that returns none.
readonly struct NoPayload;

// Thrown when a payload does not hold the operation's type.
sealed class InvalidPayloadException(string message, Exception inner = null) : Exception(message, inner);

// How values of one type travel as payloads (docs/PROTOCOL.md, Payload): byte[] and ReadOnlyMemory<byte> as raw
// bytes, NoPayload as nothing, and everything else in the connection's format: JSON through the endpoint's serializer
// options, or one of its PayloadFormats.
abstract class PayloadCodec<T>
{
  public abstract bool IsBinary { get; }

  // Reads a payload, copying what it keeps: the payload is part of the received message, whose buffer is reused.
  public abstract T Read(ReadOnlyMemory<byte> payload, bool binary);

  public abstract void Write(IBufferWriter<byte> output, T value);
}

static class PayloadCodecs
{
  // A codec for each of the endpoint's formats, by the index a connection has for its format: JSON first, then
  // formats in order. Raw bytes and NoPayload travel the same way in all of them.
  public static PayloadCodec<T>[] CreateSet<T>(JsonSerializerOptions options, IPayloadFormat[] formats)
  {
    var codecs = new PayloadCodec<T>[1 + formats.Length];
    codecs[0] = Create<T>(options);
    for (var i = 0; i < formats.Length; i++)
      codecs[i + 1] = codecs[0] is JsonCodec<T> ? new FormatCodec<T>(formats[i]) : codecs[0];

    return codecs;
  }

  static PayloadCodec<T> Create<T>(JsonSerializerOptions options)
  {
    if (typeof(T) == typeof(NoPayload))
      return (PayloadCodec<T>)(object)new NoPayloadCodec();
    if (typeof(T) == typeof(byte[]))
      return (PayloadCodec<T>)(object)new BytesCodec();
    if (typeof(T) == typeof(ReadOnlyMemory<byte>))
      return (PayloadCodec<T>)(object)new MemoryCodec();

    return new JsonCodec<T>((JsonTypeInfo<T>)options.GetTypeInfo(typeof(T)), options);
  }

  sealed class NoPayloadCodec : PayloadCodec<NoPayload>
  {
    public override bool IsBinary => false;

    public override NoPayload Read(ReadOnlyMemory<byte> payload, bool binary) =>
      payload.IsEmpty && !binary ? default : throw new InvalidPayloadException("This operation takes no payload.");

    public override void Write(IBufferWriter<byte> output, NoPayload value)
    {
    }
  }

  sealed class BytesCodec : PayloadCodec<byte[]>
  {
    public override bool IsBinary => true;

    public override byte[] Read(ReadOnlyMemory<byte> payload, bool binary) =>
      binary ? payload.ToArray() : throw new InvalidPayloadException("This operation takes bytes, not JSON.");

    public override void Write(IBufferWriter<byte> output, byte[] value)
    {
      if (value is not null)
        output.Write(value);
    }
  }

  sealed class MemoryCodec : PayloadCodec<ReadOnlyMemory<byte>>
  {
    public override bool IsBinary => true;

    public override ReadOnlyMemory<byte> Read(ReadOnlyMemory<byte> payload, bool binary) =>
      binary ? payload.ToArray() : throw new InvalidPayloadException("This operation takes bytes, not JSON.");

    public override void Write(IBufferWriter<byte> output, ReadOnlyMemory<byte> value) => output.Write(value.Span);
  }

  sealed class JsonCodec<T>(JsonTypeInfo<T> typeInfo, JsonSerializerOptions options) : PayloadCodec<T>
  {
    public override bool IsBinary => false;

    public override T Read(ReadOnlyMemory<byte> payload, bool binary)
    {
      if (binary)
        throw new InvalidPayloadException("This operation takes JSON, not bytes.");
      if (payload.IsEmpty)
        throw new InvalidPayloadException("This operation takes a payload.");

      try
      {
        return JsonSerializer.Deserialize(payload.Span, typeInfo);
      }
      catch (JsonException exception)
      {
        throw new InvalidPayloadException($"The payload is not a {typeof(T).Name}: {exception.Message}", exception);
      }
    }

    public override void Write(IBufferWriter<byte> output, T value)
    {
      var writer = JsonWriters.Rent(output, options.Encoder);
      try
      {
        JsonSerializer.Serialize(writer, value, typeInfo);
      }
      finally
      {
        JsonWriters.Return(writer);
      }
    }
  }

  // A type in one of the endpoint's own formats. Whatever the format throws while reading means the payload is not
  // a T.
  sealed class FormatCodec<T>(IPayloadFormat format) : PayloadCodec<T>
  {
    public override bool IsBinary => false;

    public override T Read(ReadOnlyMemory<byte> payload, bool binary)
    {
      if (binary)
        throw new InvalidPayloadException($"This operation takes {format.Name}, not bytes.");
      if (payload.IsEmpty)
        throw new InvalidPayloadException("This operation takes a payload.");

      try
      {
        return format.Read<T>(payload);
      }
      catch (Exception exception) when (exception is not InvalidPayloadException)
      {
        throw new InvalidPayloadException($"The payload is not a {typeof(T).Name}: {exception.Message}", exception);
      }
    }

    public override void Write(IBufferWriter<byte> output, T value) => format.Write(output, value);
  }
}

// One Utf8JsonWriter per thread, reset onto each output instead of created for it, as System.Text.Json keeps its own.
// A payload written from inside the serialization of another, by a converter, gets a writer of its own.
static class JsonWriters
{
  [ThreadStatic]
  static Utf8JsonWriter Cached;

  public static Utf8JsonWriter Rent(IBufferWriter<byte> output, JavaScriptEncoder encoder)
  {
    var writer = Cached;
    if (writer is null || writer.Options.Encoder != encoder)
      return new Utf8JsonWriter(output, new JsonWriterOptions { Encoder = encoder, SkipValidation = true });

    Cached = null;
    writer.Reset(output);
    return writer;
  }

  public static void Return(Utf8JsonWriter writer) => Cached = writer;
}
