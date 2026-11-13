using System.Buffers;
using System.Text;
using System.Text.Json;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test;

public sealed class FrameTests
{
  static IEnumerable<TestCaseData> ValidMessages() => Vectors.Cases("messages", "valid");

  static IEnumerable<TestCaseData> InvalidMessages() => Vectors.Cases("messages", "invalid");

  [TestCaseSource(nameof(ValidMessages))]
  public void ReadsValidMessages(JsonElement vector)
  {
    var expected = vector.GetProperty("frames").EnumerateArray().Select(json =>
    {
      var frame = Vectors.ToFrame(json);
      return Vectors.Render(in frame);
    }).ToList();

    Assert.That(Read(Vectors.Bytes(vector), Vectors.From(vector)), Is.EqualTo(expected));
  }

  [TestCaseSource(nameof(ValidMessages))]
  public void WritesValidMessages(JsonElement vector)
  {
    var output = new ArrayBufferWriter<byte>();
    foreach (var json in vector.GetProperty("frames").EnumerateArray())
    {
      var frame = Vectors.ToFrame(json);
      FrameWriter.Write(output, in frame);
    }

    Assert.That(Convert.ToHexStringLower(output.WrittenSpan), Is.EqualTo(Convert.ToHexStringLower(Vectors.Bytes(vector))));
  }

  [TestCaseSource(nameof(InvalidMessages))]
  public void RejectsInvalidMessages(JsonElement vector)
  {
    var bytes = Vectors.Bytes(vector);
    var from = Vectors.From(vector);

    var exception = Assert.Throws<ProtocolException>(() => Read(bytes, from));

    Assert.That(Vectors.Name(exception.Violation), Is.EqualTo(vector.GetProperty("violation").GetString()));
  }

  [Test]
  public void AcceptsNamesUpTo256Bytes()
  {
    var dotted = string.Join('.', Enumerable.Repeat("a", 128)) + "b";
    Assert.That(dotted, Has.Length.EqualTo(256));

    Assert.That(NameRules.IsValid(Encoding.ASCII.GetBytes(dotted)), Is.True, "128 segments in 256 bytes");
    Assert.That(NameRules.IsValid(Encoding.ASCII.GetBytes(new string('a', 256))), Is.True);
    Assert.That(NameRules.IsValid(Encoding.ASCII.GetBytes(new string('a', 257))), Is.False);
    Assert.That(NameRules.IsValid("Orders_2.get_All"u8), Is.True);
    Assert.That(NameRules.IsValid("_.__"u8), Is.True);
  }

  [Test]
  public void AcceptsTracesUpTo256Bytes()
  {
    Assert.That(Read(Event(trace: new string('~', 256)), Side.Client), Has.Count.EqualTo(1));

    var exception = Assert.Throws<ProtocolException>(() => Read(Event(trace: new string('~', 257)), Side.Client));
    Assert.That(exception.Violation, Is.EqualTo(ProtocolViolation.InvalidTrace));
  }

  [Test]
  public void AcceptsCustomCodesUpTo64Bytes()
  {
    Assert.That(ErrorCodes.IsValidCustom(Encoding.ASCII.GetBytes(new string('z', 64))), Is.True);
    Assert.That(ErrorCodes.IsValidCustom(Encoding.ASCII.GetBytes(new string('z', 65))), Is.False);
    Assert.That(ErrorCodes.IsValidCustom("not_found_2"u8), Is.True);
    Assert.That(ErrorCodes.IsValidCustom("not-found"u8), Is.False);
  }

  [Test]
  public void RoundTripsLargePayloads()
  {
    var payload = new byte[300_000];
    new Random(7).NextBytes(payload);
    var output = new ArrayBufferWriter<byte>();
    var frame = new Frame { Kind = FrameKind.StreamItem, Flags = FrameFlags.Binary, Id = 12, Payload = payload };

    FrameWriter.Write(output, in frame);
    var reader = new FrameReader(output.WrittenSpan, Side.Server);

    Assert.That(reader.TryRead(out var read), Is.True);
    Assert.That(read.Payload.SequenceEqual(payload), Is.True);
    Assert.That(reader.TryRead(out _), Is.False);
  }

  // Corrupted messages must be rejected with ProtocolException, never with another exception or a crash. The
  // seed makes every failure reproducible.
  [Test]
  public void RejectsCorruptedMessagesOnlyWithProtocolException()
  {
    var messages = Vectors.Get("messages", "valid").EnumerateArray()
      .Select(vector => (Bytes: Vectors.Bytes(vector), From: Vectors.From(vector)))
      .ToArray();
    var random = new Random(20261113);

    for (var i = 0; i < 20_000; i++)
    {
      var (original, from) = messages[random.Next(messages.Length)];
      var bytes = Corrupt(original, random);
      try
      {
        Read(bytes, from);
      }
      catch (ProtocolException)
      {
      }
      catch (Exception exception)
      {
        Assert.Fail($"{exception.GetType().Name} for [{Convert.ToHexStringLower(bytes)}]: {exception.Message}");
      }
    }
  }

  static byte[] Corrupt(byte[] original, Random random)
  {
    var bytes = new List<byte>(original);
    for (var edits = random.Next(1, 4); edits > 0; edits--)
    {
      var position = random.Next(bytes.Count + 1);
      switch (random.Next(4))
      {
        case 0 when position < bytes.Count:
          bytes[position] = (byte)random.Next(256);
          break;
        case 1 when position < bytes.Count:
          bytes.RemoveAt(position);
          break;
        case 2:
          bytes.Insert(position, (byte)random.Next(256));
          break;
        default:
          bytes.RemoveRange(position, bytes.Count - position);
          break;
      }
    }

    return [.. bytes];
  }

  static byte[] Event(string trace)
  {
    var output = new ArrayBufferWriter<byte>();
    var frame = new Frame
    {
      Kind = FrameKind.Event,
      Flags = FrameFlags.HasTrace,
      NameCode = 0,
      NameText = "a"u8,
      Trace = Encoding.ASCII.GetBytes(trace),
    };
    FrameWriter.Write(output, in frame);
    return output.WrittenSpan.ToArray();
  }

  static List<string> Read(byte[] bytes, Side from)
  {
    var frames = new List<string>();
    var reader = new FrameReader(bytes, from);
    while (reader.TryRead(out var frame))
      frames.Add(Vectors.Render(in frame));

    return frames;
  }
}
