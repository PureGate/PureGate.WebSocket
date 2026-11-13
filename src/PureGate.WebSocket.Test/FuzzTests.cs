using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test;

// Hostile input for the frame reader: random bytes and mutated valid messages. Every outcome is a frame or a
// ProtocolException, never another exception, a hang, or a read past the message. Seeded, so a failure repeats.
public sealed class FuzzTests
{
  const int Rounds = 20_000;

  [TestCase(true)]
  [TestCase(false)]
  public void ReadsRandomBytesSafely(bool fromClient)
  {
    var random = new Random(1234);
    for (var round = 0; round < Rounds; round++)
    {
      var message = new byte[random.Next(0, 64)];
      random.NextBytes(message);
      ReadAll(message, fromClient ? Side.Client : Side.Server);
    }
  }

  [Test]
  public void ReadsMutatedMessagesSafely()
  {
    var valid = Vectors.Get("messages", "valid").EnumerateArray()
      .Select(vector => (Bytes: Vectors.Bytes(vector), From: Vectors.From(vector)))
      .ToArray();
    var random = new Random(5678);
    for (var round = 0; round < Rounds; round++)
    {
      var (bytes, from) = valid[random.Next(valid.Length)];
      var message = Mutate(bytes, random);
      ReadAll(message, from);
    }
  }

  // Flips, replaces, inserts, removes or truncates a few bytes.
  internal static byte[] Mutate(byte[] bytes, Random random)
  {
    var message = new List<byte>(bytes);
    var mutations = random.Next(1, 4);
    for (var i = 0; i < mutations; i++)
    {
      var at = message.Count == 0 ? 0 : random.Next(message.Count);
      switch (random.Next(5))
      {
        case 0 when message.Count > 0:
          message[at] ^= (byte)(1 << random.Next(8));
          break;
        case 1 when message.Count > 0:
          message[at] = (byte)random.Next(256);
          break;
        case 2:
          message.Insert(at, (byte)random.Next(256));
          break;
        case 3 when message.Count > 0:
          message.RemoveAt(at);
          break;
        default:
          message.RemoveRange(at, message.Count - at);
          break;
      }
    }

    return [.. message];
  }

  static void ReadAll(byte[] message, Side from)
  {
    try
    {
      var reader = new FrameReader(message, from);
      var frames = 0;
      while (reader.TryRead(out var frame))
      {
        // Touch every span, so a slice past the message would throw here.
        _ = frame.NameText.Length + frame.Trace.Length + frame.ErrorCustom.Length + frame.ErrorMessage.Length + frame.Payload.Length;
        Assert.That(++frames, Is.LessThanOrEqualTo(message.Length));
      }
    }
    catch (ProtocolException)
    {
    }
  }
}
