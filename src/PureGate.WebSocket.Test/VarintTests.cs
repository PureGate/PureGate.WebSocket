using System.Text.Json;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test;

public sealed class VarintTests
{
  static IEnumerable<TestCaseData> Valid() => Vectors.Cases("varints", "valid");

  static IEnumerable<TestCaseData> Shortest() =>
    Vectors.Cases("varints", "valid").Where(test =>
      !((JsonElement)test.Arguments[0]).TryGetProperty("shortest", out var shortest) || shortest.GetBoolean());

  static IEnumerable<TestCaseData> Invalid() => Vectors.Cases("varints", "invalid");

  [TestCaseSource(nameof(Valid))]
  public void ReadsValidVarints(JsonElement vector)
  {
    var bytes = Vectors.Bytes(vector);
    var offset = 0;

    Assert.That(Varint.Read(bytes, ref offset), Is.EqualTo(vector.GetProperty("value").GetUInt32()));
    Assert.That(offset, Is.EqualTo(bytes.Length));
  }

  [TestCaseSource(nameof(Shortest))]
  public void WritesTheShortestForm(JsonElement vector)
  {
    var value = vector.GetProperty("value").GetUInt32();
    var buffer = new byte[Varint.MaxLength];

    var length = Varint.Write(buffer, value);

    Assert.That(buffer[..length], Is.EqualTo(Vectors.Bytes(vector)));
    Assert.That(Varint.GetLength(value), Is.EqualTo(length));
  }

  [TestCaseSource(nameof(Invalid))]
  public void RejectsInvalidVarints(JsonElement vector)
  {
    var bytes = Vectors.Bytes(vector);

    var exception = Assert.Throws<ProtocolException>(() =>
    {
      var offset = 0;
      Varint.Read(bytes, ref offset);
    });

    Assert.That(Vectors.Name(exception.Violation), Is.EqualTo(vector.GetProperty("violation").GetString()));
  }

  [TestCase(0u)]
  [TestCase(127u)]
  [TestCase(128u)]
  [TestCase((1u << 14) - 1)]
  [TestCase(1u << 14)]
  [TestCase((1u << 21) - 1)]
  [TestCase(1u << 21)]
  [TestCase((1u << 28) - 1)]
  [TestCase(1u << 28)]
  [TestCase(uint.MaxValue)]
  public void RoundTripsAtEveryLengthBoundary(uint value)
  {
    var buffer = new byte[Varint.MaxLength];
    var length = Varint.Write(buffer, value);
    var offset = 0;

    Assert.That(Varint.Read(buffer.AsSpan(0, length), ref offset), Is.EqualTo(value));
    Assert.That(offset, Is.EqualTo(length));
    Assert.That(Varint.GetLength(value), Is.EqualTo(length));
  }

  [Test]
  public void ReadsFromTheGivenOffset()
  {
    byte[] bytes = [0xFF, 0xAC, 0x02, 0x07];
    var offset = 1;

    Assert.That(Varint.Read(bytes, ref offset), Is.EqualTo(300u));
    Assert.That(offset, Is.EqualTo(3));
  }
}
