using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test;

public sealed class ErrorCodeTests
{
  [Test]
  public void NamesEveryCodeAsTheVectorsDo()
  {
    var names = Vectors.Get("errorCodes").EnumerateArray().Select(name => name.GetString()).ToArray();

    Assert.That(names, Has.Length.EqualTo((int)ErrorCodes.Last));
    for (var code = 1; code <= (int)ErrorCodes.Last; code++)
    {
      Assert.That(ErrorCodes.GetName((ErrorCode)code), Is.EqualTo(names[code - 1]));
      Assert.That(ErrorCodes.TryGetCode(names[code - 1], out var parsed), Is.True);
      Assert.That(parsed, Is.EqualTo((ErrorCode)code));
    }
  }

  [Test]
  public void TreatsOtherNamesAsCustomCodes()
  {
    Assert.That(ErrorCodes.TryGetCode("not_found", out var code), Is.False);
    Assert.That(code, Is.EqualTo(ErrorCode.Custom));
    Assert.Throws<ArgumentOutOfRangeException>(() => ErrorCodes.GetName(ErrorCode.Custom));
  }
}
