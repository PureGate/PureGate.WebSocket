using System.Text;
using System.Text.Json;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test;

public sealed class NameTableTests
{
  static IEnumerable<TestCaseData> Incoming() => Vectors.Cases("names", "incoming");

  static IEnumerable<TestCaseData> Outgoing() => Vectors.Cases("names", "outgoing");

  [TestCaseSource(nameof(Incoming))]
  public void ResolvesIncomingNames(JsonElement vector)
  {
    var table = new IncomingNameTable<string>(Encoding.ASCII.GetString, vector.GetProperty("limit").GetInt32());
    var fields = vector.GetProperty("fields").EnumerateArray().ToArray();
    var failsAt = vector.TryGetProperty("at", out var at) ? at.GetInt32() : fields.Length;

    var names = new List<string>();
    for (var i = 0; i < failsAt; i++)
      names.Add(Resolve(table, fields[i]));

    if (failsAt < fields.Length)
    {
      var exception = Assert.Throws<ProtocolException>(() => Resolve(table, fields[failsAt]));
      Assert.That(Vectors.Name(exception.Violation), Is.EqualTo(vector.GetProperty("violation").GetString()));
    }
    else
    {
      Assert.That(names, Is.EqualTo(vector.GetProperty("names").EnumerateArray().Select(name => name.GetString())));
    }
  }

  [TestCaseSource(nameof(Outgoing))]
  public void NumbersOutgoingNames(JsonElement vector)
  {
    var table = new OutgoingNameTable(vector.GetProperty("limit").GetInt32());

    var fields = vector.GetProperty("names").EnumerateArray().Select(name =>
    {
      var code = table.Next(name.GetString());
      return FrameLayout.NameHasText(code) ? $"{code} '{name.GetString()}'" : $"{code}";
    });

    Assert.That(fields, Is.EqualTo(vector.GetProperty("fields").EnumerateArray().Select(field =>
      field.TryGetProperty("text", out var text)
        ? $"{field.GetProperty("code").GetUInt32()} '{text.GetString()}'"
        : $"{field.GetProperty("code").GetUInt32()}")));
  }

  [Test]
  public void HoldsTheProtocolsLimitOf1024Definitions()
  {
    var incoming = new IncomingNameTable<int>(name => name.Length);
    var outgoing = new OutgoingNameTable();
    for (uint number = 1; number <= NameRules.TableLimit; number++)
    {
      Assert.That(outgoing.Next($"n{number}"), Is.EqualTo(number * 2 + 1));
      incoming.Resolve(number * 2 + 1, "x"u8);
    }

    Assert.That(outgoing.Next("one.more"), Is.Zero, "Past the limit, names go literally.");
    Assert.That(incoming.Resolve(NameRules.TableLimit * 2, default), Is.EqualTo(1), "The last name is still there.");
    var exception = Assert.Throws<ProtocolException>(() => incoming.Resolve(NameRules.TableLimit * 2 + 3, "x"u8));
    Assert.That(exception.Violation, Is.EqualTo(ProtocolViolation.NameTableFull));
  }

  [Test]
  public void KeepsOnlyWhatTheResolverReturns()
  {
    var calls = 0;
    var table = new IncomingNameTable<string>(name =>
    {
      calls++;
      return name.SequenceEqual("orders.get"u8) ? "known" : "unknown";
    });

    Assert.That(table.Resolve(3, "orders.get"u8), Is.EqualTo("known"));
    Assert.That(table.Resolve(5, "nothing.here"u8), Is.EqualTo("unknown"));
    Assert.That(table.Resolve(2, default), Is.EqualTo("known"));
    Assert.That(table.Resolve(4, default), Is.EqualTo("unknown"));
    Assert.That(calls, Is.EqualTo(2), "References don't resolve again.");
  }

  static string Resolve(IncomingNameTable<string> table, JsonElement field) =>
    table.Resolve(
      field.GetProperty("code").GetUInt32(),
      field.TryGetProperty("text", out var text) ? Encoding.UTF8.GetBytes(text.GetString()) : default);
}
