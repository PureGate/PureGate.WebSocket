using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Test;

// test-vectors/frames.json, which the TypeScript tests check too, and the helpers to read it.
static partial class Vectors
{
  static readonly Lazy<JsonElement> Root = new(() =>
  {
    var path = Path.Combine(AppContext.BaseDirectory, "test-vectors", "frames.json");
    using var document = JsonDocument.Parse(File.ReadAllBytes(path));
    return document.RootElement.Clone();
  });

  public static JsonElement Get(params string[] path)
  {
    var element = Root.Value;
    foreach (var name in path)
      element = element.GetProperty(name);

    return element;
  }

  // One test case per vector, named after its description or, failing that, its bytes.
  public static IEnumerable<TestCaseData> Cases(params string[] path) =>
    Get(path).EnumerateArray().Select(vector => new TestCaseData(vector).SetArgDisplayNames(
      vector.TryGetProperty("description", out var description)
        ? description.GetString()
        : $"[{vector.GetProperty("bytes").GetString()}]"));

  // The vectors' byte notation: two-digit hex bytes and 'quoted' UTF-8 text, separated by spaces.
  public static byte[] ParseBytes(string notation)
  {
    var bytes = new List<byte>();
    foreach (Match token in Token().Matches(notation))
    {
      if (token.Groups[1].Success)
        bytes.AddRange(Encoding.UTF8.GetBytes(token.Groups[1].Value));
      else if (token.Groups[2].Success)
        bytes.Add(byte.Parse(token.Groups[2].Value, NumberStyles.HexNumber, CultureInfo.InvariantCulture));
      else
        throw new FormatException($"Not a hex byte or quoted text: {token.Value}");
    }

    return [.. bytes];
  }

  public static byte[] Bytes(JsonElement vector) => ParseBytes(vector.GetProperty("bytes").GetString());

  public static Side From(JsonElement vector) => vector.GetProperty("from").GetString() switch
  {
    "client" => Side.Client,
    "server" => Side.Server,
    var other => throw new FormatException($"Unknown side: {other}"),
  };

  // The name a violation has in the vectors, such as varint_too_long.
  public static string Name(ProtocolViolation violation) =>
    CapitalAfterLowercase().Replace(violation.ToString(), "_$1").ToLowerInvariant();

  // The frame a vector describes. Flags follow from the fields it lists.
  public static Frame ToFrame(JsonElement json)
  {
    var frame = new Frame { Kind = Enum.Parse<FrameKind>(json.GetProperty("kind").GetString()) };
    if (json.TryGetProperty("binary", out var binary) && binary.GetBoolean())
      frame.Flags |= FrameFlags.Binary;
    if (json.TryGetProperty("upload", out var upload) && upload.GetBoolean())
      frame.Flags |= FrameFlags.Upload;
    if (json.TryGetProperty("id", out var id))
      frame.Id = id.GetUInt32();

    if (json.TryGetProperty("name", out var name))
    {
      frame.NameCode = name.GetProperty("code").GetUInt32();
      if (name.TryGetProperty("text", out var text))
        frame.NameText = Encoding.UTF8.GetBytes(text.GetString());
    }

    if (json.TryGetProperty("timeout", out var timeout))
    {
      frame.Flags |= FrameFlags.HasTimeout;
      frame.Timeout = timeout.GetUInt32();
    }

    if (json.TryGetProperty("trace", out var trace))
    {
      frame.Flags |= FrameFlags.HasTrace;
      frame.Trace = Encoding.UTF8.GetBytes(trace.GetString());
    }

    if (json.TryGetProperty("credit", out var credit))
      frame.Credit = credit.GetUInt32();
    if (json.TryGetProperty("amount", out var amount))
      frame.Amount = amount.GetUInt32();
    if (json.TryGetProperty("count", out var count))
      frame.Count = count.GetUInt32();

    if (json.TryGetProperty("error", out var error))
    {
      frame.Flags |= FrameFlags.HasError;
      frame.ErrorCode = (ErrorCode)error.GetProperty("code").GetByte();
      if (error.TryGetProperty("custom", out var custom))
        frame.ErrorCustom = Encoding.UTF8.GetBytes(custom.GetString());
      frame.ErrorMessage = Encoding.UTF8.GetBytes(error.GetProperty("message").GetString());
    }

    if (json.TryGetProperty("json", out var payloadJson))
      frame.Payload = Encoding.UTF8.GetBytes(payloadJson.GetString());
    else if (json.TryGetProperty("bytes", out var payloadBytes))
      frame.Payload = ParseBytes(payloadBytes.GetString());

    return frame;
  }

  // A frame as text, showing only the fields its kind and flags carry, so frames compare by what they mean.
  public static string Render(scoped in Frame frame)
  {
    var text = new StringBuilder(frame.Kind.ToString());
    if (frame.Has(FrameFlags.Binary))
      text.Append(" binary");
    if (frame.Has(FrameFlags.Upload))
      text.Append(" upload");
    if (FrameLayout.HasId(frame.Kind))
      text.Append(CultureInfo.InvariantCulture, $" id={frame.Id}");

    if (FrameLayout.HasName(frame.Kind))
    {
      text.Append(CultureInfo.InvariantCulture, $" name={frame.NameCode}");
      if (FrameLayout.NameHasText(frame.NameCode))
        text.Append(CultureInfo.InvariantCulture, $" '{Encoding.UTF8.GetString(frame.NameText)}'");
    }

    if (frame.Has(FrameFlags.HasTimeout))
      text.Append(CultureInfo.InvariantCulture, $" timeout={frame.Timeout}");
    if (frame.Has(FrameFlags.HasTrace))
      text.Append(CultureInfo.InvariantCulture, $" trace='{Encoding.UTF8.GetString(frame.Trace)}'");
    if (frame.Kind == FrameKind.StreamOpen)
      text.Append(CultureInfo.InvariantCulture, $" credit={frame.Credit}");
    if (frame.Kind == FrameKind.Credit)
      text.Append(CultureInfo.InvariantCulture, $" amount={frame.Amount}");
    if (frame.Kind == FrameKind.Ack)
      text.Append(CultureInfo.InvariantCulture, $" count={frame.Count}");

    if (frame.Has(FrameFlags.HasError))
    {
      text.Append(CultureInfo.InvariantCulture, $" error={(int)frame.ErrorCode}");
      if (frame.ErrorCode == ErrorCode.Custom)
        text.Append(CultureInfo.InvariantCulture, $" '{Encoding.UTF8.GetString(frame.ErrorCustom)}'");
      text.Append(CultureInfo.InvariantCulture, $" message='{Encoding.UTF8.GetString(frame.ErrorMessage)}'");
    }

    if (FrameLayout.HasPayload(frame.Kind))
      text.Append(CultureInfo.InvariantCulture, $" payload={Convert.ToHexStringLower(frame.Payload)}");

    return text.ToString();
  }

  [GeneratedRegex(@"'([^']*)'|([0-9a-fA-F]{2})(?=\s|$)|\S+")]
  private static partial Regex Token();

  [GeneratedRegex("(?<=[a-z0-9])([A-Z])")]
  private static partial Regex CapitalAfterLowercase();
}
