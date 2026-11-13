namespace PureGate.WebSocket.Protocol;

// Operation and event names (docs/PROTOCOL.md, Names): dot-separated segments of [A-Za-z_][A-Za-z0-9_]*,
// 1 to 256 bytes in all.
static class NameRules
{
  public const int MaxLength = 256;

  // Definitions per direction. A sender names further operations literally.
  public const int TableLimit = 1024;

  public static bool IsValid(ReadOnlySpan<byte> name)
  {
    if (name.IsEmpty || name.Length > MaxLength)
      return false;

    var segmentStart = true;
    foreach (var b in name)
    {
      if (b == (byte)'.')
      {
        if (segmentStart)
          return false;
        segmentStart = true;
      }
      else if (IsLetterOrUnderscore(b) || (!segmentStart && IsDigit(b)))
      {
        segmentStart = false;
      }
      else
      {
        return false;
      }
    }

    return !segmentStart;
  }

  // b | 0x20 maps A–Z onto a–z and nothing else onto them.
  static bool IsLetterOrUnderscore(byte b) => (uint)((b | 0x20) - 'a') <= 'z' - 'a' || b == (byte)'_';

  static bool IsDigit(byte b) => (uint)(b - '0') <= 9;
}
