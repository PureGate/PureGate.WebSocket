namespace PureGate.WebSocket.Protocol;

// Turns a received name's text into what the receiver keeps for it, such as the operation it names.
delegate T NameResolver<T>(ReadOnlySpan<byte> name);

// The receive side of a name table (docs/PROTOCOL.md, Names). It keeps only what the resolver returns, never
// the peer's text, so a peer that defines a full table of long names costs one entry per name.
sealed class IncomingNameTable<T>(NameResolver<T> resolve, int limit = NameRules.TableLimit)
{
  T[] Entries = [];
  int Count;

  // What a frame's name field stands for. text is the field's text, when its code carries one.
  public T Resolve(uint code, ReadOnlySpan<byte> text)
  {
    if (code == 0)
      return resolve(text);

    if ((code & 1) == 1)
    {
      if ((code - 1) / 2 != (uint)Count + 1)
        ProtocolException.Throw(ProtocolViolation.NameOutOfOrder);
      if (Count >= limit)
        ProtocolException.Throw(ProtocolViolation.NameTableFull);

      var value = resolve(text);
      if (Count == Entries.Length)
        Array.Resize(ref Entries, Math.Clamp(Count * 2, 8, Math.Max(8, limit)));

      Entries[Count++] = value;
      return value;
    }

    var number = code / 2;
    if (number > (uint)Count)
      ProtocolException.Throw(ProtocolViolation.NameUndefined);

    return Entries[number - 1];
  }
}

// The send side of a name table: decides how each frame names its operation or event.
sealed class OutgoingNameTable(int limit = NameRules.TableLimit)
{
  readonly Dictionary<string, uint> Numbers = new(StringComparer.Ordinal);

  // The code for the next frame that carries name. Codes 0 and 2n + 1 need the name's text after them
  // (FrameLayout.NameHasText). Frames must go out in the order their codes were taken, so definitions arrive
  // numbered 1, 2, 3, ….
  public uint Next(string name)
  {
    if (Numbers.TryGetValue(name, out var number))
      return number * 2;
    if (Numbers.Count >= limit)
      return 0;

    number = (uint)Numbers.Count + 1;
    Numbers.Add(name, number);
    return number * 2 + 1;
  }
}
