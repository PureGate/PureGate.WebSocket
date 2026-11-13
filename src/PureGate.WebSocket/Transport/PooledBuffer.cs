using System.Buffers;

namespace PureGate.WebSocket.Transport;

// A growable buffer over arrays rented from the shared pool. Return gives the array back; the buffer is not used
// after that.
sealed class PooledBuffer : IBufferWriter<byte>
{
  // What a thread keeps between payloads: a buffer whose array is no larger than this.
  const int ScratchLimit = 16 * 1024;

  [ThreadStatic]
  static PooledBuffer CachedScratch;

  byte[] Bytes;
  int Written;

  public PooledBuffer(int capacity) => Bytes = ArrayPool<byte>.Shared.Rent(Math.Max(capacity, 16));

  public int WrittenCount => Written;

  public ReadOnlySpan<byte> WrittenSpan => Bytes.AsSpan(0, Written);

  public ReadOnlyMemory<byte> WrittenMemory => Bytes.AsMemory(0, Written);

  public void Advance(int count) => Written += count;

  // Empties the buffer and keeps its array, for reuse.
  public void Clear() => Written = 0;

  public Memory<byte> GetMemory(int sizeHint = 0)
  {
    Reserve(sizeHint);
    return Bytes.AsMemory(Written);
  }

  public Span<byte> GetSpan(int sizeHint = 0)
  {
    Reserve(sizeHint);
    return Bytes.AsSpan(Written);
  }

  // A buffer for serializing one payload before it is copied into the outbox: the one this thread keeps, so a
  // payload costs neither an allocation nor the array growing to its size again. Given back with ReturnScratch on
  // the same thread. A payload written while another is being serialized, by a converter, gets a buffer of its own.
  public static PooledBuffer Scratch()
  {
    var buffer = CachedScratch;
    if (buffer is null)
      return new PooledBuffer(256);

    CachedScratch = null;
    return buffer;
  }

  public void ReturnScratch()
  {
    Written = 0;
    if (CachedScratch is null && Bytes.Length is > 0 and <= ScratchLimit)
      CachedScratch = this;
    else
      Return();
  }

  // Takes a new array for a buffer that was returned, so the buffer itself is used again.
  public PooledBuffer Rent(int capacity)
  {
    Bytes = ArrayPool<byte>.Shared.Rent(Math.Max(capacity, 16));
    Written = 0;
    return this;
  }

  public void Return()
  {
    var bytes = Bytes;
    Bytes = [];
    Written = 0;
    if (bytes.Length > 0)
      ArrayPool<byte>.Shared.Return(bytes);
  }

  void Reserve(int sizeHint)
  {
    var needed = Math.Max(sizeHint, 1);
    if (Bytes.Length - Written >= needed)
      return;

    var grown = ArrayPool<byte>.Shared.Rent(Math.Max(Bytes.Length * 2, Written + needed));
    Bytes.AsSpan(0, Written).CopyTo(grown);
    if (Bytes.Length > 0)
      ArrayPool<byte>.Shared.Return(Bytes);
    Bytes = grown;
  }
}
