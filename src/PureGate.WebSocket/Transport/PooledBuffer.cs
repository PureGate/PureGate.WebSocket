using System.Buffers;

namespace PureGate.WebSocket.Transport;

// A growable buffer over arrays rented from the shared pool. Return gives the array back; the buffer is not used
// after that.
sealed class PooledBuffer : IBufferWriter<byte>
{
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
    ArrayPool<byte>.Shared.Return(Bytes);
    Bytes = grown;
  }
}
