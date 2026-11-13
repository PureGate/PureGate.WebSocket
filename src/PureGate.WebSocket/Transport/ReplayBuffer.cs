using System.Buffers;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Transport;

// The last sequenced frames a session sent, as they went on the wire, for a resume to send again (docs/PROTOCOL.md,
// Acknowledgement and resume). Frames are packed into pooled chunks, and each starts with its length, so they need no
// index. The outbox numbers them and guards this with its lock.
sealed class ReplayBuffer(long limit)
{
  const int ChunkBytes = 64 * 1024;

  readonly Queue<Chunk> Chunks = new();
  Chunk Last;

  // Where the first chunk's first frame starts.
  int HeadOffset;

  public long Count { get; private set; }

  public long Bytes { get; private set; }

  // Whether a frame of this many bytes still fits.
  public bool CanHold(int length) => Bytes + length <= limit;

  public void Append(ReadOnlySpan<byte> frame)
  {
    if (Last is null || Last.Free < frame.Length)
    {
      Last = new Chunk(Math.Max(ChunkBytes, frame.Length));
      Chunks.Enqueue(Last);
    }

    frame.CopyTo(Last.Bytes.AsSpan(Last.Used));
    Last.Used += frame.Length;
    Count++;
    Bytes += frame.Length;
  }

  // Drops the oldest frames, which the browser has acknowledged.
  public void Drop(long frames)
  {
    for (; frames > 0 && Count > 0; frames--)
    {
      var chunk = Chunks.Peek();
      var length = FrameLength(chunk.Bytes.AsSpan(HeadOffset, chunk.Used - HeadOffset));
      HeadOffset += length;
      Bytes -= length;
      Count--;

      if (HeadOffset == chunk.Used)
      {
        Chunks.Dequeue();
        chunk.Return();
        if (chunk == Last)
          Last = null;
        HeadOffset = 0;
      }
    }
  }

  // Hands the newest frames to write, oldest first, one frame at a time.
  public void CopyLast(long frames, Action<ReadOnlySpan<byte>> write)
  {
    var skip = Count - frames;
    var offset = HeadOffset;
    foreach (var chunk in Chunks)
    {
      while (offset < chunk.Used)
      {
        var length = FrameLength(chunk.Bytes.AsSpan(offset, chunk.Used - offset));
        if (skip-- <= 0)
          write(chunk.Bytes.AsSpan(offset, length));

        offset += length;
      }

      offset = 0;
    }
  }

  public void Clear()
  {
    while (Chunks.TryDequeue(out var chunk))
      chunk.Return();

    Last = null;
    HeadOffset = 0;
    Count = 0;
    Bytes = 0;
  }

  // A frame's whole length, its length prefix included.
  static int FrameLength(ReadOnlySpan<byte> frame)
  {
    var offset = 0;
    var body = Varint.Read(frame, ref offset);
    return offset + (int)body;
  }

  sealed class Chunk(int size)
  {
    public byte[] Bytes = ArrayPool<byte>.Shared.Rent(size);
    public int Used;

    public int Free => Bytes.Length - Used;

    public void Return()
    {
      ArrayPool<byte>.Shared.Return(Bytes);
      Bytes = [];
    }
  }
}
