using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Transport;

namespace PureGate.WebSocket.Test;

// The replay buffer packs frames into chunks that grow with what it holds, so frames of every size cross chunk
// boundaries: what a resume sends again has to be exactly the frames after the acknowledged ones, in order.
public sealed class ReplayBufferTests
{
  [Test]
  public void SendsAgainTheFramesAfterTheAcknowledgedOnes()
  {
    var replay = new ReplayBuffer(16 * 1024 * 1024);
    var sizes = new[] { 1, 30, 200, 511, 512, 513, 1500, 4000, 70_000, 3, 64 * 1024, 9000 };
    var frames = Enumerable.Range(0, 300).Select(i => Frame(i, sizes[i % sizes.Length])).ToList();

    foreach (var frame in frames)
      replay.Append(frame);

    Assert.That(replay.Count, Is.EqualTo(frames.Count));
    Assert.That(replay.Bytes, Is.EqualTo(frames.Sum(frame => (long)frame.Length)));
    Assert.That(CopyLast(replay, 25), Is.EqualTo(frames[^25..]));

    var dropped = 0;
    foreach (var step in new[] { 1, 7, 50, 2, 100, 39 })
    {
      replay.Drop(step);
      dropped += step;
      Assert.That(replay.Count, Is.EqualTo(frames.Count - dropped));
      Assert.That(replay.Bytes, Is.EqualTo(frames.Skip(dropped).Sum(frame => (long)frame.Length)));
      Assert.That(CopyLast(replay, replay.Count), Is.EqualTo(frames[dropped..]));
    }
  }

  [Test]
  public void KeepsTakingFramesAfterItEmptied()
  {
    var replay = new ReplayBuffer(1024 * 1024);
    for (var round = 0; round < 5; round++)
    {
      var frames = Enumerable.Range(0, 40).Select(i => Frame(round * 100 + i, 20 + (i * 97 % 900))).ToList();
      foreach (var frame in frames)
        replay.Append(frame);

      Assert.That(CopyLast(replay, frames.Count), Is.EqualTo(frames));
      replay.Drop(frames.Count);
      Assert.That((replay.Count, replay.Bytes), Is.EqualTo((0L, 0L)));
    }
  }

  [Test]
  public void ClearsEverything()
  {
    var replay = new ReplayBuffer(1024 * 1024);
    for (var i = 0; i < 100; i++)
      replay.Append(Frame(i, 300));

    replay.Clear();
    Assert.That((replay.Count, replay.Bytes), Is.EqualTo((0L, 0L)));

    var frame = Frame(7, 40);
    replay.Append(frame);
    Assert.That(CopyLast(replay, 1), Is.EqualTo(new[] { frame }));
  }

  // A frame as it goes on the wire: its body's length, then the body, here filled with bytes that tell frames apart.
  static byte[] Frame(int number, int bodyLength)
  {
    var frame = new byte[Varint.GetLength((uint)bodyLength) + bodyLength];
    var offset = Varint.Write(frame, (uint)bodyLength);
    for (var i = offset; i < frame.Length; i++)
      frame[i] = (byte)(number * 31 + i);

    return frame;
  }

  static List<byte[]> CopyLast(ReplayBuffer replay, long frames)
  {
    var copies = new List<byte[]>();
    replay.CopyLast(frames, frame => copies.Add(frame.ToArray()));
    return copies;
  }
}
