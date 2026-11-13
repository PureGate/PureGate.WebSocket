using System.Runtime.CompilerServices;
using System.Threading.Channels;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Serialization;

namespace PureGate.WebSocket.Hosting;

// A stream the server produces for the browser (docs/PROTOCOL.md, Streams). The browser's credit is a semaphore:
// the producer takes one before each item, and Credit frames release more.
sealed class DownloadState(WebSocketCall call, uint credit)
{
  readonly SemaphoreSlim Credit = new((int)Math.Min(credit, int.MaxValue), int.MaxValue);

  public WebSocketCall Call { get; } = call;

  public uint Id => Call.Id;

  public Task WaitForCreditAsync(CancellationToken cancellationToken) => Credit.WaitAsync(cancellationToken);

  // Called on the receive loop only. The producer can only make room, so the room read here stays valid. Credit past
  // int.MaxValue items is as good as unlimited.
  public void Grant(uint amount)
  {
    var room = int.MaxValue - Credit.CurrentCount;
    if (room > 0)
      Credit.Release((int)Math.Min(amount, (uint)room));
  }
}

// A stream the browser sends to the server. It is registered until both sides are done with it: the server has
// answered, and the browser has sent StreamEnd or Cancel. Items that arrive after the answer are discarded.
abstract class UploadState(uint id, WebSocketCall call)
{
  // Read and written under the connection's calls lock: items the browser may send in all, items it sent, and
  // whether each side is done.
  public uint Granted;
  public uint Received;
  public bool ClientEnded;
  public bool Answered;

  public uint Id { get; } = id;

  // Null for an upload refused before a handler ran.
  public WebSocketCall Call { get; } = call;

  // One item from the browser, on the receive loop.
  public abstract void Accept(scoped in Frame item);

  // No more items: the browser's StreamEnd (error is its failure, or null), its Cancel, or the session's end.
  public abstract void Finish(Exception error);
}

// An upload refused at StreamOpen: it only waits for the browser's StreamEnd, discarding what comes before it.
sealed class RefusedUpload(uint id) : UploadState(id, null)
{
  public override void Accept(scoped in Frame item)
  {
  }

  public override void Finish(Exception error)
  {
  }
}

sealed class UploadState<TItem>(uint id, WebSocketCall call, PayloadCodec<TItem> codec, WebSocketConnection connection)
  : UploadState(id, call)
{
  readonly Channel<TItem> Items = Channel.CreateUnbounded<TItem>(new UnboundedChannelOptions
  {
    SingleReader = true,
    SingleWriter = true,
  });

  // Items the handler took since the last grant. Only the handler's enumeration touches it, one item at a time.
  public int Consumed;

  public override void Accept(scoped in Frame item)
  {
    TItem value;
    try
    {
      value = codec.Read(connection.PayloadOf(item.Payload), item.Has(FrameFlags.Binary));
    }
    catch (InvalidPayloadException exception)
    {
      Items.Writer.TryComplete(exception);
      return;
    }

    Items.Writer.TryWrite(value);
  }

  public override void Finish(Exception error) => Items.Writer.TryComplete(error);

  // The handler's view of the items. Every item it takes counts toward the browser's next credit.
  public async IAsyncEnumerable<TItem> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken)
  {
    while (await Items.Reader.WaitToReadAsync(cancellationToken))
    {
      while (Items.Reader.TryRead(out var item))
      {
        connection.UploadItemConsumed(this);
        yield return item;
      }
    }
  }
}
