using PureGate.WebSocket.Diagnostics;
using PureGate.WebSocket.Hosting;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Serialization;
using PureGate.WebSocket.Transport;

namespace PureGate.WebSocket;

// Streams in both directions (docs/PROTOCOL.md, Streams). Downloads and uploads share the streams window, and their
// ids share the space of the browser's Requests. A stream leaves the window before its last frame is written.
public sealed partial class WebSocketConnection
{
  readonly Dictionary<uint, DownloadState> Downloads = [];
  readonly Dictionary<uint, UploadState> Uploads = [];

  void OnStreamOpen(scoped in Frame frame)
  {
    lock (CallsGate)
    {
      if (Downloads.Count + Uploads.Count >= Options.MaxStreams)
        ProtocolException.Throw(ProtocolViolation.WindowExceeded);
      if (IsInUse(frame.Id))
        ProtocolException.Throw(ProtocolViolation.DuplicateId);
    }

    var operation = Names.Resolve(frame.NameCode, frame.NameText);
    var kind = frame.Has(FrameFlags.Upload) ? OperationKind.Upload : OperationKind.Stream;
    if (operation.Kind != kind)
    {
      RejectStream(in frame, ErrorCode.UnknownOperation, "No stream has this name.");
      return;
    }

    if (!TryAcquire())
    {
      RejectStream(in frame, ErrorCode.Overloaded, "Too many requests.");
      return;
    }

    operation.Start(this, in frame);
    ReleaseUnusedLease();
  }

  // Refuses a stream at StreamOpen: a download with a failed StreamEnd, an upload with a failed Response, after which
  // the upload waits for the browser's StreamEnd.
  internal void RejectStream(scoped in Frame frame, ErrorCode code, string message)
  {
    if (!frame.Has(FrameFlags.Upload))
    {
      SendError(frame.Id, code, default, message, default, FrameKind.StreamEnd);
      return;
    }

    lock (CallsGate)
      Uploads.Add(frame.Id, new RefusedUpload(frame.Id) { Answered = true });
    SendError(frame.Id, code, default, message, default);
  }

  // Called under CallsGate.
  bool IsInUse(uint id) => ActiveCalls.ContainsKey(id) || Downloads.ContainsKey(id) || Uploads.ContainsKey(id);

  // Downloads.

  internal DownloadState BeginDownload(Operation operation, scoped in Frame frame)
  {
    var call = new WebSocketCall(this, operation.Name, frame.Id, ReadTrace(in frame), Endpoint.Scopes, Closed) { Lease = TakeLease() };
    var stream = new DownloadState(call, frame.Credit);
    lock (CallsGate)
      Downloads.Add(frame.Id, stream);

    return stream;
  }

  // Writes one item. False when the stream has to end: the item is larger than the browser accepts, or the
  // connection is going away.
  internal bool WriteStreamItem<TItem>(DownloadState stream, PayloadCodec<TItem> codec, TItem item)
  {
    var buffer = new PooledBuffer(256);
    try
    {
      codec.Write(buffer, item);
      var frame = new Frame
      {
        Kind = FrameKind.StreamItem,
        Flags = codec.IsBinary ? FrameFlags.Binary : FrameFlags.None,
        Id = stream.Id,
        Payload = buffer.WrittenSpan,
      };

      var result = Write(in frame);
      if (result == WriteResult.TooLarge)
        throw new ItemTooLargeException();

      return result == WriteResult.Written;
    }
    finally
    {
      buffer.Return();
    }
  }

  internal async ValueTask EndDownloadAsync(DownloadState stream, Exception failure)
  {
    lock (CallsGate)
      Downloads.Remove(stream.Id);

    stream.Call.ReleaseLease();

    var outcome = GetOutcome(stream.Call);
    if (outcome != CallOutcome.Completed)
    {
      SendOutcome(stream.Id, outcome, FrameKind.StreamEnd);
    }
    else if (failure is null)
    {
      var end = new Frame { Kind = FrameKind.StreamEnd, Id = stream.Id };
      Write(in end);
    }
    else
    {
      SendFailure(stream.Call, failure, FrameKind.StreamEnd);
    }

    await stream.Call.DisposeAsync();
    CheckDrained();
  }

  void OnCredit(scoped in Frame frame)
  {
    // Credit with id 0 is the server's own, for the browser's events.
    if (frame.Id == 0)
      ProtocolException.Throw(ProtocolViolation.UnexpectedFrame);

    DownloadState stream;
    lock (CallsGate)
      Downloads.TryGetValue(frame.Id, out stream);

    // Credit for a stream that just ended crossed its StreamEnd on the wire.
    stream?.Grant(frame.Amount);
  }

  // Uploads.

  internal UploadState<TItem> BeginUpload<TItem>(Operation operation, scoped in Frame frame, PayloadCodec<TItem> codec)
  {
    var call = new WebSocketCall(this, operation.Name, frame.Id, ReadTrace(in frame), Endpoint.Scopes, Closed) { Lease = TakeLease() };
    var upload = new UploadState<TItem>(frame.Id, call, codec, this);
    lock (CallsGate)
      Uploads.Add(frame.Id, upload);

    return upload;
  }

  internal void GrantUploadCredit(UploadState upload, uint amount)
  {
    lock (CallsGate)
    {
      if (upload.ClientEnded || upload.Answered)
        return;
      upload.Granted += amount;
    }

    var credit = new Frame { Kind = FrameKind.Credit, Id = upload.Id, Amount = amount };
    Write(in credit);
  }

  // The handler took an item: credit goes back in batches of half the window.
  internal void UploadItemConsumed<TItem>(UploadState<TItem> upload)
  {
    var batch = Options.UploadWindow / 2;
    if (++upload.Consumed < batch)
      return;

    upload.Consumed = 0;
    GrantUploadCredit(upload, (uint)batch);
  }

  void OnStreamItem(scoped in Frame frame)
  {
    UploadState upload;
    lock (CallsGate)
    {
      // Items come only from uploads; the server produces the downloads' items itself.
      if (!Uploads.TryGetValue(frame.Id, out upload) || upload.ClientEnded)
        ProtocolException.Throw(ProtocolViolation.UnknownExchange);
      if (++upload.Received > upload.Granted)
        ProtocolException.Throw(ProtocolViolation.WindowExceeded);
      if (upload.Answered)
        return;
    }

    upload.Accept(in frame);
  }

  void OnStreamEnd(scoped in Frame frame)
  {
    UploadState upload;
    lock (CallsGate)
    {
      if (!Uploads.TryGetValue(frame.Id, out upload) || upload.ClientEnded)
        ProtocolException.Throw(ProtocolViolation.UnknownExchange);

      upload.ClientEnded = true;
      if (upload.Answered)
        Uploads.Remove(frame.Id);
    }

    upload.Finish(frame.Has(FrameFlags.HasError) ? CallException.FromFrame(in frame) : null);
    CheckDrained();
  }

  // The browser's Cancel for an upload: no more items come, and the handler is cancelled, even after the browser's
  // StreamEnd, since the upload lasts until the answer. A Cancel for an upload that has ended crossed its end on the
  // wire.
  void CancelUpload(uint id)
  {
    UploadState upload;
    var ended = false;
    lock (CallsGate)
    {
      if (!Uploads.TryGetValue(id, out upload))
        return;

      // Answered and not yet ended by the browser, or it would be gone: this Cancel is the browser's end.
      ended = upload.Answered;
      upload.ClientEnded = true;
      if (ended)
        Uploads.Remove(id);
    }

    if (ended)
    {
      CheckDrained();
      return;
    }

    upload.Call.CancelByClient();
    upload.Finish(new OperationCanceledException("The browser cancelled the upload."));
  }

  internal async ValueTask CompleteUploadAsync<TResponse>(UploadState upload, PayloadCodec<TResponse> codec, TResponse result)
  {
    Answer(upload);
    var outcome = GetOutcome(upload.Call);
    if (outcome == CallOutcome.Completed)
      SendResult(upload.Call, codec, result);
    else
      SendOutcome(upload.Id, outcome);

    await upload.Call.DisposeAsync();
    CheckDrained();
  }

  internal async ValueTask FailUploadAsync(UploadState upload, Exception exception)
  {
    Answer(upload);
    var outcome = GetOutcome(upload.Call);
    if (outcome != CallOutcome.Completed)
      SendOutcome(upload.Id, outcome);
    else
      SendFailure(upload.Call, exception, FrameKind.Response);

    await upload.Call.DisposeAsync();
    CheckDrained();
  }

  // The server's answer is about to go out. The upload leaves the window now if the browser has ended it too.
  void Answer(UploadState upload)
  {
    lock (CallsGate)
    {
      upload.Answered = true;
      if (upload.ClientEnded)
        Uploads.Remove(upload.Id);
    }

    upload.Call.ReleaseLease();
  }

  // Ends what the session still has open: their handlers see the session's token cancelled.
  void FinishStreams()
  {
    UploadState[] uploads;
    lock (CallsGate)
      uploads = [.. Uploads.Values];

    foreach (var upload in uploads)
      upload.Finish(new OperationCanceledException("The connection closed."));
  }

  // A handler's failure as an error frame: its own CallException, invalid_payload for an item it could not read,
  // payload_too_large for an item too large to send, or handler_failed.
  void SendFailure(WebSocketCall call, Exception failure, FrameKind kind)
  {
    switch (failure)
    {
      case CallException callException:
        SendCallException(call.Id, callException, kind);
        break;
      case InvalidPayloadException invalid:
        SendError(call.Id, ErrorCode.InvalidPayload, default, invalid.Message, default, kind);
        break;
      case ItemTooLargeException:
        SendError(call.Id, ErrorCode.PayloadTooLarge, default, "An item is larger than the browser accepts.", default, kind);
        break;
      default:
        Log.HandlerFailed(Logger, Id, call.Name, failure);
        SendError(call.Id, ErrorCode.HandlerFailed, default, Options.IncludeExceptionDetails ? failure.Message : GenericFailure, default, kind);
        break;
    }
  }
}

// A stream item larger than the browser accepts, which ends the stream.
sealed class ItemTooLargeException() : Exception("An item is larger than the browser accepts.");
