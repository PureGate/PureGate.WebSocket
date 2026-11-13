using System.Text;
using System.Text.Json;
using PureGate.WebSocket.Diagnostics;
using PureGate.WebSocket.Hosting;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Serialization;
using PureGate.WebSocket.Transport;

namespace PureGate.WebSocket;

// Calls from the browser. Every Request gets exactly one Response; the call leaves the window before its Response
// is written, so the browser's next call never meets a window the server still counts as full.
public sealed partial class WebSocketConnection
{
  const string GenericFailure = "The handler failed.";

  // The spare cancellation sources a connection keeps. More would hold on to what one busy moment needed for as
  // long as the connection lasts.
  const int SpareCancellationLimit = 8;

  readonly Dictionary<uint, WebSocketCall> ActiveCalls = [];
  readonly Lock CallsGate = new();

  // Cancellation sources of finished calls, linked to the session's token, for later calls to reuse as Kestrel reuses
  // RequestAborted's: TryReset keeps the link and the deadline's timer, so neither is created for every call. Guarded
  // by CallsGate.
  readonly Stack<CancellationTokenSource> SpareCancellations = new();

  enum CallOutcome
  {
    Completed,
    Cancelled,
    TimedOut,
    SessionEnded,
  }

  void OnRequest(scoped in Frame frame)
  {
    lock (CallsGate)
    {
      if (ActiveCalls.Count >= Options.MaxConcurrentCalls)
        ProtocolException.Throw(ProtocolViolation.WindowExceeded);
      if (IsInUse(frame.Id))
        ProtocolException.Throw(ProtocolViolation.DuplicateId);
    }

    var operation = Names.Resolve(frame.NameCode, frame.NameText);
    if (operation.Kind != OperationKind.Request)
    {
      RejectCall(frame.Id, ErrorCode.UnknownOperation, "No operation has this name.");
      return;
    }

    if (!TryAcquire())
    {
      RejectCall(frame.Id, ErrorCode.Overloaded, "Too many requests.");
      return;
    }

    operation.Start(this, in frame);
    ReleaseUnusedLease();
  }

  void OnCancel(uint id)
  {
    // Under the lock, the call is still active and owns its cancellation source. A Cancel for an exchange that
    // already ended crossed its end on the wire.
    lock (CallsGate)
    {
      if (ActiveCalls.TryGetValue(id, out var call))
      {
        call.CancelByClient();
        return;
      }

      if (Downloads.TryGetValue(id, out var stream))
      {
        stream.Call.CancelByClient();
        return;
      }
    }

    CancelUpload(id);
  }

  internal CancellationTokenSource RentCancellation()
  {
    lock (CallsGate)
    {
      while (SpareCancellations.TryPop(out var spare))
      {
        // Cancelled while it waited, when the session ended.
        if (!spare.IsCancellationRequested)
          return spare;

        spare.Dispose();
      }
    }

    return CancellationTokenSource.CreateLinkedTokenSource(Closed);
  }

  // A source whose token fired, or whose timer did, cannot be reset, and is disposed instead.
  internal void ReturnCancellation(CancellationTokenSource cancellation)
  {
    if (cancellation.TryReset())
    {
      lock (CallsGate)
      {
        if (SpareCancellations.Count < SpareCancellationLimit)
        {
          SpareCancellations.Push(cancellation);
          return;
        }
      }
    }

    cancellation.Dispose();
  }

  internal WebSocketCall BeginCall(Operation operation, scoped in Frame frame)
  {
    var call = new WebSocketCall(this, operation.Name, frame.Id, ReadTrace(in frame), Endpoint.Scopes) { Lease = TakeLease() };
    if (frame.Has(FrameFlags.HasTimeout))
      call.SetDeadline(frame.Timeout);

    lock (CallsGate)
      ActiveCalls.Add(frame.Id, call);

    return call;
  }

  internal string ReadTrace(scoped in Frame frame) =>
    frame.Has(FrameFlags.HasTrace) ? Encoding.ASCII.GetString(frame.Trace) : null;

  internal async ValueTask CompleteCallAsync<TResponse>(WebSocketCall call, PayloadCodec<TResponse> codec, TResponse result)
  {
    EndCall(call);
    var outcome = GetOutcome(call);
    if (outcome == CallOutcome.Completed)
      SendResult(call, codec, result);
    else
      SendOutcome(call.Id, outcome);

    Finished(call, outcome, null);
    await call.DisposeAsync();
    CheckDrained();
  }

  internal async ValueTask FailCallAsync(WebSocketCall call, Exception exception)
  {
    EndCall(call);
    var outcome = GetOutcome(call);
    if (outcome != CallOutcome.Completed)
      SendOutcome(call.Id, outcome);
    else
      SendFailure(call, exception, FrameKind.Response);

    Finished(call, outcome, exception);
    await call.DisposeAsync();
    CheckDrained();
  }

  // Ends the call's span and records its duration, with "ok" or the error code it ended with.
  void Finished(WebSocketCall call, CallOutcome outcome, Exception failure) =>
    Endpoint.Telemetry.Complete(call.Activity, call.Name, call.Started, OutcomeName(outcome, failure));

  static string OutcomeName(CallOutcome outcome, Exception failure) => outcome switch
  {
    CallOutcome.Cancelled => "cancelled",
    CallOutcome.TimedOut => "timeout",
    CallOutcome.SessionEnded => "connection_closed",
    _ => failure switch
    {
      null => "ok",
      CallException callException => callException.Code,
      InvalidPayloadException => "invalid_payload",
      ItemTooLargeException => "payload_too_large",
      _ => "handler_failed",
    },
  };

  internal void RejectCall(uint id, ErrorCode code, string message) => SendError(id, code, default, message, default);

  void EndCall(WebSocketCall call)
  {
    lock (CallsGate)
      ActiveCalls.Remove(call.Id);

    call.ReleaseLease();
  }

  // Why the call ends without its handler's own outcome, if it does.
  CallOutcome GetOutcome(WebSocketCall call) =>
    call.CancelledByClient ? CallOutcome.Cancelled
    : IsEnded || Closed.IsCancellationRequested ? CallOutcome.SessionEnded
    : call.DeadlinePassed ? CallOutcome.TimedOut
    : CallOutcome.Completed;

  void SendOutcome(uint id, CallOutcome outcome, FrameKind kind = FrameKind.Response)
  {
    if (outcome == CallOutcome.Cancelled)
      SendError(id, ErrorCode.Cancelled, default, "The call was cancelled.", default, kind);
    else if (outcome == CallOutcome.TimedOut)
      SendError(id, ErrorCode.Timeout, default, "The call's deadline passed.", default, kind);
  }

  void SendResult<TResponse>(WebSocketCall call, PayloadCodec<TResponse> codec, TResponse result)
  {
    var buffer = PooledBuffer.Scratch();
    try
    {
      codec.Write(buffer, result);
      var frame = new Frame
      {
        Kind = FrameKind.Response,
        Flags = codec.IsBinary ? FrameFlags.Binary : FrameFlags.None,
        Id = call.Id,
        Payload = buffer.WrittenSpan,
      };
      if (Write(in frame) == WriteResult.TooLarge)
        RejectCall(call.Id, ErrorCode.PayloadTooLarge, "The result is larger than the browser accepts.");
    }
    catch (Exception exception)
    {
      Log.ResultNotWritten(Logger, Id, call.Name, exception);
      SendError(call.Id, ErrorCode.HandlerFailed, default, Options.IncludeExceptionDetails ? exception.Message : GenericFailure, default);
    }
    finally
    {
      buffer.ReturnScratch();
    }
  }

  void SendCallException(uint id, CallException failure, FrameKind kind = FrameKind.Response)
  {
    var custom = ErrorCodes.TryGetCode(failure.Code, out var code) ? default : Encoding.ASCII.GetBytes(failure.Code);
    if (failure.Details is null)
    {
      SendError(id, code, custom, failure.Message, default, kind);
      return;
    }

    var buffer = PooledBuffer.Scratch();
    try
    {
      try
      {
        using var writer = new Utf8JsonWriter(buffer);
        JsonSerializer.Serialize(writer, failure.Details, Options.SerializerOptions.GetTypeInfo(failure.Details.GetType()));
      }
      catch (Exception exception)
      {
        Log.DetailsNotWritten(Logger, Id, exception);
        SendError(id, code, custom, failure.Message, default, kind);
        return;
      }

      SendError(id, code, custom, failure.Message, buffer.WrittenSpan, kind);
    }
    finally
    {
      buffer.ReturnScratch();
    }
  }

  // A failed Response, or a failed StreamEnd.
  internal void SendError(
      uint id,
      ErrorCode code,
      ReadOnlySpan<byte> custom,
      string message,
      ReadOnlySpan<byte> details,
      FrameKind kind = FrameKind.Response)
  {
    var frame = new Frame
    {
      Kind = kind,
      Flags = FrameFlags.HasError,
      Id = id,
      ErrorCode = code,
      ErrorCustom = custom,
      ErrorMessage = Encoding.UTF8.GetBytes(message ?? ""),
      Payload = details,
    };

    if (Write(in frame) == WriteResult.TooLarge)
    {
      frame.Payload = default;
      frame.ErrorMessage = "The error was too large to send."u8;
      Write(in frame);
    }
  }
}
