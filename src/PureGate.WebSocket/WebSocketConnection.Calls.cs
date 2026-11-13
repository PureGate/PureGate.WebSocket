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

  readonly Dictionary<uint, WebSocketCall> ActiveCalls = [];
  readonly Lock CallsGate = new();

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
      if (ActiveCalls.ContainsKey(frame.Id))
        ProtocolException.Throw(ProtocolViolation.DuplicateId);
    }

    var operation = Names.Resolve(frame.NameCode, frame.NameText);
    if (operation.Kind != OperationKind.Request)
    {
      RejectCall(frame.Id, ErrorCode.UnknownOperation, "No operation has this name.");
      return;
    }

    operation.Start(this, in frame);
  }

  void OnCancel(uint id)
  {
    WebSocketCall call;
    lock (CallsGate)
      ActiveCalls.TryGetValue(id, out call);

    // A Cancel for a call that already ended crossed its Response on the wire.
    call?.CancelByClient();
  }

  internal WebSocketCall BeginCall(Operation operation, scoped in Frame frame)
  {
    var call = new WebSocketCall(this, operation.Name, frame.Id, ReadTrace(in frame), Endpoint.Scopes, Closed);
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

    await call.DisposeAsync();
  }

  internal async ValueTask FailCallAsync(WebSocketCall call, Exception exception)
  {
    EndCall(call);
    var outcome = GetOutcome(call);
    if (outcome != CallOutcome.Completed)
    {
      SendOutcome(call.Id, outcome);
    }
    else if (exception is CallException failure)
    {
      SendCallException(call.Id, failure);
    }
    else
    {
      Log.HandlerFailed(Logger, Id, call.Name, exception);
      SendError(call.Id, ErrorCode.HandlerFailed, default, Options.IncludeExceptionDetails ? exception.Message : GenericFailure, default);
    }

    await call.DisposeAsync();
  }

  internal void RejectCall(uint id, ErrorCode code, string message) => SendError(id, code, default, message, default);

  void EndCall(WebSocketCall call)
  {
    lock (CallsGate)
      ActiveCalls.Remove(call.Id);
  }

  // Why the call ends without its handler's own outcome, if it does.
  CallOutcome GetOutcome(WebSocketCall call) =>
    call.CancelledByClient ? CallOutcome.Cancelled
    : IsEnded || Closed.IsCancellationRequested ? CallOutcome.SessionEnded
    : call.DeadlinePassed ? CallOutcome.TimedOut
    : CallOutcome.Completed;

  void SendOutcome(uint id, CallOutcome outcome)
  {
    if (outcome == CallOutcome.Cancelled)
      SendError(id, ErrorCode.Cancelled, default, "The call was cancelled.", default);
    else if (outcome == CallOutcome.TimedOut)
      SendError(id, ErrorCode.Timeout, default, "The call's deadline passed.", default);
  }

  void SendResult<TResponse>(WebSocketCall call, PayloadCodec<TResponse> codec, TResponse result)
  {
    var buffer = new PooledBuffer(256);
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
      buffer.Return();
    }
  }

  void SendCallException(uint id, CallException failure)
  {
    var custom = ErrorCodes.TryGetCode(failure.Code, out var code) ? default : Encoding.ASCII.GetBytes(failure.Code);
    if (failure.Details is null)
    {
      SendError(id, code, custom, failure.Message, default);
      return;
    }

    var buffer = new PooledBuffer(256);
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
        SendError(id, code, custom, failure.Message, default);
        return;
      }

      SendError(id, code, custom, failure.Message, buffer.WrittenSpan);
    }
    finally
    {
      buffer.Return();
    }
  }

  internal void SendError(uint id, ErrorCode code, ReadOnlySpan<byte> custom, string message, ReadOnlySpan<byte> details)
  {
    var frame = new Frame
    {
      Kind = FrameKind.Response,
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
