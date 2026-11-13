using Microsoft.Extensions.Logging;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket.Diagnostics;

// The library's log messages, source-generated so they cost nothing when their level is off.
static partial class Log
{
  [LoggerMessage(1, LogLevel.Error, "PureGate.WebSocket needs the WebSocket middleware: call app.UseWebSockets() before mapping the endpoint.")]
  public static partial void WebSocketsNotEnabled(ILogger logger);

  [LoggerMessage(2, LogLevel.Warning, "Refused a WebSocket upgrade from origin {Origin}. Allow it in PureGateWebSocketOptions.AllowedOrigins if it is yours.")]
  public static partial void OriginRefused(ILogger logger, string origin);

  [LoggerMessage(3, LogLevel.Warning, "Connection {ConnectionId} broke the protocol ({Violation}): {Reason}")]
  public static partial void ProtocolViolation(ILogger logger, string connectionId, ProtocolViolation violation, string reason);

  [LoggerMessage(4, LogLevel.Warning, "Connection {ConnectionId} fell too far behind and is being closed.")]
  public static partial void SlowConsumer(ILogger logger, string connectionId);

  [LoggerMessage(5, LogLevel.Error, "An OnConnected handler failed for connection {ConnectionId}.")]
  public static partial void ConnectedHandlerFailed(ILogger logger, string connectionId, Exception exception);

  [LoggerMessage(6, LogLevel.Error, "An OnDisconnected handler failed for connection {ConnectionId}.")]
  public static partial void DisconnectedHandlerFailed(ILogger logger, string connectionId, Exception exception);

  [LoggerMessage(7, LogLevel.Error, "The handler of {Operation} failed for connection {ConnectionId}.")]
  public static partial void HandlerFailed(ILogger logger, string connectionId, string operation, Exception exception);

  [LoggerMessage(8, LogLevel.Error, "The result of {Operation} could not be written for connection {ConnectionId}.")]
  public static partial void ResultNotWritten(ILogger logger, string connectionId, string operation, Exception exception);

  [LoggerMessage(9, LogLevel.Warning, "The details of a CallException could not be written for connection {ConnectionId}; it was sent without them.")]
  public static partial void DetailsNotWritten(ILogger logger, string connectionId, Exception exception);

  [LoggerMessage(10, LogLevel.Warning, "The event {Event} is larger than connection {ConnectionId} accepts, and was not sent.")]
  public static partial void EventTooLarge(ILogger logger, string connectionId, string @event);

  [LoggerMessage(11, LogLevel.Debug, "The event {Event} was not sent to connection {ConnectionId}, which is behind.")]
  public static partial void EventNotSent(ILogger logger, string connectionId, string @event);

  [LoggerMessage(12, LogLevel.Debug, "Dropped an event {Event} from connection {ConnectionId}: {Reason}")]
  public static partial void EventDropped(ILogger logger, string connectionId, string @event, string reason);

  [LoggerMessage(13, LogLevel.Error, "The handler of the event {Event} failed for connection {ConnectionId}.")]
  public static partial void EventHandlerFailed(ILogger logger, string connectionId, string @event, Exception exception);
}
