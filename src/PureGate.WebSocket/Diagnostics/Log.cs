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

  [LoggerMessage(14, LogLevel.Warning, "Connection {ConnectionId} has a stale contract.ts ({Hash}; the server's is {Current}). Pull it again.")]
  public static partial void ContractStale(ILogger logger, string connectionId, string hash, string current);

  [LoggerMessage(15, LogLevel.Error, "The contract could not be generated, so browsers' contract hashes are not checked.")]
  public static partial void ContractFailed(ILogger logger, Exception exception);

  [LoggerMessage(16, LogLevel.Debug, "Connection {ConnectionId} resumed its session.")]
  public static partial void SessionResumed(ILogger logger, string connectionId);

  [LoggerMessage(17, LogLevel.Debug, "The session of connection {ConnectionId} expired without a resume.")]
  public static partial void SessionExpired(ILogger logger, string connectionId);

  [LoggerMessage(18, LogLevel.Error, "Authenticate failed for connection {ConnectionId}.")]
  public static partial void AuthenticationFailed(ILogger logger, string connectionId, Exception exception);

  [LoggerMessage(19, LogLevel.Information, "A token was refused for connection {ConnectionId}.")]
  public static partial void TokenRefused(ILogger logger, string connectionId);

  [LoggerMessage(20, LogLevel.Information, "The authentication of connection {ConnectionId} expired without a refresh.")]
  public static partial void AuthenticationExpired(ILogger logger, string connectionId);

  [LoggerMessage(21, LogLevel.Debug, "Connection {ConnectionId} is over its rate limit.")]
  public static partial void RateLimited(ILogger logger, string connectionId);

  [LoggerMessage(22, LogLevel.Warning, "The backplane failed to {Change} the topic {Topic}; trying again in {Retry}.")]
  public static partial void BackplaneSubscriptionFailed(ILogger logger, string change, string topic, TimeSpan retry, Exception exception);

  [LoggerMessage(23, LogLevel.Warning, "Dropped a message from the backplane that could not be read.")]
  public static partial void BackplaneMessageDropped(ILogger logger, Exception exception);

  [LoggerMessage(24, LogLevel.Debug, "Dropped a message from the backplane for the event {Event}, which no endpoint on this server maps.")]
  public static partial void BackplaneEventNotMapped(ILogger logger, string @event);

  [LoggerMessage(25, LogLevel.Warning, "The event {Event} from the backplane could not be read, so the connections that take it in another format or from another endpoint did not get it.")]
  public static partial void BackplanePayloadUnreadable(ILogger logger, string @event, Exception exception);
}
