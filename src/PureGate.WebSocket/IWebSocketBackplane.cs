namespace PureGate.WebSocket;

// Carries events between the servers of an app, so hub.All, hub.User and hub.Group reach the connections on every
// server, and hub.Connection the server its connection is on. Register one as a service:
// services.AddSingleton<IWebSocketBackplane>(...).
//
// A backplane only moves bytes on topics: the hub encodes the messages it publishes and decodes those it receives.
// It subscribes to "all", to its own server's topic, and to the topic of each user and group that has connections on
// this server, for as long as it has them. Topics are any strings, as user ids and group names are: a backend with
// rules for names encodes them, and one shared by several apps prefixes them. A backend without cheap topics can
// ignore SubscribeAsync and UnsubscribeAsync and deliver every message to every server: the hub drops those that no
// connection here takes.
public interface IWebSocketBackplane
{
  // Called once when the app starts, before SubscribeAsync and UnsubscribeAsync, and runs until cancellationToken
  // fires. Hands receive each message published to a topic this server subscribes to; the message is valid only until
  // receive returns. The messages one server publishes to a topic must arrive in the order it published them.
  // Reconnecting after a failure, with the subscriptions, is the backplane's work: ending before cancellationToken
  // fires fails the app's host, as a failed BackgroundService does.
  Task ReceiveAsync(Action<ReadOnlyMemory<byte>> receive, CancellationToken cancellationToken);

  // Starts handing the topic's messages to receive. The hub subscribes to a topic at most once until it unsubscribes,
  // and retries a call that throws.
  ValueTask SubscribeAsync(string topic, CancellationToken cancellationToken);

  ValueTask UnsubscribeAsync(string topic, CancellationToken cancellationToken);

  // Delivers message to every server subscribed to topic, this one too if it is: the hub skips its own messages. It
  // may be called before ReceiveAsync. The message is valid only until the returned task completes.
  ValueTask PublishAsync(string topic, ReadOnlyMemory<byte> message, CancellationToken cancellationToken);
}
