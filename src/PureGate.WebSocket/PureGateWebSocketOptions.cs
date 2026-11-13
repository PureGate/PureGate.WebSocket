using System.Security.Claims;
using System.Text.Json;

namespace PureGate.WebSocket;

// How a PureGate.WebSocket endpoint behaves. The defaults are sized for untrusted clients on the public internet.
// services.AddPureGateWebSocket sets them for every endpoint, and MapPureGateWebSocket can override them for one.
public sealed class PureGateWebSocketOptions
{
  // Reads and writes JSON payloads. camelCase by default, as TypeScript expects. For Native AOT, add a
  // source-generated context to TypeInfoResolverChain.
  public JsonSerializerOptions SerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);

  // The largest message accepted from a client, in bytes, at least 16 KiB. Larger data belongs in a stream.
  public int MaxMessageBytes { get; set; } = 64 * 1024;

  // How many calls one client may have running at once, at least 4. More wait in the browser.
  public int MaxConcurrentCalls { get; set; } = 32;

  // How many events from one client may wait for a handler, at least 16.
  public int EventWindow { get; set; } = 256;

  // How long a new connection has to send its Hello.
  public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(10);

  // How often the browser checks that the connection is alive. Sent to it in Welcome.
  public TimeSpan Heartbeat { get; set; } = TimeSpan.FromSeconds(15);

  // WebSocket ping frames from the server, which browsers answer on their own. A browser that stops answering
  // for KeepAliveTimeout is disconnected.
  public TimeSpan KeepAliveInterval { get; set; } = TimeSpan.FromSeconds(15);
  public TimeSpan KeepAliveTimeout { get; set; } = TimeSpan.FromSeconds(30);

  // Bytes that may wait to be sent to one client. Past it, SlowConsumer applies.
  public long SendQueueLimitBytes { get; set; } = 1024 * 1024;

  public SlowConsumerPolicy SlowConsumer { get; set; } = SlowConsumerPolicy.Close;

  // Origins allowed to connect, such as "https://app.example.com", or "*" for any. When empty, only the endpoint's
  // own origin is allowed, and clients that send no Origin header (not browsers). This stops other sites from
  // using a visitor's cookies to connect.
  public IList<string> AllowedOrigins { get; set; } = [];

  // The id hub.User(id) finds a user's connections by. By default, the NameIdentifier claim.
  public Func<ClaimsPrincipal, string> UserId { get; set; } = static user => user?.FindFirstValue(ClaimTypes.NameIdentifier);

  // How long a server → browser call waits for its answer, unless the call says otherwise.
  public TimeSpan InvokeTimeout { get; set; } = TimeSpan.FromSeconds(30);

  // Whether a failing handler's exception message goes to the browser. Off: browsers get a generic message, and
  // the exception goes to the log.
  public bool IncludeExceptionDetails { get; set; }

  public EventOrdering EventOrdering { get; set; } = EventOrdering.Sequential;

  // permessage-deflate. Off by default: when a secret and text an attacker controls share a compressed message,
  // the message's size can give the secret away.
  public bool EnableCompression { get; set; }

  // How the contract describes .NET types (see WebSocketEndpointBuilder.MapContract).
  public ContractOptions Contract { get; set; } = new();

  // What happens when a browser's contract.ts is not the server's.
  public ContractMismatchPolicy ContractMismatch { get; set; } = ContractMismatchPolicy.Warn;

  internal PureGateWebSocketOptions Clone() => new()
  {
    SerializerOptions = new JsonSerializerOptions(SerializerOptions),
    MaxMessageBytes = MaxMessageBytes,
    MaxConcurrentCalls = MaxConcurrentCalls,
    EventWindow = EventWindow,
    HandshakeTimeout = HandshakeTimeout,
    Heartbeat = Heartbeat,
    KeepAliveInterval = KeepAliveInterval,
    KeepAliveTimeout = KeepAliveTimeout,
    SendQueueLimitBytes = SendQueueLimitBytes,
    SlowConsumer = SlowConsumer,
    AllowedOrigins = [.. AllowedOrigins],
    UserId = UserId,
    InvokeTimeout = InvokeTimeout,
    IncludeExceptionDetails = IncludeExceptionDetails,
    EventOrdering = EventOrdering,
    EnableCompression = EnableCompression,
    Contract = Contract.Clone(),
    ContractMismatch = ContractMismatch,
  };

  internal void Validate()
  {
    ArgumentNullException.ThrowIfNull(SerializerOptions, nameof(SerializerOptions));
    ArgumentNullException.ThrowIfNull(AllowedOrigins, nameof(AllowedOrigins));
    ArgumentNullException.ThrowIfNull(UserId, nameof(UserId));
    ArgumentNullException.ThrowIfNull(Contract, nameof(Contract));
    ArgumentOutOfRangeException.ThrowIfLessThan(MaxMessageBytes, 16 * 1024, nameof(MaxMessageBytes));
    ArgumentOutOfRangeException.ThrowIfLessThan(MaxConcurrentCalls, 4, nameof(MaxConcurrentCalls));
    ArgumentOutOfRangeException.ThrowIfLessThan(EventWindow, 16, nameof(EventWindow));
    ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(HandshakeTimeout, TimeSpan.Zero, nameof(HandshakeTimeout));
    ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(Heartbeat, TimeSpan.Zero, nameof(Heartbeat));
    ArgumentOutOfRangeException.ThrowIfLessThan(SendQueueLimitBytes, MaxMessageBytes, nameof(SendQueueLimitBytes));
    if (InvokeTimeout != Timeout.InfiniteTimeSpan)
      ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(InvokeTimeout, TimeSpan.Zero, nameof(InvokeTimeout));
  }
}

// What happens to a client that does not read its messages as fast as the server sends them.
public enum SlowConsumerPolicy
{
  // Close the connection with 4003 (slow_consumer).
  Close,

  // Drop events for it until it catches up. Responses still close the connection, since they cannot be dropped.
  DropEvents,
}

// How the events from one connection are handled.
public enum EventOrdering
{
  // One at a time, in the order they were sent.
  Sequential,

  // As they arrive, at the same time.
  Concurrent,
}
