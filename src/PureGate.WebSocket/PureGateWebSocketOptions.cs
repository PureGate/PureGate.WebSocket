using System.Security.Claims;
using System.Text.Json;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.Http;

namespace PureGate.WebSocket;

// How a PureGate.WebSocket endpoint behaves. The defaults are sized for untrusted clients on the public internet.
// services.AddPureGateWebSocket sets them for every endpoint, and MapPureGateWebSocket can override them for one.
public sealed class PureGateWebSocketOptions
{
  // Reads and writes JSON payloads. camelCase by default, as TypeScript expects. For Native AOT, add a
  // source-generated context to TypeInfoResolverChain.
  public JsonSerializerOptions SerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);

  // Payload formats besides JSON that a browser may ask for, such as MessagePack (see IPayloadFormat). JSON is always
  // there, for browsers that ask for none.
  public IList<IPayloadFormat> PayloadFormats { get; set; } = [];

  // The largest message accepted from a client, in bytes, at least 16 KiB. Larger data belongs in a stream.
  public int MaxMessageBytes { get; set; } = 64 * 1024;

  // How many calls one client may have running at once, at least 4. More wait in the browser.
  public int MaxConcurrentCalls { get; set; } = 32;

  // How many events from one client may wait for a handler, at least 16.
  public int EventWindow { get; set; } = 256;

  // How many streams one client may have open at once, downloads and uploads together, at least 1.
  public int MaxStreams { get; set; } = 8;

  // How many items of an upload may be on the way to its handler, at least 2. The server grants this much credit
  // when the handler starts, and more each time the handler has read half of it.
  public int UploadWindow { get; set; } = 16;

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

  // How far the page may fall behind, in frames it has not acknowledged, before SlowConsumer applies.
  public int MaxUnacknowledgedFrames { get; set; } = 4096;

  public SlowConsumerPolicy SlowConsumer { get; set; } = SlowConsumerPolicy.Close;

  // How long a dropped session stays resumable: its handlers keep running, and what it sends waits for the browser.
  // TimeSpan.Zero turns resume off.
  public TimeSpan ResumeWindow { get; set; } = TimeSpan.FromSeconds(30);

  // Bytes of unacknowledged frames kept for a resume. Past it, the session can no longer resume.
  public long ReplayBufferBytes { get; set; } = 1024 * 1024;

  // This server instance's routing id, sent to browsers so a load balancer can route their resume back here. Not a
  // secret. None by default.
  public string ResumeRoute { get; set; }

  // How long a stopping server lets exchanges in progress finish after GoAway.
  public TimeSpan DrainTimeout { get; set; } = TimeSpan.FromSeconds(10);

  // Origins allowed to connect, such as "https://app.example.com", or "*" for any. When empty, only the endpoint's
  // own origin is allowed, and clients that send no Origin header (not browsers). This stops other sites from
  // using a visitor's cookies to connect.
  public IList<string> AllowedOrigins { get; set; } = [];

  // Validates the token a browser sends in Hello, and again when it refreshes it, for apps that cannot use cookies. Set,
  // the endpoint authenticates by token only: a Hello without one closes with 4001, and the connection's user is the
  // result's. Unset, the connection's user is the upgrade request's, from the usual ASP.NET Core schemes. Tokens never
  // travel in the URL, where proxies and logs would keep them.
  public Func<string, HttpContext, CancellationToken, ValueTask<WebSocketAuthenticationResult>> Authenticate { get; set; }

  // A rate limiter for each connection, created when it starts. Every call, stream and event takes a permit, held
  // until its handler finishes. Past the limit, calls and streams fail with overloaded, and events are dropped.
  public Func<WebSocketConnection, RateLimiter> RateLimiter { get; set; }

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
    PayloadFormats = [.. PayloadFormats],
    MaxMessageBytes = MaxMessageBytes,
    MaxConcurrentCalls = MaxConcurrentCalls,
    EventWindow = EventWindow,
    MaxStreams = MaxStreams,
    UploadWindow = UploadWindow,
    HandshakeTimeout = HandshakeTimeout,
    Heartbeat = Heartbeat,
    KeepAliveInterval = KeepAliveInterval,
    KeepAliveTimeout = KeepAliveTimeout,
    SendQueueLimitBytes = SendQueueLimitBytes,
    MaxUnacknowledgedFrames = MaxUnacknowledgedFrames,
    SlowConsumer = SlowConsumer,
    ResumeWindow = ResumeWindow,
    ReplayBufferBytes = ReplayBufferBytes,
    ResumeRoute = ResumeRoute,
    DrainTimeout = DrainTimeout,
    AllowedOrigins = [.. AllowedOrigins],
    Authenticate = Authenticate,
    RateLimiter = RateLimiter,
    UserId = UserId,
    InvokeTimeout = InvokeTimeout,
    IncludeExceptionDetails = IncludeExceptionDetails,
    EventOrdering = EventOrdering,
    EnableCompression = EnableCompression,
    Contract = Contract.Clone(),
    ContractMismatch = ContractMismatch,
  };

  static bool IsFormatName(string name) =>
    name is { Length: > 0 and <= 32 } && name.AsSpan().IndexOfAnyExcept("abcdefghijklmnopqrstuvwxyz0123456789-.") < 0;

  internal void Validate()
  {
    ArgumentNullException.ThrowIfNull(SerializerOptions, nameof(SerializerOptions));
    ArgumentNullException.ThrowIfNull(PayloadFormats, nameof(PayloadFormats));
    ArgumentNullException.ThrowIfNull(AllowedOrigins, nameof(AllowedOrigins));
    ArgumentNullException.ThrowIfNull(UserId, nameof(UserId));
    ArgumentNullException.ThrowIfNull(Contract, nameof(Contract));
    ArgumentOutOfRangeException.ThrowIfLessThan(MaxMessageBytes, 16 * 1024, nameof(MaxMessageBytes));
    ArgumentOutOfRangeException.ThrowIfLessThan(MaxConcurrentCalls, 4, nameof(MaxConcurrentCalls));
    ArgumentOutOfRangeException.ThrowIfLessThan(EventWindow, 16, nameof(EventWindow));
    ArgumentOutOfRangeException.ThrowIfLessThan(MaxStreams, 1, nameof(MaxStreams));
    ArgumentOutOfRangeException.ThrowIfLessThan(UploadWindow, 2, nameof(UploadWindow));
    ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(HandshakeTimeout, TimeSpan.Zero, nameof(HandshakeTimeout));
    ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(Heartbeat, TimeSpan.Zero, nameof(Heartbeat));
    ArgumentOutOfRangeException.ThrowIfLessThan(SendQueueLimitBytes, MaxMessageBytes, nameof(SendQueueLimitBytes));
    ArgumentOutOfRangeException.ThrowIfLessThan(MaxUnacknowledgedFrames, 64, nameof(MaxUnacknowledgedFrames));
    ArgumentOutOfRangeException.ThrowIfLessThan(ResumeWindow, TimeSpan.Zero, nameof(ResumeWindow));
    ArgumentOutOfRangeException.ThrowIfLessThan(DrainTimeout, TimeSpan.Zero, nameof(DrainTimeout));
    if (ResumeWindow > TimeSpan.Zero)
      ArgumentOutOfRangeException.ThrowIfLessThan(ReplayBufferBytes, MaxMessageBytes, nameof(ReplayBufferBytes));
    if (InvokeTimeout != Timeout.InfiniteTimeSpan)
      ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(InvokeTimeout, TimeSpan.Zero, nameof(InvokeTimeout));

    var names = new HashSet<string>(StringComparer.Ordinal) { "json" };
    foreach (var format in PayloadFormats)
    {
      ArgumentNullException.ThrowIfNull(format, nameof(PayloadFormats));
      if (!IsFormatName(format.Name))
        throw new ArgumentException($"'{format.Name}' is not a payload format name: 1 to 32 lowercase letters, digits, '-' and '.'.", nameof(PayloadFormats));
      if (!names.Add(format.Name))
        throw new ArgumentException($"The payload format '{format.Name}' is there twice, or is JSON's own.", nameof(PayloadFormats));
    }
  }
}

// What happens to a client that does not read its messages as fast as the server sends them.
public enum SlowConsumerPolicy
{
  // Close the connection with 4003 (slow_consumer). The browser reconnects, and resumes when it can.
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
