using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PureGate.WebSocket.TestServer;

public sealed record Note(string Text, int Number);

public sealed record AddRequest(int A, int B);

public sealed record Question(string Text);

public sealed record Click(int X, int Y);

public sealed record GroupNote(string Group, Note Note);

public sealed record ErrorDetails(int Id);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(Note))]
[JsonSerializable(typeof(AddRequest))]
[JsonSerializable(typeof(Question))]
[JsonSerializable(typeof(Click))]
[JsonSerializable(typeof(Click[]))]
[JsonSerializable(typeof(GroupNote))]
[JsonSerializable(typeof(ErrorDetails))]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(bool))]
[JsonSerializable(typeof(int[]))]
public sealed partial class TestJson : JsonSerializerContext;

// The operations the tests call. Everything is per connection, so tests running at the same time don't meet.
public static class TestApi
{
  public static readonly ServerEvent<string> Received = new("events.received");
  public static readonly ServerEvent<Note> Broadcast = new("events.broadcast");
  public static readonly ClientRequest<Question, bool> Confirm = new("ui.confirm");
  public static readonly ClientRequest<int, int> Slow = new("ui.slow");

  public static void Configure(PureGateWebSocketOptions options)
  {
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, TestJson.Default);

    // The browser tests' page comes from Vite's server, a different origin.
    options.AllowedOrigins = ["*"];
    options.MaxConcurrentCalls = 8;
    options.EventWindow = 16;
    options.MaxStreams = 4;
    options.UploadWindow = 4;
  }

  public static void Map(WebSocketEndpointBuilder ws)
  {
    ws.MapEvent(Received).MapEvent(Broadcast).MapClientRequest(Confirm).MapClientRequest(Slow);

    var echo = ws.MapGroup("echo");
    echo.MapRequest<Note, Note>("note", static (note, call, cancellationToken) => ValueTask.FromResult(note));
    echo.MapRequest<string, string>("text", static (text, call, cancellationToken) => ValueTask.FromResult(text));
    echo.MapRequest<byte[], byte[]>("bytes", static (bytes, call, cancellationToken) => ValueTask.FromResult(bytes));
    echo.MapRequest<int[], int[]>("numbers", static (numbers, call, cancellationToken) => ValueTask.FromResult(numbers));
    echo.MapRequest<string>("trace", static (call, cancellationToken) => ValueTask.FromResult(call.TraceParent ?? ""));
    echo.MapRequest<int, string>("repeat", static (length, call, cancellationToken) => ValueTask.FromResult(new string('x', length)));

    ws.MapRequest<AddRequest, int>("math.add", static (add, call, cancellationToken) => ValueTask.FromResult(add.A + add.B));
    ws.MapRequest<long>("time.now", static (call, cancellationToken) => ValueTask.FromResult(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
    ws.MapRequest("misc.noop", static (call, cancellationToken) => ValueTask.CompletedTask);
    ws.MapRequest<string>("misc.consume", static (text, call, cancellationToken) => ValueTask.CompletedTask);

    var calls = ws.MapGroup("calls");
    calls.MapRequest<int, string>("delay", static async (milliseconds, call, cancellationToken) =>
    {
      try
      {
        await Task.Delay(milliseconds, cancellationToken);
        return "done";
      }
      catch (OperationCanceledException)
      {
        Interlocked.Increment(ref State(call).Cancelled);
        throw;
      }
    });
    calls.MapRequest<int>("cancelled", static (call, cancellationToken) => ValueTask.FromResult(State(call).Cancelled));
    calls.MapRequest<int>("block", static async (call, cancellationToken) =>
    {
      var state = State(call);
      var running = Interlocked.Increment(ref state.Blocked);
      lock (state)
        state.Peak = Math.Max(state.Peak, running);
      try
      {
        await state.Gate.Task.WaitAsync(cancellationToken);
        return running;
      }
      finally
      {
        Interlocked.Decrement(ref state.Blocked);
      }
    });
    calls.MapRequest("release", static (call, cancellationToken) =>
    {
      var state = State(call);
      var gate = state.Gate;
      state.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      gate.TrySetResult();
      return ValueTask.CompletedTask;
    });
    calls.MapRequest<int>("peak", static (call, cancellationToken) => ValueTask.FromResult(State(call).Peak));

    var fail = ws.MapGroup("fail");
    fail.MapRequest<int, string>("notFound", static (id, call, cancellationToken) =>
      throw new CallException("not_found", $"Item {id} does not exist.", new ErrorDetails(id)));
    fail.MapRequest<string>("crash", static (call, cancellationToken) =>
      throw new InvalidOperationException("A secret the browser must not see."));

    var events = ws.MapGroup("events");
    events.MapRequest<string>("echo", static (text, call, cancellationToken) => call.Connection.SendAsync(Received, text, cancellationToken));
    events.MapRequest<Note>("broadcast", static (note, call, cancellationToken) =>
      call.Services.GetRequiredService<IWebSocketHub>().All.SendAsync(Broadcast, note, cancellationToken));
    events.MapRequest<string>("join", static (group, call, cancellationToken) =>
    {
      call.Connection.Groups.Add(group);
      return ValueTask.CompletedTask;
    });
    events.MapRequest<GroupNote>("toGroup", static (groupNote, call, cancellationToken) =>
      call.Services.GetRequiredService<IWebSocketHub>().Group(groupNote.Group).SendAsync(Broadcast, groupNote.Note, cancellationToken));

    var telemetry = ws.MapGroup("telemetry");
    telemetry.MapClientEvent<Click>("click", static (click, call, cancellationToken) =>
    {
      var state = State(call);
      lock (state)
      {
        state.Clicks.Add(click);
        Monitor.PulseAll(state);
      }

      return ValueTask.CompletedTask;
    });
    telemetry.MapRequest<int, Click[]>("waitFor", static async (count, call, cancellationToken) =>
    {
      var state = State(call);
      var deadline = DateTime.UtcNow.AddSeconds(10);
      while (DateTime.UtcNow < deadline)
      {
        lock (state)
        {
          if (state.Clicks.Count >= count)
            return [.. state.Clicks];
        }

        await Task.Delay(10, cancellationToken);
      }

      lock (state)
        return [.. state.Clicks];
    });

    MapStreams(ws.MapGroup("feed"));
    MapUploads(ws.MapGroup("store"));

    var browser = ws.MapGroup("browser");
    browser.MapRequest<string, string>("confirm", static async (text, call, cancellationToken) =>
    {
      try
      {
        return (await call.Connection.InvokeAsync(Confirm, new Question(text), cancellationToken)).ToString();
      }
      catch (CallException exception)
      {
        return $"{exception.Code}: {exception.Message}";
      }
    });
    browser.MapRequest<int, string>("slow", static async (milliseconds, call, cancellationToken) =>
    {
      try
      {
        return (await call.Connection.InvokeAsync(Slow, milliseconds, TimeSpan.FromMilliseconds(200), cancellationToken)).ToString();
      }
      catch (CallException exception)
      {
        return exception.Code;
      }
    });
  }

  // Streams the browser reads: conn.streams.feed.*.
  static void MapStreams(WebSocketGroup feed)
  {
    feed.MapStream<int, int>("count", static (count, call, cancellationToken) => Count(count, cancellationToken));
    feed.MapStream<Note>("notes", static (call, cancellationToken) => Notes(State(call), cancellationToken));
    feed.MapStream<int, byte[]>("bytes", static (count, call, cancellationToken) => Bytes(count, cancellationToken));
    feed.MapStream<int, int>("fail", static (after, call, cancellationToken) => Fail(after, cancellationToken));
    feed.MapStream<int>("crash", static (call, cancellationToken) => Crash(cancellationToken));
    feed.MapStream<int, string>("huge", static (length, call, cancellationToken) => Huge(length, cancellationToken));
    feed.MapRequest<int>("produced", static (call, cancellationToken) => ValueTask.FromResult(Volatile.Read(ref State(call).Produced)));
    feed.MapRequest<int>("cancelled", static (call, cancellationToken) => ValueTask.FromResult(Volatile.Read(ref State(call).StreamsCancelled)));
  }

  static async IAsyncEnumerable<int> Count(int count, [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    for (var i = 1; i <= count; i++)
    {
      await Task.Yield();
      yield return i;
    }
  }

  // Never ends: the browser stops it. Counts what it produced, which credit holds back.
  static async IAsyncEnumerable<Note> Notes(ConnectionState state, [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    try
    {
      for (var i = 1; ; i++)
      {
        await Task.Yield();
        cancellationToken.ThrowIfCancellationRequested();
        Interlocked.Increment(ref state.Produced);
        yield return new Note("tick", i);
      }
    }
    finally
    {
      if (cancellationToken.IsCancellationRequested)
        Interlocked.Increment(ref state.StreamsCancelled);
    }
  }

  static async IAsyncEnumerable<byte[]> Bytes(int count, [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    for (var i = 0; i < count; i++)
    {
      await Task.Yield();
      yield return [(byte)i, (byte)i, (byte)i];
    }
  }

  static async IAsyncEnumerable<int> Fail(int after, [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    for (var i = 1; i <= after; i++)
    {
      await Task.Yield();
      yield return i;
    }

    throw new CallException("broken", "The feed broke.", new ErrorDetails(after));
  }

  static async IAsyncEnumerable<int> Crash([EnumeratorCancellation] CancellationToken cancellationToken)
  {
    await Task.Yield();
    throw new InvalidOperationException("A secret the browser must not see.");
#pragma warning disable CS0162 // An iterator needs a yield, even one it never reaches.
    yield break;
#pragma warning restore CS0162
  }

  static async IAsyncEnumerable<string> Huge(int length, [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    await Task.Yield();
    yield return "first";
    yield return new string('x', length);
  }

  // Streams the browser sends: conn.uploads.store.*.
  static void MapUploads(WebSocketGroup store)
  {
    store.MapUpload<int, int>("sum", static async (items, call, cancellationToken) =>
    {
      var sum = 0;
      await foreach (var item in items)
        sum += item;
      return sum;
    });
    store.MapUpload<string, string, string>("join", static async (separator, items, call, cancellationToken) =>
    {
      var parts = new List<string>();
      await foreach (var item in items)
        parts.Add(item);
      return string.Join(separator, parts);
    });
    store.MapUpload<byte[], int>("size", static async (items, call, cancellationToken) =>
    {
      var size = 0;
      await foreach (var item in items)
        size += item.Length;
      return size;
    });

    // Answers before the browser has sent everything.
    store.MapUpload<int, int>("first", static async (items, call, cancellationToken) =>
    {
      await foreach (var item in items)
        return item;
      return -1;
    });
    store.MapUpload<int, int, int>("fail", static async (after, items, call, cancellationToken) =>
    {
      var read = 0;
      await foreach (var item in items)
      {
        if (++read == after)
          throw new CallException("rejected", $"Item {item} was refused.", new ErrorDetails(item));
      }

      return read;
    });

    // Reads until the browser gives up.
    store.MapUpload<int, int>("hold", static async (items, call, cancellationToken) =>
    {
      try
      {
        var read = 0;
        await foreach (var item in items)
          read++;
        await Task.Delay(Timeout.Infinite, cancellationToken);
        return read;
      }
      catch (OperationCanceledException)
      {
        Interlocked.Increment(ref State(call).UploadsCancelled);
        throw;
      }
    });
    store.MapUpload<string, Note>("count", static async (tag, items, call, cancellationToken) =>
    {
      var count = 0;
      await foreach (var note in items)
        count++;
      State(call).Counted = $"{tag}:{count}";
    });
    store.MapUpload<string>("drain", static async (items, call, cancellationToken) =>
    {
      await foreach (var item in items)
        State(call).Counted = item;
    });
    store.MapRequest<string>("counted", static (call, cancellationToken) => ValueTask.FromResult(State(call).Counted ?? ""));
    store.MapRequest<int>("cancelled", static (call, cancellationToken) => ValueTask.FromResult(Volatile.Read(ref State(call).UploadsCancelled)));
  }

  static ConnectionState State(WebSocketCall call) =>
    (ConnectionState)((ConcurrentDictionary<object, object>)call.Connection.Items).GetOrAdd(typeof(ConnectionState), static _ => new ConnectionState());

  sealed class ConnectionState
  {
    public int Cancelled;
    public int Blocked;
    public int Peak;
    public TaskCompletionSource Gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public readonly List<Click> Clicks = [];
    public int Produced;
    public int StreamsCancelled;
    public int UploadsCancelled;
    public string Counted;
  }
}
