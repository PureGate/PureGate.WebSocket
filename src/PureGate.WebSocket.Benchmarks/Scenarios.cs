using System.Diagnostics;

namespace PureGate.WebSocket.Benchmarks;

// The scenarios of docs/PLAN-SERVER.md, Benchmarks. Each returns its metrics for one run against one system, and
// lasts a few seconds, so a full run of every scenario stays within a few minutes.
sealed class Scenarios(ServerProcess server, Settings settings)
{
  // A 1 KB payload.
  static readonly string Kilobyte = new('x', 1024);

  public (string Name, string Title, Func<BenchSystem, Task<Dictionary<string, double>>> Run)[] All =>
  [
    ("echo", "1 KB echo, sequential, one connection", EchoAsync),
    ("echo64", "1 KB echo, 64 callers on one connection", ConcurrentEchoAsync),
    ("steady", $"{settings.Steady:N0} connections making steady calls", SteadyAsync),
    ("fanout", $"{settings.Fanout:N0} idle connections, then one broadcast to all", FanoutAsync),
  ];

  TimeSpan Duration => TimeSpan.FromSeconds(settings.Seconds);

  // Calls/s, latency, and the server's allocations per call.
  async Task<Dictionary<string, double>> EchoAsync(BenchSystem system)
  {
    await using var client = await system.Connect();
    var latencies = new List<double>();
    var before = await server.StatsAsync();
    var elapsed = Stopwatch.StartNew();
    while (elapsed.Elapsed < Duration)
    {
      var started = Stopwatch.GetTimestamp();
      await client.EchoAsync(Kilobyte);
      latencies.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
    }

    var seconds = elapsed.Elapsed.TotalSeconds;
    var after = await server.StatsAsync();
    latencies.Sort();
    return new()
    {
      ["calls/s"] = latencies.Count / seconds,
      ["p50 ms"] = Report.Percentile(latencies, 0.5),
      ["p99 ms"] = Report.Percentile(latencies, 0.99),
      ["server bytes/call"] = (after.Allocated - before.Allocated) / (double)latencies.Count,
    };
  }

  async Task<Dictionary<string, double>> ConcurrentEchoAsync(BenchSystem system)
  {
    await using var client = await system.Connect();
    var elapsed = Stopwatch.StartNew();
    var callers = Enumerable.Range(0, 64).Select(async _ =>
    {
      var latencies = new List<double>();
      while (elapsed.Elapsed < Duration)
      {
        var started = Stopwatch.GetTimestamp();
        await client.EchoAsync(Kilobyte);
        latencies.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
      }

      return latencies;
    });

    var all = (await Task.WhenAll(callers)).SelectMany(latencies => latencies).ToList();
    var seconds = elapsed.Elapsed.TotalSeconds;
    all.Sort();
    return new()
    {
      ["calls/s"] = all.Count / seconds,
      ["p50 ms"] = Report.Percentile(all, 0.5),
      ["p99 ms"] = Report.Percentile(all, 0.99),
    };
  }

  // Every connection calls, waits 10 ms, and calls again: latency under load, and the server's CPU.
  async Task<Dictionary<string, double>> SteadyAsync(BenchSystem system)
  {
    var clients = await ConnectAsync(system, settings.Steady);
    try
    {
      var before = await server.StatsAsync();
      var elapsed = Stopwatch.StartNew();
      var callers = clients.Select(async client =>
      {
        var latencies = new List<double>();
        while (elapsed.Elapsed < Duration)
        {
          var started = Stopwatch.GetTimestamp();
          await client.EchoAsync(Kilobyte);
          latencies.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
          await Task.Delay(10);
        }

        return latencies;
      });

      var all = (await Task.WhenAll(callers)).SelectMany(latencies => latencies).ToList();
      var seconds = elapsed.Elapsed.TotalSeconds;
      var after = await server.StatsAsync();
      all.Sort();
      return new()
      {
        ["calls/s"] = all.Count / seconds,
        ["p50 ms"] = Report.Percentile(all, 0.5),
        ["p99 ms"] = Report.Percentile(all, 0.99),
        ["server CPU %"] = (after.CpuMilliseconds - before.CpuMilliseconds) / (seconds * 1000 * Environment.ProcessorCount) * 100,
      };
    }
    finally
    {
      await DisposeAllAsync(clients);
    }
  }

  // The server's memory per idle connection, then how long one broadcast takes to reach the last of them.
  async Task<Dictionary<string, double>> FanoutAsync(BenchSystem system)
  {
    var before = await server.StatsAsync(collect: true);
    var clients = await ConnectAsync(system, settings.Fanout);
    try
    {
      var idle = await server.StatsAsync(collect: true);
      var remaining = clients.Count;
      var everyone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      foreach (var client in clients)
      {
        var counted = 0;
        client.OnTick = _ =>
        {
          if (Interlocked.Exchange(ref counted, 1) == 0 && Interlocked.Decrement(ref remaining) == 0)
            everyone.TrySetResult();
        };
      }

      var started = Stopwatch.GetTimestamp();
      await clients[0].BroadcastAsync("tick");
      await everyone.Task.WaitAsync(TimeSpan.FromMinutes(1));
      return new()
      {
        ["server KB/connection"] = (idle.Heap - before.Heap) / 1024.0 / clients.Count,
        ["broadcast ms"] = Stopwatch.GetElapsedTime(started).TotalMilliseconds,
      };
    }
    finally
    {
      await DisposeAllAsync(clients);
    }
  }

  static async Task<List<IBenchClient>> ConnectAsync(BenchSystem system, int count)
  {
    var clients = new List<IBenchClient>(count);
    for (var start = 0; start < count; start += 100)
      clients.AddRange(await Task.WhenAll(Enumerable.Range(0, Math.Min(100, count - start)).Select(_ => system.Connect())));

    return clients;
  }

  static async Task DisposeAllAsync(List<IBenchClient> clients)
  {
    for (var start = 0; start < clients.Count; start += 500)
      await Task.WhenAll(clients.Skip(start).Take(500).Select(client => client.DisposeAsync().AsTask()));
  }
}
