using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.SignalR;

namespace PureGate.WebSocket.Benchmarks;

// The server under test, in its own process so its memory, allocations and CPU are its own: PureGate.WebSocket at
// /ws, SignalR at /signalr with the JSON and MessagePack protocols, and /stats for the measurements.
static class BenchServer
{
  public static readonly ServerEvent<string> Tick = new("bench.tick");

  public static async Task RunAsync()
  {
    var builder = WebApplication.CreateSlimBuilder();
    builder.WebHost.UseUrls("http://127.0.0.1:0");
    builder.Logging.ClearProviders();
    builder.Services.AddPureGateWebSocket(static options =>
    {
      options.SerializerOptions.TypeInfoResolverChain.Insert(0, BenchJson.Default);
      options.MaxConcurrentCalls = 128;
    });
    builder.Services.AddSignalR().AddMessagePackProtocol();

    var app = builder.Build();
    app.UseWebSockets();

    var ws = app.MapPureGateWebSocket("/ws");
    ws.MapEvent(Tick);
    ws.MapRequest<string, string>("bench.echo", static (text, _, _) => ValueTask.FromResult(text));
    ws.MapRequest<string>("bench.broadcast", static (text, call, cancellationToken) =>
      call.Services.GetRequiredService<IWebSocketHub>().All.SendAsync(Tick, text, cancellationToken));

    app.MapHub<BenchHub>("/signalr");
    app.MapGet("/stats", static (bool collect) =>
    {
      if (collect)
      {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
      }

      var process = Process.GetCurrentProcess();
      return new Stats(
        GC.GetTotalAllocatedBytes(precise: true),
        GC.GetTotalMemory(forceFullCollection: false),
        process.TotalProcessorTime.TotalMilliseconds,
        process.WorkingSet64);
    });

    await app.StartAsync();
    Console.WriteLine($"BENCH_SERVER {app.Urls.First()}");
    await app.WaitForShutdownAsync();
  }
}

public sealed class BenchHub : Hub
{
  public string Echo(string text) => text;

  public Task Broadcast(string text) => Clients.All.SendAsync("tick", text);
}

public sealed record Stats(long Allocated, long Heap, double CpuMilliseconds, long WorkingSet);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(Stats))]
public sealed partial class BenchJson : JsonSerializerContext;
