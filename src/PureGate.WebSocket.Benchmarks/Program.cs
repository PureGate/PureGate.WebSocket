using System.Diagnostics;
using System.Net.Http.Json;
using PureGate.WebSocket.Benchmarks;

// PureGate.WebSocket against SignalR (docs/PLAN-SERVER.md, Benchmarks). Starts the server as a child process (this
// program with --server), then runs each scenario against every system and prints the median of several runs.
//
//   dotnet run -c Release --project src/PureGate.WebSocket.Benchmarks -- [--scenario echo] [--runs 3]
//     [--seconds 2] [--steady 1000] [--fanout 10000]
//
// Tiered compilation is off (TC=0) in both processes, and nothing warms up first.
if (args.Contains("--server"))
{
  await BenchServer.RunAsync();
  return;
}

var settings = Settings.Parse(args);
using var server = await ServerProcess.StartAsync();
var systems = new BenchSystem[]
{
  new("PureGate.WebSocket", () => PureGateClient.ConnectAsync(new Uri(server.WebSocketBase, "/ws"))),
  new("SignalR JSON", () => SignalRClient.ConnectAsync(server.HttpBase, messagePack: false)),
  new("SignalR MessagePack", () => SignalRClient.ConnectAsync(server.HttpBase, messagePack: true)),
};

var scenarios = new Scenarios(server, settings);
var total = Stopwatch.StartNew();
foreach (var (name, title, run) in scenarios.All.Where(scenario => settings.Scenarios.Count == 0 || settings.Scenarios.Contains(scenario.Name)))
{
  Console.WriteLine();
  Console.WriteLine($"## {title}");
  Console.WriteLine();
  var rows = new List<(string System, Dictionary<string, double> Metrics)>();
  foreach (var system in systems)
  {
    var runs = new List<Dictionary<string, double>>();
    for (var i = 0; i < settings.Runs; i++)
      runs.Add(await run(system));

    rows.Add((system.Name, Report.Median(runs)));
  }

  Report.Print(rows);
}

Console.WriteLine();
Console.WriteLine($"Finished in {total.Elapsed.TotalSeconds:F0} s.");

namespace PureGate.WebSocket.Benchmarks
{
  sealed record BenchSystem(string Name, Func<Task<IBenchClient>> Connect);

  sealed record Settings(HashSet<string> Scenarios, int Runs, double Seconds, int Steady, int Fanout)
  {
    public static Settings Parse(string[] args)
    {
      var scenarios = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
      int runs = 3, steady = 1000, fanout = 10_000;
      var seconds = 2.0;
      for (var i = 0; i + 1 < args.Length; i += 2)
      {
        switch (args[i])
        {
          case "--scenario":
            scenarios.Add(args[i + 1]);
            break;
          case "--runs":
            runs = int.Parse(args[i + 1]);
            break;
          case "--seconds":
            seconds = double.Parse(args[i + 1], System.Globalization.CultureInfo.InvariantCulture);
            break;
          case "--steady":
            steady = int.Parse(args[i + 1]);
            break;
          case "--fanout":
            fanout = int.Parse(args[i + 1]);
            break;
          default:
            throw new ArgumentException($"Unknown option {args[i]}.");
        }
      }

      return new Settings(scenarios, runs, seconds, steady, fanout);
    }
  }

  // The server, in its own process, and its measurements.
  sealed class ServerProcess : IDisposable
  {
    readonly Process Process;
    readonly HttpClient Http;

    ServerProcess(Process process, Uri httpBase)
    {
      Process = process;
      HttpBase = httpBase;
      WebSocketBase = new Uri($"ws{httpBase.AbsoluteUri["http".Length..]}");
      Http = new HttpClient { BaseAddress = httpBase };
    }

    public Uri HttpBase { get; }

    public Uri WebSocketBase { get; }

    public static async Task<ServerProcess> StartAsync()
    {
      var self = Environment.ProcessPath;
      var arguments = Path.GetFileNameWithoutExtension(self) == "dotnet"
        ? $"\"{typeof(ServerProcess).Assembly.Location}\" --server"
        : "--server";
      var process = Process.Start(new ProcessStartInfo(self, arguments) { RedirectStandardOutput = true, UseShellExecute = false })
        ?? throw new InvalidOperationException("The server did not start.");

      while (await process.StandardOutput.ReadLineAsync() is { } line)
      {
        if (line.StartsWith("BENCH_SERVER ", StringComparison.Ordinal))
          return new ServerProcess(process, new Uri(line["BENCH_SERVER ".Length..]));
      }

      throw new InvalidOperationException("The server exited before it listened.");
    }

    // collect runs full garbage collections first, for the memory still in use.
    public async Task<Stats> StatsAsync(bool collect = false) =>
      await Http.GetFromJsonAsync($"/stats?collect={collect}", BenchJson.Default.Stats);

    public void Dispose()
    {
      Http.Dispose();
      if (!Process.HasExited)
        Process.Kill(entireProcessTree: true);
      Process.Dispose();
    }
  }

  static class Report
  {
    public static double Percentile(List<double> sorted, double percentile) =>
      sorted.Count == 0 ? double.NaN : sorted[(int)Math.Round(percentile * (sorted.Count - 1))];

    public static Dictionary<string, double> Median(List<Dictionary<string, double>> runs) =>
      runs[0].Keys.ToDictionary(key => key, key =>
      {
        var values = runs.Select(run => run[key]).Order().ToArray();
        return values[values.Length / 2];
      });

    // A Markdown table padded to its widest cells, so it also reads in a console. Each column has one format: whole
    // numbers once it reaches 100, 3 decimals below that, so the digits line up.
    public static void Print(List<(string System, Dictionary<string, double> Metrics)> rows)
    {
      var columns = rows[0].Metrics.Keys.ToArray();
      var decimals = columns.Select(column => rows.Max(row => double.IsNaN(row.Metrics[column]) ? 0 : row.Metrics[column]) >= 100 ? 0 : 3).ToArray();
      var cells = rows.Select(row => columns.Select((column, i) => Format(row.Metrics[column], decimals[i])).ToArray()).ToArray();
      var first = Math.Max("System".Length, rows.Max(row => row.System.Length));
      var widths = columns.Select((column, i) => Math.Max(column.Length, cells.Max(row => row[i].Length))).ToArray();

      Console.WriteLine($"| {"System".PadRight(first)} | {string.Join(" | ", columns.Select((column, i) => column.PadLeft(widths[i])))} |");
      Console.WriteLine($"|{new string('-', first + 2)}|{string.Join("|", widths.Select(width => new string('-', width + 1) + ":"))}|");
      for (var r = 0; r < rows.Count; r++)
        Console.WriteLine($"| {rows[r].System.PadRight(first)} | {string.Join(" | ", cells[r].Select((cell, i) => cell.PadLeft(widths[i])))} |");
    }

    static string Format(double value, int decimals) =>
      double.IsNaN(value) ? "-" : value.ToString($"N{decimals}", System.Globalization.CultureInfo.InvariantCulture);
  }
}
