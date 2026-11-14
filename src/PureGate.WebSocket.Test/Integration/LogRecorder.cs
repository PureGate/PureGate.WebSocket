using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace PureGate.WebSocket.Test.Integration;

// A logger provider that keeps each entry's event name and structured fields, for tests that check what was logged.
sealed class LogRecorder : ILoggerProvider, ILogger
{
  public sealed record Entry(string Event, Dictionary<string, object> Fields);

  public ConcurrentQueue<Entry> Entries { get; } = new();

  public ILogger CreateLogger(string categoryName) => this;

  public bool IsEnabled(LogLevel logLevel) => true;

  public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
  {
    var fields = new Dictionary<string, object>();
    if (state is IEnumerable<KeyValuePair<string, object>> pairs)
    {
      foreach (var pair in pairs)
        fields[pair.Key] = pair.Value;
    }

    Entries.Enqueue(new(eventId.Name, fields));
  }

  IDisposable ILogger.BeginScope<TState>(TState state) => null;

  public void Dispose()
  {
  }
}
