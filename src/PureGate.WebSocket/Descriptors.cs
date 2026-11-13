using System.Text;
using PureGate.WebSocket.Protocol;

namespace PureGate.WebSocket;

// An event the server sends to browsers. Declared once, usually as a static field, mapped on the endpoint with
// MapEvent, and sent through it, so a send site never repeats a name or a type.
public sealed class ServerEvent<T>(string name)
{
  public string Name { get; } = NameCheck.Validate(name);

  internal byte[] NameUtf8 { get; } = Encoding.ASCII.GetBytes(name);
}

// A call the server makes to a browser, which answers it with a handler. Declared once, mapped on the endpoint
// with MapClientRequest, and called through WebSocketConnection.InvokeAsync.
public sealed class ClientRequest<TRequest, TResponse>(string name)
{
  public string Name { get; } = NameCheck.Validate(name);

  internal byte[] NameUtf8 { get; } = Encoding.ASCII.GetBytes(name);
}

static class NameCheck
{
  // Dot-separated segments of [A-Za-z_][A-Za-z0-9_]*, 1 to 256 bytes (docs/PROTOCOL.md, Names).
  public static string Validate(string name, string parameter = "name")
  {
    ArgumentNullException.ThrowIfNull(name, parameter);
    if (!NameRules.IsValid(Encoding.UTF8.GetBytes(name)))
      throw new ArgumentException($"'{name}' is not a name: use dot-separated segments of letters, digits and underscores, starting with a letter or underscore, 1 to 256 characters in all.", parameter);

    return name;
  }
}
