using System.Collections.Concurrent;
using PureGate.WebSocket;

// A chat: rooms you join, messages you post, and who is typing. Run this, then the page in ../web with yarn dev.
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddPureGateWebSocket(options =>
{
  // In development the page comes from Vite's server, another origin.
  if (builder.Environment.IsDevelopment())
    options.AllowedOrigins = ["http://localhost:5173"];
});
builder.Services.AddSingleton<Rooms>();

var app = builder.Build();
app.UseWebSockets();
app.UseDefaultFiles();
app.UseStaticFiles();

var ws = app.MapPureGateWebSocket("/chat");
ws.MapEvent(Chat.Posted).MapEvent(Chat.Typing);

var room = ws.MapGroup("room");

// Joins a room and returns what was said there so far.
room.MapRequest<string, Message[]>("join", static (name, call, cancellationToken) =>
{
  call.Connection.Groups.Add(Rooms.Group(name));
  return ValueTask.FromResult(call.Services.GetRequiredService<Rooms>().History(name));
});

room.MapRequest<string>("leave", static (name, call, cancellationToken) =>
{
  call.Connection.Groups.Remove(Rooms.Group(name));
  return ValueTask.CompletedTask;
});

// Posts to a room: everyone in it gets the message, its author included.
room.MapRequest<Post, Message>("post", static async (post, call, cancellationToken) =>
{
  if (string.IsNullOrWhiteSpace(post.Text) || post.Text.Length > 2000)
    throw new CallException("invalid_message", "A message is 1 to 2,000 characters.");

  var message = call.Services.GetRequiredService<Rooms>().Add(post);
  await call.Services.GetRequiredService<IWebSocketHub>().Group(Rooms.Group(post.Room)).SendAsync(Chat.Posted, message, cancellationToken);
  return message;
});

// Someone is typing: everyone else in the room hears about it.
room.MapClientEvent<Typing>("typing", static (typing, call, cancellationToken) =>
  call.Services.GetRequiredService<IWebSocketHub>().Group(Rooms.Group(typing.Room)).Except(call.Connection).SendAsync(Chat.Typing, typing, cancellationToken));

// The page pulls its types from here: yarn contract in ../web.
if (app.Environment.IsDevelopment())
  ws.MapContract("/chat/contract.ts");

app.Run();

public sealed record Post(string Room, string Author, string Text);

public sealed record Message(long Id, string Room, string Author, string Text, DateTimeOffset At);

public sealed record Typing(string Room, string Author);

static class Chat
{
  public static readonly ServerEvent<Message> Posted = new("room.posted");
  public static readonly ServerEvent<Typing> Typing = new("room.typing");
}

// The last 100 messages of every room, in memory.
sealed class Rooms
{
  const int Kept = 100;

  readonly ConcurrentDictionary<string, Queue<Message>> ByRoom = new(StringComparer.Ordinal);
  long LastId;

  public static string Group(string room) => $"room:{room}";

  public Message[] History(string room)
  {
    var messages = ByRoom.GetOrAdd(room, static _ => new Queue<Message>());
    lock (messages)
      return [.. messages];
  }

  public Message Add(Post post)
  {
    var message = new Message(Interlocked.Increment(ref LastId), post.Room, post.Author, post.Text, DateTimeOffset.UtcNow);
    var messages = ByRoom.GetOrAdd(post.Room, static _ => new Queue<Message>());
    lock (messages)
    {
      messages.Enqueue(message);
      if (messages.Count > Kept)
        messages.Dequeue();
    }

    return message;
  }
}
