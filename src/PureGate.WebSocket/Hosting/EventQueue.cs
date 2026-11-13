namespace PureGate.WebSocket.Hosting;

// One connection's events, handled one at a time in the order they arrived. Nothing runs while the queue is
// empty: the first event starts a worker on the thread pool, and the worker stops when it has drained the queue.
sealed class EventQueue(WebSocketConnection connection)
{
  readonly Queue<EventWork> Items = new();
  readonly Lock Gate = new();
  bool Running;

  public void Enqueue(EventWork work)
  {
    bool start;
    lock (Gate)
    {
      Items.Enqueue(work);
      start = !Running;
      Running = true;
    }

    if (start)
      ThreadPool.UnsafeQueueUserWorkItem(static queue => _ = queue.RunAsync(), this, preferLocal: false);
  }

  async Task RunAsync()
  {
    while (true)
    {
      EventWork next;
      lock (Gate)
      {
        if (!Items.TryDequeue(out next))
        {
          Running = false;
          return;
        }
      }

      connection.EventPickedUp();
      if (!connection.IsEnded)
        await next.RunAsync(connection);
    }
  }
}
