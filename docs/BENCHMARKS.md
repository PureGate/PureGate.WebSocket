# Benchmarks

PureGate.WebSocket against SignalR on the same machine, with the scenarios of [the server's design](PLAN-SERVER.md#benchmarks), and the browser client's hot paths.

Measured on 2026-11-13: Intel Core Ultra 7 265KF (20 cores), 64 GB, Windows 11 Pro, .NET SDK 11.0.100, Node 26.12.1.

## Server

The server runs in its own process, so its memory, allocations and CPU are its own, and the clients run in another. Both have tiered compilation off (`TC=0`), and nothing warms up first. Each figure is the median of 3 runs of 2 seconds.

- **PureGate.WebSocket** is driven by a minimal C# client on the library's own frame codec. It sends the way the browser client does: every call carries the browser's default 30 s deadline, and the frames written while a message is going out are coalesced into the next one.
- **SignalR** is driven by its own .NET client, over WebSockets, with the JSON and the MessagePack protocols.

### 1 KB echo, sequential, one connection

| System | calls/s | p50 ms | p99 ms | server bytes/call |
|---|---:|---:|---:|---:|
| PureGate.WebSocket | 22,141 | 0.041 | 0.097 | 2,236 |
| SignalR JSON | 14,057 | 0.067 | 0.155 | 2,689 |
| SignalR MessagePack | 15,463 | 0.061 | 0.123 | 2,688 |

The echoed 1 KB string is 2,072 of the bytes each call allocates.

### 1 KB echo, 64 callers on one connection

| System | calls/s | p50 ms | p99 ms |
|---|---:|---:|---:|
| PureGate.WebSocket | 602,888 | 0.09 | 0.367 |
| SignalR JSON | 118,042 | 0.494 | 0.915 |
| SignalR MessagePack | 292,592 | 0.192 | 0.488 |

### 1,000 connections making steady calls

Every connection calls, waits 10 ms and calls again. Windows rounds the wait up to its 15.6 ms timer tick, which caps all three at the same rate, and makes the p99 vary from run to run.

| System | calls/s | p50 ms | p99 ms | server CPU % |
|---|---:|---:|---:|---:|
| PureGate.WebSocket | 58,916 | 1.956 | 13.458 | 10.273 |
| SignalR JSON | 58,592 | 2.164 | 17.878 | 11.202 |
| SignalR MessagePack | 58,847 | 2.077 | 9.449 | 10.795 |

### 10,000 idle connections, then one broadcast to all

The server's heap per idle connection, and the time from the broadcast call until the last connection has the event.

| System | server KB/connection | broadcast ms |
|---|---:|---:|
| PureGate.WebSocket | 23.785 | 23.755 |
| SignalR JSON | 22.424 | 27.629 |
| SignalR MessagePack | 23.725 | 29.065 |

PureGate.WebSocket also keeps the event, as it keeps everything it sends, until the browser acknowledges it, so that a session can resume without losing it.

### Running them

They load the machine, and the broadcast opens 10,000 local connections for each system, so run them on purpose only:

```sh
dotnet run -c Release --project src/PureGate.WebSocket.Benchmarks
```

`--scenario echo|echo64|steady|fanout` runs one scenario, and may be repeated. `--runs`, `--seconds`, `--steady` and `--fanout` change the number of runs, their length and the number of connections. The defaults finish in under 2 minutes.

## Browser client

Node runs the sources directly, without Vite's module runner, whose export getters the loops would measure too.

| Path | per second |
|---|---:|
| Write a 1 KB Request | 11,081,852 |
| Read one message of 16 Responses of 1 KB | 2,712,509 |
| Decode 1 KB of JSON | 2,485,481 |
| Encode 1 KB of JSON | 1,782,941 |

```sh
yarn workspace @puregate/web-socket bench
```
