# Chat

A chat with rooms, built on PureGate.WebSocket: the server in `Chat.Server`, the page in `web`. It shows calls (`room.join`, `room.post`), events to a group (`room.posted`), an event from the browser to the others in the room (`room.typing`), and the contract that types the page.

Run the server, then the page:

```sh
dotnet run --project samples/chat/Chat.Server
yarn workspace chat-web dev
```

Open http://localhost:5173 in two tabs, or http://localhost:5173/?room=another for another room.

When the server's operations change, pull the contract again while the server runs in Development:

```sh
yarn workspace chat-web contract
```

`yarn workspace chat-web build` writes the page into the server's `wwwroot`, where the server serves it on its own at http://localhost:5080.
