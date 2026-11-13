import { CallError, connect } from "@puregate/web-socket";
import { type Contract, contractHash, type Message } from "./contract.generated.ts";

// The room comes from the address, ?room=lobby by default, and the name is made up for the tab.
const room = new URLSearchParams(location.search).get("room") ?? "lobby";
const author = `guest-${Math.floor(Math.random() * 1000)}`;

const element = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const messages = element<HTMLUListElement>("messages");
const status = element<HTMLParagraphElement>("status");
const typing = element<HTMLParagraphElement>("typing");
const form = element<HTMLFormElement>("post");
const text = element<HTMLInputElement>("text");
element("room").textContent = room;

const conn = connect<Contract>("/chat", { contractHash });
const shown = new Set<number>();

function show(message: Message): void {
  if (shown.has(message.id)) return;
  shown.add(message.id);
  const item = document.createElement("li");
  const name = document.createElement("b");
  name.textContent = message.author;
  item.append(name, message.text);
  messages.append(item);
}

// Joining returns the room's history; after a reconnect that did not resume, join again for what was missed.
async function join(): Promise<void> {
  for (const message of await conn.api.room.join(room)) show(message);
}

conn.on.room.posted((message) => {
  if (message.room === room) show(message);
});

let typingTimer: ReturnType<typeof setTimeout> | undefined;
conn.on.room.typing((who) => {
  typing.textContent = `${who.author} is typing…`;
  clearTimeout(typingTimer);
  typingTimer = setTimeout(() => {
    typing.textContent = "";
  }, 2000);
});

conn.onStateChange((state, change) => {
  status.textContent = state === "open" ? `Connected as ${author}` : `${state[0]?.toUpperCase()}${state.slice(1)}…`;
  if (state === "open" && change.resumed === false) void join();
});

let lastTyping = 0;
text.addEventListener("input", () => {
  if (performance.now() - lastTyping < 1000) return;
  lastTyping = performance.now();
  conn.emit.room.typing({ room, author });
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    show(await conn.api.room.post({ room, author, text: text.value }));
    text.value = "";
  } catch (error) {
    status.textContent = error instanceof CallError ? error.message : String(error);
  }
});
