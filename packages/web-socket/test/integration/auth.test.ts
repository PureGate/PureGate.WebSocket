import { describe, expect, inject, it } from "vitest";
import { type Connection, type ConnectionState, type ConnectOptions, connect } from "../../src/index.ts";

// The TestServer's /ws-token endpoint authenticates by token (TestApi.AuthenticateToken): "user" or "user:role",
// with "+milliseconds" for one that expires.
interface TokenContract {
  api: { auth: { whoami(): string }; secure: { member(): string; admin(): string } };
}

const pause = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));

function openWithToken(options: ConnectOptions): Connection<TokenContract> {
  return connect<TokenContract>(inject("serverUrl").replace(/\/ws$/, "/ws-token"), options);
}

describe("token authentication", () => {
  it("authenticates by token", async () => {
    const conn = openWithToken({ token: () => "alice" });
    try {
      expect(await conn.api.auth.whoami()).toBe("alice");
    } finally {
      await conn.close();
    }
  });

  it("refreshes the token before it expires, and stays connected", async () => {
    const tokens: string[] = [];
    const conn = openWithToken({
      token: () => {
        const token = tokens.length === 0 ? "alice+1500" : "alice+60000";
        tokens.push(token);
        return token;
      },
    });
    try {
      await conn.opened;
      await pause(2_000);

      expect(tokens).toEqual(["alice+1500", "alice+60000"]);
      expect(conn.state).toBe("open");
      expect(await conn.api.auth.whoami()).toBe("alice");
    } finally {
      await conn.close();
    }
  });

  it("closes for good when the token is refused twice", async () => {
    const states: ConnectionState[] = [];
    const conn = openWithToken({ token: () => "bad" });
    conn.onStateChange((state) => states.push(state));

    await expect(conn.opened).rejects.toMatchObject({ code: "connection_closed" });
    expect(conn.state).toBe("closed");
  });

  it("answers forbidden and unauthorized from the operations' policies", async () => {
    const member = openWithToken({ token: () => "alice" });
    const admin = openWithToken({ token: () => "root:admin" });
    try {
      expect(await member.api.secure.member()).toBe("member");
      await expect(member.api.secure.admin()).rejects.toMatchObject({ code: "forbidden" });
      expect(await admin.api.secure.admin()).toBe("admin");
    } finally {
      await Promise.all([member.close(), admin.close()]);
    }
  });
});
