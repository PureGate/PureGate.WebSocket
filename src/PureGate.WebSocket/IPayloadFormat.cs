using System.Buffers;

namespace PureGate.WebSocket;

// A payload format besides JSON, such as MessagePack, that a browser can ask for in its Hello (docs/PROTOCOL.md,
// Payload). Add it to PureGateWebSocketOptions.PayloadFormats, and give the browser client a format of the same name.
// It writes and reads every type the endpoint's operations, events and calls to the browser carry, and must read them
// as the same shapes their JSON has, since contract.ts describes those. byte[] and ReadOnlyMemory<byte> travel as raw
// bytes in every format, and error details stay JSON.
public interface IPayloadFormat
{
  // What a browser asks for: 1 to 32 lowercase ASCII letters, digits, '-' and '.'. "json" is JSON's own.
  string Name { get; }

  void Write<T>(IBufferWriter<byte> output, T value);

  // Reads a payload, which is never empty. The memory is part of the received message and is reused once the call
  // returns, so the value must not keep it. An exception means the payload is not a T: the exchange fails with
  // invalid_payload.
  T Read<T>(ReadOnlyMemory<byte> payload);
}
