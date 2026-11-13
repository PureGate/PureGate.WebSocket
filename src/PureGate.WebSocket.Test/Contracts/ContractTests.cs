using System.Net;
using System.Net.WebSockets;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PureGate.WebSocket.Protocol;
using PureGate.WebSocket.Test.Integration;

namespace PureGate.WebSocket.Test.Contracts;

public sealed class ContractTests
{
  // The sample model's contract. The TypeScript tests type-check it too, so it must compile.
  [Test]
  public async Task DescribesTheSampleModel()
  {
    var contract = await SampleContractAsync();

    Snapshots.AssertMatches("packages/web-socket/test/contracts/sample.generated.ts", contract.TypeScript);
  }

  // The TestServer's contract, which the TypeScript integration tests are typed against.
  [Test]
  public async Task DescribesTheTestServer()
  {
    await using var host = await TestHost.StartAsync();

    Snapshots.AssertMatches("packages/web-socket/test/integration/contract.generated.ts", host.Endpoint.Contract.TypeScript);
  }

  [Test]
  public async Task HashesTheSameWayEveryTime()
  {
    var first = await SampleContractAsync();
    var second = await SampleContractAsync(options => options.SerializerOptions.WriteIndented = true);

    Assert.That(first.Hash, Has.Length.EqualTo(16));
    Assert.That(second.Hash, Is.EqualTo(first.Hash));
    Assert.That(first.TypeScript, Does.Contain($"contractHash = \"{first.Hash}\""));
  }

  [Test]
  public async Task HashesDifferentApisDifferently()
  {
    var first = await SampleContractAsync();
    var second = await SampleContractAsync(map: ws => ws.MapRequest<int, int>("extra.op", static (n, _, _) => ValueTask.FromResult(n)));

    Assert.That(second.Hash, Is.Not.EqualTo(first.Hash));
  }

  [Test]
  public async Task HashesAnUploadsItemType()
  {
    var numbers = await SampleContractAsync(map: ws => ws.MapUpload<int, int>("extra.put", static (_, _, _) => ValueTask.FromResult(0)));
    var texts = await SampleContractAsync(map: ws => ws.MapUpload<string, int>("extra.put", static (_, _, _) => ValueTask.FromResult(0)));

    Assert.That(texts.Hash, Is.Not.EqualTo(numbers.Hash));
    Assert.That(numbers.TypeScript, Does.Contain("put(item: number): number;"));
  }

  [Test]
  public async Task DescribesReferenceTypesThatCanBeNullWhenAsked()
  {
    var contract = await SampleContractAsync(options => options.Contract.ReferenceTypesCanBeNull = true);

    Assert.That(contract.TypeScript, Does.Contain("customer: string | null;"));
  }

  [Test]
  public async Task RefusesNamesThatAreBothAnOperationAndAGroup()
  {
    var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await SampleContractAsync(map: ws =>
      ws.MapRequest("shop.orders", static (_, _) => ValueTask.CompletedTask)));

    Assert.That(exception.Message, Does.Contain("both an operation and a group"));
  }

  [Test]
  public async Task TellsTheBrowserWhetherItsContractIsCurrent()
  {
    await using var host = await TestHost.StartAsync();
    var hash = host.Endpoint.Contract.Hash;

    await using var current = await host.ConnectAndHelloAsync($$"""{"calls":4,"maxMessage":65536,"contract":"{{hash}}"}""");
    Assert.That(current.Welcome.GetProperty("contract").GetString(), Is.EqualTo("current"));

    await using var stale = await host.ConnectAndHelloAsync("""{"calls":4,"maxMessage":65536,"contract":"0000000000000000"}""");
    Assert.That(stale.Welcome.GetProperty("contract").GetString(), Is.EqualTo("stale"));
  }

  [Test]
  public async Task RefusesStaleContractsWhenAsked()
  {
    await using var host = await TestHost.StartAsync(options => options.ContractMismatch = ContractMismatchPolicy.Reject);
    await using var client = await host.ConnectAsync();
    await client.SendAsync(Outgoing.Hello("""{"calls":4,"maxMessage":65536,"contract":"0000000000000000"}"""));

    Assert.That(await client.ReceiveCloseAsync(), Is.EqualTo((WebSocketCloseStatus)4004));
  }

  [Test]
  public async Task IgnoresContractsWhenAsked()
  {
    await using var host = await TestHost.StartAsync(options => options.ContractMismatch = ContractMismatchPolicy.Ignore);
    await using var client = await host.ConnectAndHelloAsync("""{"calls":4,"maxMessage":65536,"contract":"0000000000000000"}""");

    Assert.That(client.Welcome.TryGetProperty("contract", out _), Is.False);
  }

  [Test]
  public async Task ServesTheContractOverHttp()
  {
    await using var host = await TestHost.StartAsync(map: ws => ws.MapContract("/ws/contract.ts"));
    using var http = new HttpClient();
    var url = $"{host.Origin}/ws/contract.ts";

    using var response = await http.GetAsync(url);
    Assert.That(response.StatusCode, Is.EqualTo(HttpStatusCode.OK));
    Assert.That(response.Content.Headers.ContentType?.MediaType, Is.EqualTo("text/plain"));
    Assert.That(await response.Content.ReadAsStringAsync(), Is.EqualTo(host.Endpoint.Contract.TypeScript));

    using var conditional = new HttpRequestMessage(HttpMethod.Get, url);
    conditional.Headers.IfNoneMatch.Add(response.Headers.ETag!);
    using var notModified = await http.SendAsync(conditional);
    Assert.That(notModified.StatusCode, Is.EqualTo(HttpStatusCode.NotModified));
  }

  [Test]
  public async Task FreezesTheMappingOnceTheContractIsRead()
  {
    await using var host = await TestHost.StartAsync();
    _ = host.Endpoint.Contract;

    Assert.Throws<InvalidOperationException>(() => host.Endpoint.MapRequest("late.op", static (_, _) => ValueTask.CompletedTask));
  }

  static async Task<WebSocketContract> SampleContractAsync(
      Action<PureGateWebSocketOptions> configure = null,
      Action<WebSocketEndpointBuilder> map = null)
  {
    var builder = WebApplication.CreateSlimBuilder();
    builder.Logging.ClearProviders();
    builder.Services.AddPureGateWebSocket(options =>
    {
      SampleApi.Configure(options);
      configure?.Invoke(options);
    });

    await using var app = builder.Build();
    var ws = app.MapPureGateWebSocket("/ws");
    SampleApi.Map(ws);
    map?.Invoke(ws);
    return ws.Contract;
  }
}
