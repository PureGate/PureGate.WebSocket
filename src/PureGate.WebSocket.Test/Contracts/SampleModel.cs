using System.Text.Json;
using System.Text.Json.Serialization;
using PureGate.WebSocket.Test.Contracts.Billing;
using PureGate.WebSocket.Test.Contracts.Shop;

// Types that cover what the contract generator maps (docs/PLAN-SERVER.md, Contract generation).

namespace PureGate.WebSocket.Test.Contracts.Shop
{
  public sealed record Order(
      int Id,
      string Customer,
      decimal Total,
      long Version,
      DateTimeOffset Placed,
      Guid Reference,
      Status Status,
      Priority Priority,
      int? Discount,
      List<Line> Lines,
      Dictionary<string, int> Tags,
      byte[] Attachment,
      Order Previous,
      [property: JsonPropertyName("is-rush")] bool Rush,
      [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string Note,
      Money Price);

  public sealed record Line(string Sku, int Quantity);

  public enum Status
  {
    Open,
    Closed,
  }

  [JsonConverter(typeof(JsonStringEnumConverter<Priority>))]
  public enum Priority
  {
    Low,
    High,
  }

  public sealed record Page<T>(T[] Items, int Total);

  [JsonPolymorphic]
  [JsonDerivedType(typeof(Circle), "circle")]
  [JsonDerivedType(typeof(Square), "square")]
  public abstract record Shape;

  public sealed record Circle(double Radius) : Shape;

  public sealed record Square(double Side) : Shape;

  public sealed record Invoice(int Id);

  public sealed record Money(decimal Amount);
}

namespace PureGate.WebSocket.Test.Contracts.Billing
{
  public sealed record Invoice(string Number);
}

namespace PureGate.WebSocket.Test.Contracts
{
  [JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
  [JsonSerializable(typeof(Order))]
  [JsonSerializable(typeof(Line))]
  [JsonSerializable(typeof(Page<Order>))]
  [JsonSerializable(typeof(Shape))]
  [JsonSerializable(typeof(Shop.Invoice))]
  [JsonSerializable(typeof(Billing.Invoice), TypeInfoPropertyName = "BillingInvoice")]
  [JsonSerializable(typeof(Money))]
  [JsonSerializable(typeof(int))]
  [JsonSerializable(typeof(double))]
  [JsonSerializable(typeof(string))]
  [JsonSerializable(typeof(bool))]
  public sealed partial class SampleJson : JsonSerializerContext;

  static class SampleApi
  {
    public static readonly ServerEvent<Order> Shipped = new("shop.orders.shipped");
    public static readonly ClientRequest<string, bool> Confirm = new("ui.confirm");

    public static void Map(WebSocketEndpointBuilder ws)
    {
      ws.MapEvent(Shipped).MapClientRequest(Confirm);

      var orders = ws.MapGroup("shop.orders");
      orders.MapRequest<int, Order>("get", static (_, _, _) => throw new NotSupportedException());
      orders.MapRequest<Page<Order>>("list", static (_, _) => throw new NotSupportedException());
      orders.MapRequest<Order>("place", static (_, _, _) => ValueTask.CompletedTask);
      ws.MapRequest<Shape, double>("shop.shapes.area", static (_, _, _) => throw new NotSupportedException());
      ws.MapRequest<int, Shop.Invoice>("shop.invoices.get", static (_, _, _) => throw new NotSupportedException());
      ws.MapRequest<int, Billing.Invoice>("billing.invoices.get", static (_, _, _) => throw new NotSupportedException());
      ws.MapRequest<int, byte[]>("shop.files.download", static (_, _, _) => throw new NotSupportedException());
      ws.MapRequest<byte[]>("shop.files.upload", static (_, _, _) => ValueTask.CompletedTask);
      ws.MapRequest("ping", static (_, _) => ValueTask.CompletedTask);
      ws.MapClientEvent<Line>("ui.clicked", static (_, _, _) => ValueTask.CompletedTask);
    }

    public static void Configure(PureGateWebSocketOptions options)
    {
      options.SerializerOptions.TypeInfoResolverChain.Insert(0, SampleJson.Default);
      options.Contract.MapType<Money>("string");
    }
  }
}
