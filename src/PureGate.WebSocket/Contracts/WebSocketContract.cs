namespace PureGate.WebSocket;

/// <summary>
/// An endpoint's API as TypeScript: the contract.ts browsers are typed against (docs/PLAN-SERVER.md, Contract
/// generation). From MapContract over HTTP, or from WebSocketEndpointBuilder.Contract, for instance in a test that
/// fails when the committed contract.ts is stale.
/// </summary>
public sealed class WebSocketContract
{
  internal WebSocketContract(string typeScript, string hash)
  {
    TypeScript = typeScript;
    Hash = hash;
  }

  /// <summary>
  /// The contents of contract.ts.
  /// </summary>
  public string TypeScript { get; }

  /// <summary>
  /// 16 hex digits over the names and schemas the contract describes, not over the TypeScript text, so an improved
  /// emitter does not change it. Browsers send it in Hello.
  /// </summary>
  public string Hash { get; }
}

/// <summary>
/// How the contract describes .NET types.
/// </summary>
public sealed class ContractOptions
{
  internal Dictionary<Type, string> Overrides { get; } = [];

  /// <summary>
  /// Whether reference-type properties and results may be null in TypeScript (T | null). Off by default: types from
  /// code without nullable annotations would otherwise need a null check at every property.
  /// </summary>
  public bool ReferenceTypesCanBeNull { get; set; }

  /// <summary>
  /// Describes T with this TypeScript type instead, for types with converters the schema exporter cannot see
  /// into, such as MapType&lt;Money&gt;("string").
  /// </summary>
  public ContractOptions MapType<T>(string typeScript)
  {
    ArgumentException.ThrowIfNullOrWhiteSpace(typeScript);
    Overrides[typeof(T)] = typeScript;
    return this;
  }

  internal ContractOptions Clone()
  {
    var clone = new ContractOptions { ReferenceTypesCanBeNull = ReferenceTypesCanBeNull };
    foreach (var (type, typeScript) in Overrides)
      clone.Overrides[type] = typeScript;

    return clone;
  }
}

/// <summary>
/// What the server does when a browser's contract hash does not match its own.
/// </summary>
public enum ContractMismatchPolicy
{
  /// <summary>
  /// Nothing.
  /// </summary>
  Ignore,

  /// <summary>
  /// Logs it, and tells the browser in Welcome, which warns in its console.
  /// </summary>
  Warn,

  /// <summary>
  /// Closes the connection with 4004 (contract_rejected).
  /// </summary>
  Reject,
}
