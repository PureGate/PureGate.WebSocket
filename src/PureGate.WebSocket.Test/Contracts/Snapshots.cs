namespace PureGate.WebSocket.Test.Contracts;

// Files checked into the repository that tests compare generated output with. Run the tests with
// PUREGATE_UPDATE_SNAPSHOTS=1 to write the new output instead, then review the diff.
static class Snapshots
{
  public static string Repository
  {
    get
    {
      var directory = new DirectoryInfo(AppContext.BaseDirectory);
      while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PureGate.WebSocket.slnx")))
        directory = directory.Parent;

      return directory?.FullName ?? throw new InvalidOperationException("The repository was not found above the tests.");
    }
  }

  public static void AssertMatches(string relativePath, string actual)
  {
    var path = Path.Combine(Repository, relativePath);
    if (Environment.GetEnvironmentVariable("PUREGATE_UPDATE_SNAPSHOTS") == "1")
    {
      File.WriteAllText(path, actual);
      return;
    }

    var expected = File.Exists(path) ? File.ReadAllText(path).ReplaceLineEndings("\n") : "(missing)";
    Assert.That(actual, Is.EqualTo(expected), $"{relativePath} is stale. Run the tests with PUREGATE_UPDATE_SNAPSHOTS=1 to update it.");
  }
}
