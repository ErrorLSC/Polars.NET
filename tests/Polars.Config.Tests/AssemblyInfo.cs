using Xunit;

// Disable parallel execution in Polars.Config.Tests because these tests mutate
// process-wide environment variables and global configuration.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
