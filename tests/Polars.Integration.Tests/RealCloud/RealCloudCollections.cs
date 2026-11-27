namespace Polars.Integration.Tests;

// Each cloud's real-cloud test classes run one at a time and share the run's cleanup, done once all are over: a cleanup
// per class deleted the run's prefix while another class was still writing below it, and missed what a class without
// one left behind.

[CollectionDefinition(RealAws.Collection)]
public sealed class RealAwsCollection : ICollectionFixture<RealAwsCleanup>;

[CollectionDefinition(RealAzure.Collection)]
public sealed class RealAzureCollection : ICollectionFixture<RealAzureCleanup>;

[CollectionDefinition(RealGcs.Collection)]
public sealed class RealGcsCollection : ICollectionFixture<RealGcsCleanup>;
