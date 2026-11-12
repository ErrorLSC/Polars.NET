/// Credentials Unity Catalog vends for a staged managed table, renewed while its version 0 is written.
module Polars.FSharp.Tests.SeseragiCatalogCredentialsTests

open System
open System.Net
open System.Net.Http
open System.Text
open Xunit
open Polars.NET.Core
open Polars.NET.Core.Delta
open Polars.NET.Core.Wrappers
open Kaminari.Seseragi.Lakehouse.Catalog.Unity

/// Answers staging requests with a staged table whose credentials expire, and credential requests with fresh ones.
type private StagingCredentialsHandler() =
    inherit HttpMessageHandler()
    member val Paths = ResizeArray<string>()
    member private this.Answer(request: HttpRequestMessage) =
        this.Paths.Add request.RequestUri.PathAndQuery
        let body =
            if request.RequestUri.AbsolutePath.EndsWith "/staging-tables" then
                """{"table-id":"a33b079e","table-type":"MANAGED","location":"s3://b/uc/t1",
                    "storage-credentials":[{"prefix":"s3://b/uc/t1","operation":"READ_WRITE",
                      "config":{"s3.access-key-id":"OLD","s3.secret-access-key":"S"},"expiration-time-ms":1}],
                    "required-protocol":{"min-reader-version":3,"min-writer-version":7,"reader-features":["catalogManaged"],
                      "writer-features":["catalogManaged","inCommitTimestamp"]},
                    "required-properties":{"delta.enableInCommitTimestamps":"true"}}"""
            else
                """{"storage-credentials":[{"prefix":"s3://b/uc/t1","operation":"READ_WRITE",
                    "config":{"s3.access-key-id":"FRESH","s3.secret-access-key":"S"},"expiration-time-ms":4102444800000}]}"""
        new HttpResponseMessage(HttpStatusCode.OK, Content = new StringContent(body, Encoding.UTF8, "application/json"))
    override this.Send(request, _) = this.Answer request
    override this.SendAsync(request, _) = Threading.Tasks.Task.FromResult(this.Answer request)

[<Fact>]
let ``A staged table's expired credentials are renewed through the catalog's Delta API`` () =
    let handler = new StagingCredentialsHandler()
    use handle = new CatalogHandle(new UnityCatalogClient("http://uc.test/", "tok", new HttpClient(handler)))
    let expired =
        TemporaryCredentials(Kind = CredentialKind.Aws, AwsAccessKeyId = "OLD", AwsSecretAccessKey = "S", ExpiresAt = DateTimeOffset.UtcNow.AddSeconds -1.0)

    let id = handle.RenewableStagingCredentials("a33b079e", "s3://b/uc/t1", expired)
    let mutable renewAt = Nullable<DateTimeOffset>()
    let current = HostCredentials.Get(id).Current(&renewAt)

    Assert.Equal("FRESH", current.Options["aws_access_key_id"])
    Assert.Equal<string list>([ "/api/2.1/unity-catalog/delta/v1/staging-tables/a33b079e/credentials" ], List.ofSeq handler.Paths)
    // The same staged table keeps one source.
    Assert.Equal(id, handle.RenewableStagingCredentials("a33b079e", "s3://b/uc/t1", expired))

[<Fact>]
let ``Staging a table for a write names the renewable credentials in its cloud options`` () =
    let handler = new StagingCredentialsHandler()
    use handle = new CatalogHandle(new UnityCatalogClient("http://uc.test/", "tok", new HttpClient(handler)))

    let staged = PolarsWrapper.StageManagedTable(handle, "c", "s", "t", null, [| "region" |], [| "eu" |])

    let options = Seq.zip staged.Cloud.Keys staged.Cloud.Values |> dict
    Assert.Equal("eu", options["region"])
    let mutable renewAt = Nullable<DateTimeOffset>()
    // The vended credentials expired long ago: the source named in the options fetches the staged table's again.
    let current = HostCredentials.Get(UInt64.Parse options[HostCredentials.Key]).Current(&renewAt)
    Assert.Equal("FRESH", current.Options["aws_access_key_id"])
    Assert.Contains("/api/2.1/unity-catalog/delta/v1/staging-tables/a33b079e/credentials", handler.Paths)
