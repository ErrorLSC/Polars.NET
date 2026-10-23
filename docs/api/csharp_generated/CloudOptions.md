---
uid: Polars.CSharp.CloudOptions
title: CloudOptions
---

# CloudOptions

Namespace: `Polars.CSharp`

Configuration options for Cloud IO (S3, Azure, GCP, etc).

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `Credentials` | `Dictionary<string, string>` | Key-value pairs for authentication and configuration (e.g., "aws_access_key_id", "endpoint_url"). <br>See Polars documentation for supported keys for each provider. |
| `FileCacheTtl` | `ulong` | Time-to-live for the file cache in seconds. Default is 0 (no cache/default). |
| `MaxRetries` | `uint` | Maximum number of retries for cloud requests. Default is 2. |
| `Provider` | `CloudProvider` | The cloud provider to use. |
| `RetryInitBackoffMs` | `ulong` | The initial backoff time for retries in milliseconds. Default is 500ms. |
| `RetryMaxBackoffMs` | `ulong` | The maximum backoff time for retries in milliseconds. Default is 10,000ms (10 seconds). |
| `RetryTimeoutMs` | `ulong` | The timeout for a single cloud request in milliseconds. Default is 30,000ms (30 seconds). |

## Constructors & Static Methods

### CloudOptions

```csharp
public CloudOptions()
```

---

### Aws

Creates options for Amazon S3 or S3-compatible services (like MinIO).

```csharp
public static CloudOptions Aws(
    string region,
    string? accessKey = null,
    string? secretKey = null,
    string? endpoint = null,
    string? bucket = null,
    string? sessionToken = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `region` | `string` | The AWS region (e.g., "us-east-1"). |
| `accessKey` | `string?` | AWS Access Key ID. |
| `secretKey` | `string?` | AWS Secret Access Key. |
| `endpoint` | `string?` | Custom endpoint URL (required for MinIO/R2 etc). |
| `bucket` | `string?` | Optional bucket name (usually inferred from path). |
| `sessionToken` | `string?` | Optional session token for temporary credentials. |

---

### Azure

Creates options for Azure Blob Storage or Azure Data Lake Gen2 (ADLS).

```csharp
public static CloudOptions Azure(
    string accountName,
    string? accessKey = null,
    string? sasToken = null,
    string? endpoint = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `accountName` | `string` | The storage account name. |
| `accessKey` | `string?` | The storage account access key (Shared Key). |
| `sasToken` | `string?` | Shared Access Signature (SAS) token. (Mutually exclusive with accessKey). |
| `endpoint` | `string?` | Custom endpoint URL. |

---

### Gcp

Creates options for Google Cloud Storage (GCS).

```csharp
public static CloudOptions Gcp(string? serviceAccountPath = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `serviceAccountPath` | `string?` | Path to the Service Account JSON file. If null, uses default authentication chain. |

---

### Http

Creates options for generic HTTP/HTTPS sources.

Creates options for generic HTTP/HTTPS with a Bearer token.

```csharp
public static CloudOptions Http(Dictionary<string, string> headers)
public static CloudOptions Http(string bearerToken)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `headers` | `Dictionary<string, string>` | Custom headers (e.g. Authorization) to include in requests. |
| `bearerToken` | `string` | The bearer token string. |

---

### HuggingFace

Creates options for Hugging Face datasets (hf://).

```csharp
public static CloudOptions HuggingFace(string? token = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `token` | `string?` | Hugging Face API token. If null, Polars will look for HF_TOKEN env var. |

---

