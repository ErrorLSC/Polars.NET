namespace Polars.FSharp

type CloudOptions =
    {
        Provider: CloudProvider
        Credentials: Map<string, string>
        Retries: uint32
        RetryTimeoutMs: uint64
        RetryInitBackoffMs: uint64
        RetryMaxBackoffMs: uint64
        CacheTTL: uint64
    }
    with
        static member Default = 
            {
                Provider = CloudProvider.NotCloud
                Credentials = Map.empty
                Retries = 2u
                RetryTimeoutMs = 0UL
                RetryInitBackoffMs = 0UL
                RetryMaxBackoffMs = 0UL
                CacheTTL = 0UL
            }
        /// <summary>
        /// Internal helper to parse CloudOptions into native FFI primitives.
        /// </summary>
        static member ParseCloudOptions(cloudOptions: CloudOptions option) =
            match cloudOptions with
            | Some opts ->
                // 1. Provider
                let provider = opts.Provider.ToNative()

                // 2. Integers (Assuming your F# CloudOptions record has these mapped)
                let retries = unativeint opts.Retries
                let timeoutMs = opts.RetryTimeoutMs
                let initBackoffMs = opts.RetryInitBackoffMs
                let maxBackoffMs = opts.RetryMaxBackoffMs
                let cacheTtl = opts.CacheTTL

                // 3. Credentials: Map -> Arrays
                let keys = opts.Credentials |> Map.toSeq |> Seq.map fst |> Seq.toArray
                let values = opts.Credentials |> Map.toSeq |> Seq.map snd |> Seq.toArray

                provider, retries, timeoutMs, initBackoffMs, maxBackoffMs, cacheTtl, keys, values

            | None ->
                // Default: NotCloud
                CloudProvider.NotCloud.ToNative(), unativeint 0, 0UL, 0UL, 0UL, 0UL, null, null
        // ==========================================
        // Helper Methods (Factory Functions)
        // ==========================================

        /// AWS S3
        static member Aws(
            bucket: string,
            region: string,
            ?accessKey: string,
            ?secretKey: string,
            ?sessionToken: string,
            ?endpoint: string,
            ?allowHttp: bool) =
            
            let mutable creds = 
                [
                    "aws_region", region
                    "aws_bucket", bucket
                ] |> Map.ofList

            if accessKey.IsSome then creds <- creds.Add("aws_access_key_id", accessKey.Value)
            if secretKey.IsSome then creds <- creds.Add("aws_secret_access_key", secretKey.Value)
            if sessionToken.IsSome then creds <- creds.Add("aws_session_token", sessionToken.Value)
            if endpoint.IsSome then creds <- creds.Add("aws_endpoint", endpoint.Value)
            if allowHttp = Some true then creds <- creds.Add("aws_allow_http", "true")

            { CloudOptions.Default with Provider = CloudProvider.Aws; Credentials = creds }

        /// Azure Blob Storage
        static member Azure(
            accountName: string,
            ?accessKey: string,
            ?container: string,
            ?endpoint: string,
            ?allowHttp: bool) =

            let mutable creds = 
                [ "azure_storage_account_name", accountName ] |> Map.ofList

            if accessKey.IsSome then creds <- creds.Add("azure_storage_access_key", accessKey.Value)
            if container.IsSome then creds <- creds.Add("azure_container", container.Value)
            if endpoint.IsSome then creds <- creds.Add("azure_endpoint", endpoint.Value)
            if allowHttp = Some true then creds <- creds.Add("azure_allow_http", "true")

            { CloudOptions.Default with Provider = CloudProvider.Azure; Credentials = creds }

        /// Google Cloud Storage
        static member Gcp(?serviceAccountPath: string, ?bucket: string) =
            let mutable creds = Map.empty
            
            if serviceAccountPath.IsSome then creds <- creds.Add("google_service_account", serviceAccountPath.Value)
            if bucket.IsSome then creds <- creds.Add("google_bucket", bucket.Value)

            { CloudOptions.Default with Provider = CloudProvider.Gcp; Credentials = creds }
            
        /// HTTP
        static member Http(?headers: Map<string, string>) =
            let creds = defaultArg headers Map.empty
            { CloudOptions.Default with Provider = CloudProvider.Http; Credentials = creds }

        /// Hugging Face
        static member HuggingFace(?token: string) =
            let mutable creds = Map.empty
            if token.IsSome then creds <- creds.Add("hf_token", token.Value)
            { CloudOptions.Default with Provider = CloudProvider.HuggingFace; Credentials = creds }