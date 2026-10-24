#r "System.Xml.Linq"

open System
open System.IO
open System.Reflection
open System.Runtime.CompilerServices
open System.Xml.Linq
open System.Text
open System.Text.RegularExpressions
open System.Collections.Generic
open Microsoft.FSharp.Reflection

// ============================================================================
// 1. Configuration (Default to Debug build to avoid slow Release builds)
// ============================================================================
let args = fsi.CommandLineArgs |> Array.skip 1
let buildConfig = if args.Length > 0 && not (args.[0].StartsWith "-") then args.[0] else "Debug"
let targetFramework = "net8.0"

let dllPath = sprintf "Polars.FSharp/bin/%s/%s/Polars.FSharp.dll" buildConfig targetFramework
let xmlPath = sprintf "Polars.FSharp/bin/%s/%s/Polars.FSharp.xml" buildConfig targetFramework
let outputDir = "docs/api/fsharp_generated"

if not (File.Exists dllPath) then
    eprintfn "ERROR: Assembly not found at '%s'. Run 'dotnet build Polars.FSharp/Polars.FSharp.fsproj' first." dllPath
    exit 1

if not (File.Exists xmlPath) then
    eprintfn "ERROR: XML documentation not found at '%s'." xmlPath
    exit 1

let binDir = Path.GetDirectoryName(Path.GetFullPath dllPath)
let testBinDir = Path.GetFullPath(sprintf "Polars.FSharp.Tests/bin/%s/%s" buildConfig targetFramework)
let nugetCacheDir =
    let envNuget = Environment.GetEnvironmentVariable("NUGET_PACKAGES")
    if not (String.IsNullOrWhiteSpace envNuget) then envNuget
    else Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".nuget", "packages")

AppDomain.CurrentDomain.add_AssemblyResolve(fun _ resolveArgs ->
    let reqName = AssemblyName(resolveArgs.Name)
    let assemblyName = reqName.Name
    let candidates = [
        Path.Combine(binDir, assemblyName + ".dll")
        Path.Combine(testBinDir, assemblyName + ".dll")
    ]
    match candidates |> List.tryFind File.Exists with
    | Some p -> Assembly.LoadFrom p
    | None ->
        let pkgDir = Path.Combine(nugetCacheDir, assemblyName.ToLowerInvariant())
        if Directory.Exists pkgDir then
            let dlls =
                Directory.GetFiles(pkgDir, assemblyName + ".dll", SearchOption.AllDirectories)
                |> Array.filter (fun p -> p.Contains("lib" + string Path.DirectorySeparatorChar))
                |> Array.sortByDescending (fun p ->
                    let tfmScore =
                        if p.Contains("net8.0") then 3
                        elif p.Contains("net9.0") then 2
                        elif p.Contains("netstandard2.1") then 1
                        else 0
                    let ver =
                        try AssemblyName.GetAssemblyName(p).Version
                        with _ -> Version(0, 0)
                    (ver, tfmScore)
                )
            if dlls.Length > 0 then Assembly.LoadFrom(dlls.[0])
            else null
        else null
)

// ============================================================================
// 2. XML Documentation Parser (XDocument + Fallback Sanitizer + Dedent)
// ============================================================================
type DocMetadata = {
    Summary: string
    Params: Dictionary<string, string>
    TypeParams: Dictionary<string, string>
    Returns: string
    Remarks: string
    Examples: string list
    InheritDocCref: string option
}

module DocMetadata =
    let empty = {
        Summary = ""
        Params = Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        TypeParams = Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        Returns = ""
        Remarks = ""
        Examples = []
        InheritDocCref = None
    }

    let isEmpty (d: DocMetadata) =
        String.IsNullOrWhiteSpace d.Summary
        && d.Params.Count = 0
        && String.IsNullOrWhiteSpace d.Returns
        && String.IsNullOrWhiteSpace d.Remarks
        && d.Examples.IsEmpty

/// Dedent a multi-line code snippet while preserving relative indentation and newlines
let dedentCode (raw: string) =
    let normalized = raw.Replace("\r\n", "\n").Replace("\r", "\n")
    let lines = normalized.Split('\n')
    // Trim leading and trailing blank lines
    let startIdx = lines |> Array.tryFindIndex (fun l -> not (String.IsNullOrWhiteSpace l)) |> Option.defaultValue 0
    let endIdx = lines |> Array.tryFindIndexBack (fun l -> not (String.IsNullOrWhiteSpace l)) |> Option.defaultValue -1
    if endIdx < startIdx then
        ""
    else
        let slice = lines.[startIdx .. endIdx]
        let minIndent =
            slice
            |> Array.filter (fun l -> not (String.IsNullOrWhiteSpace l))
            |> Array.map (fun l -> l.Length - l.TrimStart(' ').Length)
            |> fun arr -> if arr.Length = 0 then 0 else Array.min arr
        slice
        |> Array.map (fun l ->
            if String.IsNullOrWhiteSpace l then ""
            elif l.Length >= minIndent then l.Substring(minIndent)
            else l.TrimStart())
        |> String.concat "\n"

let cleanCref (cref: string) =
    if String.IsNullOrWhiteSpace cref then "reference"
    else
        let noPrefix =
            if cref.Length > 2 && cref.[1] = ':' then cref.Substring(2) else cref
        let noParams =
            match noPrefix.IndexOf('(') with
            | -1 -> noPrefix
            | i -> noPrefix.Substring(0, i)
        let noGeneric = Regex.Replace(noParams, @"(`+\d+)", "")
        noGeneric.Replace("Polars.FSharp.", "").Replace("Polars.CSharp.", "")

let rec xmlNodeToMarkdown (preserveWhitespace: bool) (node: XNode) : string =
    match node with
    | :? XText as text ->
        if preserveWhitespace then text.Value
        else Regex.Replace(text.Value, @"\s+", " ").Replace("<", "&lt;").Replace(">", "&gt;")
    | :? XElement as el ->
        let tagName = el.Name.LocalName.ToLowerInvariant()
        match tagName with
        | "code" ->
            let rawCode = el.Nodes() |> Seq.map (xmlNodeToMarkdown true) |> String.concat ""
            let cleaned = dedentCode rawCode
            if String.IsNullOrWhiteSpace cleaned then ""
            else sprintf "\n```fsharp\n%s\n```\n" cleaned
        | "c" ->
            let inner = el.Nodes() |> Seq.map (xmlNodeToMarkdown true) |> String.concat ""
            sprintf "`%s`" (inner.Trim())
        | "para" ->
            let inner = el.Nodes() |> Seq.map (xmlNodeToMarkdown false) |> String.concat ""
            sprintf "\n\n%s\n\n" (inner.Trim())
        | "see" | "seealso" ->
            let crefAttr = el.Attribute(XName.Get "cref")
            let langwordAttr = el.Attribute(XName.Get "langword")
            if not (isNull crefAttr) then sprintf "`%s`" (cleanCref crefAttr.Value)
            elif not (isNull langwordAttr) then sprintf "`%s`" langwordAttr.Value
            else
                let inner = el.Nodes() |> Seq.map (xmlNodeToMarkdown false) |> String.concat ""
                if String.IsNullOrWhiteSpace inner then "" else sprintf "`%s`" (inner.Trim())
        | "paramref" | "typeparamref" ->
            let nameAttr = el.Attribute(XName.Get "name")
            if not (isNull nameAttr) then sprintf "`%s`" nameAttr.Value else ""
        | "list" ->
            let items =
                el.Elements(XName.Get "item")
                |> Seq.map (fun item ->
                    let desc = item.Nodes() |> Seq.map (xmlNodeToMarkdown false) |> String.concat ""
                    sprintf "- %s" (desc.Trim()))
                |> String.concat "\n"
            sprintf "\n\n%s\n\n" items
        | _ ->
            el.Nodes() |> Seq.map (xmlNodeToMarkdown preserveWhitespace) |> String.concat ""
    | _ -> ""

let sanitizeXmlFragment (xmlSnippet: string) =
    if String.IsNullOrEmpty xmlSnippet then ""
    else
        let s1 = Regex.Replace(xmlSnippet, @"&(?!(amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)", "&amp;")
        let validTags = "summary|remarks|param|returns|example|exception|see|seealso|c|code|para|list|item|description|term|paramref|typeparam|typeparamref|value|root|member|inheritdoc"
        let pattern = sprintf @"<(?!(/?(%s)\b))" validTags
        Regex.Replace(s1, pattern, "&lt;", RegexOptions.IgnoreCase)

let parseMemberElement (el: XElement) : (string * DocMetadata) option =
    let nameAttr = el.Attribute(XName.Get "name")
    if isNull nameAttr then None
    else
        let fullId = nameAttr.Value.Trim()
        let getElementMarkdown (childName: string) =
            el.Elements(XName.Get childName)
            |> Seq.map (fun child ->
                child.Nodes()
                |> Seq.map (xmlNodeToMarkdown false)
                |> String.concat ""
                |> fun s -> Regex.Replace(s.Trim(), @"\n{3,}", "\n\n"))
            |> Seq.filter (not << String.IsNullOrWhiteSpace)
            |> String.concat "\n\n"

        let paramDict = Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        for p in el.Elements(XName.Get "param") do
            let pName = p.Attribute(XName.Get "name")
            if not (isNull pName) && not (paramDict.ContainsKey pName.Value) then
                let desc = p.Nodes() |> Seq.map (xmlNodeToMarkdown false) |> String.concat "" |> fun s -> s.Trim()
                paramDict.Add(pName.Value, desc)

        let typeParamDict = Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        for tp in el.Elements(XName.Get "typeparam") do
            let tpName = tp.Attribute(XName.Get "name")
            if not (isNull tpName) && not (typeParamDict.ContainsKey tpName.Value) then
                let desc = tp.Nodes() |> Seq.map (xmlNodeToMarkdown false) |> String.concat "" |> fun s -> s.Trim()
                typeParamDict.Add(tpName.Value, desc)

        let examples =
            el.Elements(XName.Get "example")
            |> Seq.map (fun ex ->
                ex.Nodes()
                |> Seq.map (xmlNodeToMarkdown false)
                |> String.concat ""
                |> fun s -> s.Trim())
            |> Seq.filter (not << String.IsNullOrWhiteSpace)
            |> Seq.toList

        let inheritDocCref =
            let node = el.Element(XName.Get "inheritdoc")
            if isNull node then None
            else
                let cref = node.Attribute(XName.Get "cref")
                if isNull cref then Some "" else Some cref.Value

        Some(fullId, {
            Summary = getElementMarkdown "summary"
            Params = paramDict
            TypeParams = typeParamDict
            Returns = getElementMarkdown "returns"
            Remarks = getElementMarkdown "remarks"
            Examples = examples
            InheritDocCref = inheritDocCref
        })

printfn "Loading XML documentation from '%s'..." xmlPath
let xmlExactById = Dictionary<string, DocMetadata>(StringComparer.Ordinal)
let xmlByNoParams = Dictionary<string, ResizeArray<string * DocMetadata>>(StringComparer.Ordinal)

let indexDocEntry (fullId: string) (meta: DocMetadata) =
    xmlExactById.[fullId] <- meta
    let noParams =
        match fullId.IndexOf('(') with
        | -1 -> fullId
        | i -> fullId.Substring(0, i)
    if not (xmlByNoParams.ContainsKey noParams) then
        xmlByNoParams.[noParams] <- ResizeArray()
    xmlByNoParams.[noParams].Add((fullId, meta))

try
    let xdoc = XDocument.Load(xmlPath)
    for memberEl in xdoc.Descendants(XName.Get "member") do
        match parseMemberElement memberEl with
        | Some (id, meta) -> indexDocEntry id meta
        | None -> ()
    printfn "Successfully parsed XML via standard XDocument (%d entries)." xmlExactById.Count
with ex ->
    printfn "Standard XDocument.Load encountered malformed XML (%s). Falling back to per-member sanitizer..." ex.Message
    let rawXml = File.ReadAllText xmlPath
    let memberRegex = Regex(@"<member name=""([^""]+)"">.*?</member>", RegexOptions.Singleline)
    for m in memberRegex.Matches(rawXml) do
        try
            let safe = sanitizeXmlFragment m.Value
            let el = XElement.Parse(safe)
            match parseMemberElement el with
            | Some (id, meta) -> indexDocEntry id meta
            | None -> ()
        with memberEx ->
            eprintfn "Warning: Skipped malformed XML member '%s': %s" m.Groups.[1].Value memberEx.Message

// ============================================================================
// 3. .NET Reflection -> XML Documentation ID & Idiomatic F# Type Formatter
// ============================================================================
let rec getXmlDocTypeName (methodGenericArgs: Type[]) (typeGenericArgs: Type[]) (t: Type) : string =
    if t.IsGenericParameter then
        let mIdx = Array.IndexOf(methodGenericArgs, t)
        if mIdx >= 0 then sprintf "``%d" mIdx
        else
            let tIdx = Array.IndexOf(typeGenericArgs, t)
            if tIdx >= 0 then sprintf "`%d" tIdx else t.Name
    elif t.IsByRef then
        getXmlDocTypeName methodGenericArgs typeGenericArgs (t.GetElementType()) + "@"
    elif t.IsArray then
        let rank = t.GetArrayRank()
        let brackets = if rank = 1 then "[]" else "[" + String.replicate (rank - 1) "0:," + "0:]"
        getXmlDocTypeName methodGenericArgs typeGenericArgs (t.GetElementType()) + brackets
    elif t.IsGenericType then
        let def = t.GetGenericTypeDefinition()
        let fullName = (if isNull def.FullName then def.Name else def.FullName).Replace('+', '.')
        let baseName =
            match fullName.IndexOf('`') with
            | -1 -> fullName
            | i -> fullName.Substring(0, i)
        let args =
            t.GetGenericArguments()
            |> Array.map (getXmlDocTypeName methodGenericArgs typeGenericArgs)
            |> String.concat ","
        sprintf "%s{%s}" baseName args
    else
        (if isNull t.FullName then t.Name else t.FullName).Replace('+', '.')

let getMethodXmlId (m: MethodBase) : string =
    let declType = m.DeclaringType
    let typeName = (if isNull declType.FullName then declType.Name else declType.FullName).Replace('+', '.')
    let typeGenArgs = if declType.IsGenericType then declType.GetGenericArguments() else [||]
    let methodGenArgs = if m.IsGenericMethod then m.GetGenericArguments() else [||]
    let genSuffix = if methodGenArgs.Length > 0 then sprintf "``%d" methodGenArgs.Length else ""
    let memberName = if m.IsConstructor then "#ctor" else m.Name
    let pars = m.GetParameters()
    let paramPart =
        if pars.Length = 0 then ""
        else
            pars
            |> Array.map (fun p -> getXmlDocTypeName methodGenArgs typeGenArgs p.ParameterType)
            |> String.concat ","
            |> sprintf "(%s)"
    sprintf "M:%s.%s%s%s" typeName memberName genSuffix paramPart

let getPropertyXmlId (p: PropertyInfo) : string =
    let declType = p.DeclaringType
    let typeName = (if isNull declType.FullName then declType.Name else declType.FullName).Replace('+', '.')
    let typeGenArgs = if declType.IsGenericType then declType.GetGenericArguments() else [||]
    let indexPars = p.GetIndexParameters()
    let paramPart =
        if indexPars.Length = 0 then ""
        else
            indexPars
            |> Array.map (fun ip -> getXmlDocTypeName [||] typeGenArgs ip.ParameterType)
            |> String.concat ","
            |> sprintf "(%s)"
    sprintf "P:%s.%s%s" typeName p.Name paramPart

let getTypeXmlId (t: Type) : string =
    let fullName = (if isNull t.FullName then t.Name else t.FullName).Replace('+', '.')
    sprintf "T:%s" fullName

let lookupMethodDoc (m: MethodBase) : DocMetadata =
    let exactId = getMethodXmlId m
    match xmlExactById.TryGetValue exactId with
    | true, meta -> meta
    | false, _ ->
        let noParams =
            match exactId.IndexOf('(') with
            | -1 -> exactId
            | i -> exactId.Substring(0, i)
        match xmlByNoParams.TryGetValue noParams with
        | true, list when list.Count = 1 -> snd list.[0]
        | true, list ->
            let pCount = m.GetParameters().Length
            list
            |> Seq.tryFind (fun (id, _) ->
                let cCount = id |> Seq.filter ((=) ',') |> Seq.length
                let idParamCount = if id.Contains("(") then cCount + 1 else 0
                idParamCount = pCount)
            |> Option.map snd
            |> Option.defaultWith (fun () -> snd list.[0])
        | false, _ -> DocMetadata.empty

let lookupPropertyDoc (p: PropertyInfo) : DocMetadata =
    let propId = getPropertyXmlId p
    match xmlExactById.TryGetValue propId with
    | true, meta -> meta
    | false, _ ->
        if not (isNull p.GetMethod) then
            let getterDoc = lookupMethodDoc p.GetMethod
            if not (DocMetadata.isEmpty getterDoc) then getterDoc
            else
                // Also check M:TypeName.PropName (F# property getter without get_ in XML)
                let declName = p.DeclaringType.FullName.Replace('+', '.')
                let altId = sprintf "M:%s.%s" declName p.Name
                match xmlExactById.TryGetValue altId with
                | true, m -> m
                | false, _ -> DocMetadata.empty
        else DocMetadata.empty

let lookupTypeDoc (t: Type) : DocMetadata =
    let typeId = getTypeXmlId t
    match xmlExactById.TryGetValue typeId with
    | true, meta -> meta
    | false, _ -> DocMetadata.empty

// Idiomatic F# type formatting
let rec formatFSharpType (t: Type) : string =
    if isNull t then "unit"
    elif t.IsGenericParameter then
        sprintf "'%s" t.Name
    elif t.IsByRef then
        sprintf "byref<%s>" (formatFSharpType (t.GetElementType()))
    elif t.IsArray then
        let rank = t.GetArrayRank()
        let brackets = if rank = 1 then "[]" else "[" + String.replicate (rank - 1) "," + "]"
        sprintf "%s%s" (formatFSharpType (t.GetElementType())) brackets
    elif t.IsGenericType then
        let def = t.GetGenericTypeDefinition()
        let args = t.GetGenericArguments()
        if def = typedefof<option<_>> then
            sprintf "%s option" (formatFSharpType args.[0])
        elif def = typedefof<voption<_>> then
            sprintf "%s voption" (formatFSharpType args.[0])
        elif def = typedefof<list<_>> then
            sprintf "%s list" (formatFSharpType args.[0])
        elif def = typedefof<seq<_>> then
            sprintf "seq<%s>" (formatFSharpType args.[0])
        elif def = typedefof<Map<_, _>> then
            sprintf "Map<%s, %s>" (formatFSharpType args.[0]) (formatFSharpType args.[1])
        elif def = typedefof<Set<_>> then
            sprintf "Set<%s>" (formatFSharpType args.[0])
        elif def = typedefof<FSharpFunc<_, _>> then
            sprintf "(%s -> %s)" (formatFSharpType args.[0]) (formatFSharpType args.[1])
        elif def = typedefof<Nullable<_>> then
            sprintf "Nullable<%s>" (formatFSharpType args.[0])
        elif FSharpType.IsTuple t then
            FSharpType.GetTupleElements t
            |> Array.map formatFSharpType
            |> String.concat " * "
            |> sprintf "(%s)"
        else
            let rawName = t.Name
            let cleanBase =
                match rawName.IndexOf('`') with
                | -1 -> rawName
                | i -> rawName.Substring(0, i)
            let argStr = args |> Array.map formatFSharpType |> String.concat ", "
            sprintf "%s<%s>" cleanBase argStr
    else
        match t.FullName with
        | "Microsoft.FSharp.Core.Unit" | "System.Void" -> "unit"
        | "System.String" -> "string"
        | "System.Boolean" -> "bool"
        | "System.Byte" -> "byte"
        | "System.SByte" -> "sbyte"
        | "System.Int16" -> "int16"
        | "System.UInt16" -> "uint16"
        | "System.Int32" -> "int"
        | "System.UInt32" -> "uint32"
        | "System.Int64" -> "int64"
        | "System.UInt64" -> "uint64"
        | "System.Single" -> "float32"
        | "System.Double" -> "float"
        | "System.Decimal" -> "decimal"
        | "System.Char" -> "char"
        | "System.Object" -> "obj"
        | _ -> t.Name

let isOptionalParameter (p: ParameterInfo) =
    p.IsDefined(typeof<OptionalArgumentAttribute>, false)
    && p.ParameterType.IsGenericType
    && p.ParameterType.GetGenericTypeDefinition() = typedefof<option<_>>

let formatParameterForSig (p: ParameterInfo) =
    let pName = if String.IsNullOrEmpty p.Name then "arg" else p.Name
    if isOptionalParameter p then
        let innerType = p.ParameterType.GetGenericArguments().[0]
        sprintf "?%s: %s" pName (formatFSharpType innerType)
    elif p.IsDefined(typeof<ParamArrayAttribute>, false) then
        sprintf "[<ParamArray>] %s: %s" pName (formatFSharpType p.ParameterType)
    else
        sprintf "%s: %s" pName (formatFSharpType p.ParameterType)

let formatParameterTypeForTable (p: ParameterInfo) =
    if isOptionalParameter p then
        let innerType = p.ParameterType.GetGenericArguments().[0]
        sprintf "%s (optional)" (formatFSharpType innerType)
    else
        formatFSharpType p.ParameterType

// ============================================================================
// 4. Unified Member Representation & Automatic Assembly Discovery
// ============================================================================
type UnifiedProperty = {
    Name: string
    PropertyType: Type
    IndexParameters: ParameterInfo[]
    IsStatic: bool
    Doc: DocMetadata
    SourceModule: string option
}

type UnifiedMethod = {
    Name: string
    Method: MethodBase
    Parameters: ParameterInfo[]
    ReturnType: Type
    IsStatic: bool
    IsPipelineFunction: bool
    PipelinePrefix: string
    Doc: DocMetadata
    SourceModule: string option
}

type PageDescriptor = {
    PageId: string              // e.g., "DataFrame", "Expr.Str", "pl.cs"
    Title: string               // e.g., "DataFrame", "Expr.Str (StringOps)", "pl.cs"
    Category: string            // "Core Types" | "Expr Sub-namespaces" | "Series Sub-namespaces" | "DataTypeExpr Sub-namespaces" | "DSL & Modules" | "Delta & Catalog" | "Auxiliary Types"
    PrimaryType: Type
    CompanionModule: Type option
    Properties: ResizeArray<UnifiedProperty>
    StaticMethods: ResizeArray<UnifiedMethod>
    InstanceMethods: ResizeArray<UnifiedMethod>
    ModuleFunctions: ResizeArray<UnifiedMethod>
    SubNamespaceLinks: ResizeArray<string * string * string> // (PropName, PageId, Summary)
}

let assembly = Assembly.LoadFrom dllPath
let exportedTypes =
    assembly.GetExportedTypes()
    |> Array.filter (fun t ->
        not (t.Name.Contains("@"))
        && not (t.Name.Contains("$"))
        && not (t.Name.StartsWith("<")))

// Noise methods from System.Object / F# records / interfaces to exclude from docs
let ignoredMethodNames =
    HashSet<string>(
        [
            "GetType"; "ToString"; "Equals"; "GetHashCode"; "Finalize"; "MemberwiseClone"
            "Dispose"; "CloneHandle"; "get_Handle"; "ToDump"
        ],
        StringComparer.Ordinal
    )

let isAutoOpenModule (t: Type) =
    FSharpType.IsModule t && t.IsDefined(typeof<AutoOpenAttribute>, false)

let isExtensionContainer (t: Type) =
    t.IsDefined(typeof<ExtensionAttribute>, false)

// Map of Type.Name -> Type for primary non-AutoOpen types
let primaryTypesByName = Dictionary<string, Type>(StringComparer.Ordinal)
let companionModulesByTarget = Dictionary<string, Type>(StringComparer.Ordinal)

for t in exportedTypes do
    if FSharpType.IsModule t && t.Name.EndsWith("Module") then
        let baseName = t.Name.Substring(0, t.Name.Length - "Module".Length)
        companionModulesByTarget.[baseName] <- t
    elif not (isAutoOpenModule t) && not (isExtensionContainer t) then
        if not (primaryTypesByName.ContainsKey t.Name) then
            primaryTypesByName.[t.Name] <- t

// Also include AutoOpen modules that are primarily user-facing standalone functional modules (like Display, Query)
// We will detect any top-level functions inside AutoOpen modules that are NOT type extensions.

// Identify Sub-namespace wrapper structs/classes (e.g. DtOps, StringOps, SeriesStrNameSpace, ListNameSpace)
// We map SubNamespaceType -> (ParentTypeName, PropertyName, Summary)
let subNamespaceMap = Dictionary<Type, string * string * string>()

let registerSubNamespaceIfWrapper (parentTypeName: string) (propName: string) (propType: Type) (doc: DocMetadata) =
    if propType.Namespace = "Polars.FSharp"
       && (propType.Name.EndsWith("Ops") || propType.Name.EndsWith("NameSpace"))
       && propType.Name <> "MetaOpsSchemaExtensions" then
        if not (subNamespaceMap.ContainsKey propType) then
            subNamespaceMap.[propType] <- (parentTypeName, propName, doc.Summary)

// First pass: check intrinsic properties on primary types + extension properties in AutoOpen modules for Sub-namespaces
for kvp in primaryTypesByName do
    let t = kvp.Value
    if not (FSharpType.IsModule t) && not t.IsEnum then
        for p in t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance ||| BindingFlags.DeclaredOnly) do
            if p.GetIndexParameters().Length = 0 then
                let doc = lookupPropertyDoc p
                registerSubNamespaceIfWrapper t.Name p.Name p.PropertyType doc

for t in exportedTypes do
    if isAutoOpenModule t then
        for m in t.GetMethods(BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.DeclaredOnly) do
            // F# extension property getter pattern: <HostType>.get_<PropName>
            let parts = m.Name.Split('.')
            if parts.Length = 2 && parts.[1].StartsWith("get_") then
                let hostTypeName = parts.[0]
                let propName = parts.[1].Substring(4)
                let doc = lookupMethodDoc m
                registerSubNamespaceIfWrapper hostTypeName propName m.ReturnType doc

let isEditorHidden (memberOrType: MemberInfo) =
    let attr = memberOrType.GetCustomAttribute<System.ComponentModel.EditorBrowsableAttribute>()
    not (isNull attr) && attr.State = System.ComponentModel.EditorBrowsableState.Never

let isHandleConstructor (ctor: ConstructorInfo) (declaringType: Type) =
    let pars = ctor.GetParameters()
    subNamespaceMap.ContainsKey declaringType
    || (pars.Length = 1 && pars.[0].ParameterType.Name.EndsWith("Handle"))

// Build PageDescriptors for all relevant types & modules
let pagesByTypeName = Dictionary<string, PageDescriptor>(StringComparer.Ordinal)
let enumUnionTypes = ResizeArray<Type>()

let classifyPage (t: Type) : (string * string * string) option =
    if isEditorHidden t then
        None
    elif subNamespaceMap.ContainsKey t then
        let (parentName, propName, _) = subNamespaceMap.[t]
        let pageId = sprintf "%s.%s" parentName propName
        let title = sprintf "%s.%s (%s)" parentName propName t.Name
        let category = sprintf "%s Sub-namespaces" parentName
        Some (pageId, title, category)
    elif t.IsEnum || (FSharpType.IsUnion t && t.Name <> "DataType" && t.Name <> "Selector") then
        if t.DeclaringType = null then
            enumUnionTypes.Add t
        None // Aggregated into Enums.md
    elif t.Name.EndsWith("Enumerator") || t.Name.Contains("Enumerator`") || t.Name.StartsWith("FSharpRow") || t.Name = "IColumnExpr" || t.Name = "LitMechanism" then
        None // Skip internal/plumbing enumerator structs
    elif t.FullName = "Polars.FSharp.pl+cs" then
        Some ("pl.cs", "pl.cs (Column Selectors)", "DSL & Modules")
    elif t.DeclaringType <> null then
        None // Skip other nested helper types like ColSpec
    else
        let category =
            match t.Name with
            | "DataFrame" | "LazyFrame" | "Series" | "Expr" | "PolarsSchema" | "DataType" | "DataTypeExpr" -> "Core Types"
            | "pl" | "Selector" | "Config" | "Set" | "SqlContext" | "Query" | "Display" -> "DSL & Modules"
            | "Delta" | "UnityCatalog" | "DeltaMergeBuilder" | "DeltaTableFeatures" | "DeltaTableProperties" -> "Delta & Catalog"
            | _ when FSharpType.IsModule t -> "DSL & Modules"
            | _ -> "Auxiliary Types"
        Some (t.Name, t.Name, category)

for kvp in primaryTypesByName do
    let t = kvp.Value
    match classifyPage t with
    | None -> ()
    | Some (pageId, title, category) ->
        let companion =
            match companionModulesByTarget.TryGetValue t.Name with
            | true, cm -> Some cm
            | false, _ -> None
        pagesByTypeName.[t.Name] <- {
            PageId = pageId
            Title = title
            Category = category
            PrimaryType = t
            CompanionModule = companion
            Properties = ResizeArray()
            StaticMethods = ResizeArray()
            InstanceMethods = ResizeArray()
            ModuleFunctions = ResizeArray()
            SubNamespaceLinks = ResizeArray()
        }

// Populate SubNamespaceLinks on parent pages
for kvp in subNamespaceMap do
    let subType = kvp.Key
    let (parentName, propName, summary) = kvp.Value
    if pagesByTypeName.ContainsKey parentName && pagesByTypeName.ContainsKey subType.Name then
        let subPage = pagesByTypeName.[subType.Name]
        pagesByTypeName.[parentName].SubNamespaceLinks.Add((propName, subPage.PageId, summary))

// Populate intrinsic members for each page
for kvp in pagesByTypeName do
    let page = kvp.Value
    let t = page.PrimaryType

    if FSharpType.IsModule t then
        // Module functions & values
        for prop in t.GetProperties(BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.DeclaredOnly) do
            let doc = lookupPropertyDoc prop
            page.Properties.Add({
                Name = prop.Name
                PropertyType = prop.PropertyType
                IndexParameters = [||]
                IsStatic = true
                Doc = doc
                SourceModule = None
            })
        for m in t.GetMethods(BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.DeclaredOnly) do
            if not (m.Name.StartsWith("get_") || m.Name.StartsWith("set_")) && not (ignoredMethodNames.Contains m.Name) then
                let doc = lookupMethodDoc m
                page.ModuleFunctions.Add({
                    Name = m.Name
                    Method = m
                    Parameters = m.GetParameters()
                    ReturnType = m.ReturnType
                    IsStatic = true
                    IsPipelineFunction = true
                    PipelinePrefix = page.PageId
                    Doc = doc
                    SourceModule = None
                })
    else
        // Class / Struct constructors
        for ctor in t.GetConstructors(BindingFlags.Public ||| BindingFlags.Instance) do
            if not (isEditorHidden ctor) && not (isHandleConstructor ctor t) then
                let doc = lookupMethodDoc ctor
                page.StaticMethods.Add({
                    Name = "new"
                    Method = ctor
                    Parameters = ctor.GetParameters()
                    ReturnType = t
                    IsStatic = true
                    IsPipelineFunction = false
                    PipelinePrefix = ""
                    Doc = doc
                    SourceModule = None
                })

        // Class / Struct properties
        for prop in t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance ||| BindingFlags.Static ||| BindingFlags.DeclaredOnly) do
            if prop.Name <> "Handle" && not (isEditorHidden prop) && not (subNamespaceMap.ContainsKey prop.PropertyType) then
                let isStatic =
                    (not (isNull prop.GetMethod) && prop.GetMethod.IsStatic)
                    || (not (isNull prop.SetMethod) && prop.SetMethod.IsStatic)
                let doc = lookupPropertyDoc prop
                page.Properties.Add({
                    Name = prop.Name
                    PropertyType = prop.PropertyType
                    IndexParameters = prop.GetIndexParameters()
                    IsStatic = isStatic
                    Doc = doc
                    SourceModule = None
                })

        // Class / Struct methods
        for m in t.GetMethods(BindingFlags.Public ||| BindingFlags.Instance ||| BindingFlags.Static ||| BindingFlags.DeclaredOnly) do
            if not (m.Name.StartsWith("get_") || m.Name.StartsWith("set_"))
               && not (ignoredMethodNames.Contains m.Name)
               && not (isEditorHidden m) then
                let doc = lookupMethodDoc m
                let unified = {
                    Name = m.Name
                    Method = m
                    Parameters = m.GetParameters()
                    ReturnType = m.ReturnType
                    IsStatic = m.IsStatic
                    IsPipelineFunction = false
                    PipelinePrefix = ""
                    Doc = doc
                    SourceModule = None
                }
                if m.IsStatic then page.StaticMethods.Add(unified)
                else page.InstanceMethods.Add(unified)

        // Companion Module functions (e.g. DataFrameModule -> DataFrame.*)
        match page.CompanionModule with
        | Some cm ->
            for m in cm.GetMethods(BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.DeclaredOnly) do
                if not (m.Name.StartsWith("get_") || m.Name.StartsWith("set_")) then
                    let doc = lookupMethodDoc m
                    page.ModuleFunctions.Add({
                        Name = m.Name
                        Method = m
                        Parameters = m.GetParameters()
                        ReturnType = m.ReturnType
                        IsStatic = true
                        IsPipelineFunction = true
                        PipelinePrefix = t.Name
                        Doc = doc
                        SourceModule = Some cm.Name
                    })
        | None -> ()

// Second pass: Route all F# AutoOpen Type Extensions & C#-style [<Extension>] methods to their target Host Type!
for t in exportedTypes do
    if isAutoOpenModule t then
        for m in t.GetMethods(BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.DeclaredOnly) do
            let parts = m.Name.Split('.')
            if parts.Length >= 2 then
                // Pattern 1: <HostType>.<MemberName> (instance extension) or <HostType>.<MemberName>.Static (static extension)
                let hostTypeName = parts.[0]
                let memberRaw = parts.[1]
                let isStaticExt = parts.Length >= 3 && parts.[2] = "Static"
                if pagesByTypeName.ContainsKey hostTypeName then
                    let targetPage = pagesByTypeName.[hostTypeName]
                    let doc = lookupMethodDoc m
                    if memberRaw.StartsWith("get_") then
                        if not (subNamespaceMap.ContainsKey m.ReturnType) then
                            let propName = memberRaw.Substring(4)
                            let allPars = m.GetParameters()
                            let indexPars = if isStaticExt || allPars.Length = 0 then allPars else allPars.[1..]
                            targetPage.Properties.Add({
                                Name = propName
                                PropertyType = m.ReturnType
                                IndexParameters = indexPars
                                IsStatic = isStaticExt
                                Doc = doc
                                SourceModule = Some t.Name
                            })
                    elif not (memberRaw.StartsWith("set_")) && not (ignoredMethodNames.Contains memberRaw) && not (isEditorHidden m) then
                        let allPars = m.GetParameters()
                        let effectivePars = if isStaticExt || allPars.Length = 0 then allPars else allPars.[1..]
                        let unified = {
                            Name = memberRaw
                            Method = m
                            Parameters = effectivePars
                            ReturnType = m.ReturnType
                            IsStatic = isStaticExt
                            IsPipelineFunction = false
                            PipelinePrefix = ""
                            Doc = doc
                            SourceModule = Some t.Name
                        }
                        if isStaticExt then targetPage.StaticMethods.Add(unified)
                        else targetPage.InstanceMethods.Add(unified)
    elif isExtensionContainer t then
        for m in t.GetMethods(BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.DeclaredOnly) do
            let pars = m.GetParameters()
            if m.IsDefined(typeof<ExtensionAttribute>, false) && pars.Length > 0 then
                let hostType = pars.[0].ParameterType
                if pagesByTypeName.ContainsKey hostType.Name && not (ignoredMethodNames.Contains m.Name) then
                    let targetPage = pagesByTypeName.[hostType.Name]
                    let doc = lookupMethodDoc m
                    targetPage.InstanceMethods.Add({
                        Name = m.Name
                        Method = m
                        Parameters = pars.[1..]
                        ReturnType = m.ReturnType
                        IsStatic = false
                        IsPipelineFunction = false
                        PipelinePrefix = ""
                        Doc = doc
                        SourceModule = Some t.Name
                    })

// ============================================================================
// 5. Markdown Generation (Overloads Merged Under Single Heading)
// ============================================================================
let cleanForTable (raw: string) =
    if String.IsNullOrWhiteSpace raw then "-"
    else
        raw.Replace("|", "\\|")
           .Replace("\r", "")
           .Replace("\n\n", "<br>")
           .Replace("\n", " ")
           .Trim()

let formatOperatorDisplayName (opName: string) =
    match opName with
    | "op_Addition" -> "( + ) Addition"
    | "op_Subtraction" -> "( - ) Subtraction"
    | "op_Multiply" -> "( * ) Multiply"
    | "op_Division" -> "( / ) Division"
    | "op_Modulus" -> "( % ) Modulus"
    | "op_UnaryNegation" -> "( ~- ) UnaryNegation"
    | "op_DotGreater" -> "( .&gt; ) Greater Than"
    | "op_DotGreaterEquals" -> "( .&gt;= ) Greater Or Equal"
    | "op_DotLess" -> "( .&lt; ) Less Than"
    | "op_DotLessEquals" -> "( .&lt;= ) Less Or Equal"
    | "op_DotEqualsEquals" -> "( .== ) Equal"
    | "op_DotBangEquals" -> "( .!= ) Not Equal"
    | "op_DotAmpAmp" -> "( .&& ) Logical AND"
    | "op_DotBarBar" -> "( .|| ) Logical OR"
    | "op_DotMultiplyMultiply" -> "( .** ) Power"
    | "op_DotHat" -> "( .^ ) Bitwise XOR"
    | "op_Dereference" -> "( ! ) Logical NOT"
    | "op_LeftShift" -> "( <<< ) Left Shift"
    | "op_RightShift" -> "( >>> ) Right Shift"
    | "op_BitwiseAnd" -> "( &&& ) Bitwise AND"
    | "op_BitwiseOr" -> "( ||| ) Bitwise OR"
    | "op_ExclusiveOr" -> "( ^^^ ) Exclusive OR"
    | "op_LogicalNot" -> "( ~~~ ) Bitwise NOT"
    | _ -> opName

let formatMethodSignature (m: UnifiedMethod) : string =
    let retStr = formatFSharpType m.ReturnType
    let parStrs = m.Parameters |> Array.map formatParameterForSig
    if m.IsPipelineFunction then
        let args =
            if parStrs.Length = 0 then "unit"
            else String.concat " -> " parStrs
        sprintf "%s.%s : %s -> %s" m.PipelinePrefix m.Name args retStr
    else
        let prefix =
            if m.Name = "new" then "new"
            elif m.IsStatic then sprintf "static member %s" m.Name
            else sprintf "member %s" m.Name
        if parStrs.Length = 0 then
            sprintf "%s : unit -> %s" prefix retStr
        elif parStrs.Length <= 3 then
            sprintf "%s(%s) : %s" prefix (String.concat ", " parStrs) retStr
        else
            let indented = parStrs |> Array.map (sprintf "    %s") |> String.concat ",\n"
            sprintf "%s(\n%s\n) : %s" prefix indented retStr

let renderMergedMethodGroup (sb: StringBuilder) (groupName: string) (overloads: UnifiedMethod[]) =
    let displayHeading =
        if groupName.StartsWith("op_") then formatOperatorDisplayName groupName
        else groupName
    sb.AppendLine(sprintf "### %s" displayHeading) |> ignore
    sb.AppendLine() |> ignore

    // 1. Collect unique non-empty summaries
    let summaries =
        overloads
        |> Array.map (fun o -> o.Doc.Summary)
        |> Array.filter (not << String.IsNullOrWhiteSpace)
        |> Array.distinct
    if summaries.Length > 0 then
        for s in summaries do
            sb.AppendLine s |> ignore
            sb.AppendLine() |> ignore

    // 2. Render all overload signatures in a single fsharp block
    sb.AppendLine "```fsharp" |> ignore
    let signatures = overloads |> Array.map formatMethodSignature |> Array.distinct
    for i = 0 to signatures.Length - 1 do
        sb.AppendLine signatures.[i] |> ignore
        if i < signatures.Length - 1 && signatures.[i].Contains("\n") then
            sb.AppendLine() |> ignore
    sb.AppendLine "```" |> ignore

    // 3. Render merged Parameters table across overloads
    let mergedParams = ResizeArray<string * string * string>()
    let seenParams = HashSet<string>(StringComparer.OrdinalIgnoreCase)
    for o in overloads do
        for p in o.Parameters do
            let pName = if String.IsNullOrEmpty p.Name then "arg" else p.Name
            let pType = formatParameterTypeForTable p
            let desc =
                match o.Doc.Params.TryGetValue pName with
                | true, d when not (String.IsNullOrWhiteSpace d) -> d
                | _ -> "-"
            if not (seenParams.Contains pName) then
                seenParams.Add pName |> ignore
                mergedParams.Add((pName, pType, desc))
            else
                // Upgrade description if a later overload has documentation for this parameter
                let idx = mergedParams.FindIndex(fun (n, _, _) -> String.Equals(n, pName, StringComparison.OrdinalIgnoreCase))
                if idx >= 0 then
                    let (oldN, oldT, oldD) = mergedParams.[idx]
                    let combinedType =
                        if oldT.Contains(pType) then oldT else sprintf "%s / %s" oldT pType
                    let bestDesc = if oldD = "-" && desc <> "-" then desc else oldD
                    mergedParams.[idx] <- (oldN, combinedType, bestDesc)

    if mergedParams.Count > 0 then
        sb.AppendLine() |> ignore
        sb.AppendLine "**Parameters**" |> ignore
        sb.AppendLine() |> ignore
        sb.AppendLine "| Name | Type | Description |" |> ignore
        sb.AppendLine "| :--- | :--- | :--- |" |> ignore
        for (pName, pType, pDesc) in mergedParams do
            sb.AppendLine(sprintf "| `%s` | `%s` | %s |" pName pType (cleanForTable pDesc)) |> ignore

    // 4. Merged Returns
    let returns =
        overloads
        |> Array.map (fun o -> o.Doc.Returns)
        |> Array.filter (not << String.IsNullOrWhiteSpace)
        |> Array.distinct
    if returns.Length > 0 then
        sb.AppendLine() |> ignore
        sb.AppendLine(sprintf "**Returns:** %s" (String.concat " / " returns)) |> ignore

    // 5. Merged Examples
    let examples =
        overloads
        |> Seq.collect (fun o -> o.Doc.Examples)
        |> Seq.filter (not << String.IsNullOrWhiteSpace)
        |> Seq.distinct
        |> Seq.toArray
    if examples.Length > 0 then
        sb.AppendLine() |> ignore
        sb.AppendLine "**Example**" |> ignore
        sb.AppendLine() |> ignore
        for ex in examples do
            if ex.Contains("```") then
                sb.AppendLine ex |> ignore
            else
                sb.AppendLine "```fsharp" |> ignore
                sb.AppendLine (dedentCode ex) |> ignore
                sb.AppendLine "```" |> ignore

    // 6. Merged Remarks
    let remarks =
        overloads
        |> Array.map (fun o -> o.Doc.Remarks)
        |> Array.filter (not << String.IsNullOrWhiteSpace)
        |> Array.distinct
    if remarks.Length > 0 then
        sb.AppendLine() |> ignore
        sb.AppendLine "> [!NOTE]" |> ignore
        for r in remarks do
            for line in r.Split('\n') do
                sb.AppendLine(sprintf "> %s" line) |> ignore

    sb.AppendLine() |> ignore
    sb.AppendLine "---" |> ignore
    sb.AppendLine() |> ignore

// Clean output directory
if Directory.Exists outputDir then Directory.Delete(outputDir, true)
Directory.CreateDirectory outputDir |> ignore

let generatedPages = ResizeArray<PageDescriptor>()

for kvp in pagesByTypeName |> Seq.sortBy (fun k -> k.Value.PageId) do
    let page = kvp.Value
    let hasContent =
        page.Properties.Count > 0
        || page.StaticMethods.Count > 0
        || page.InstanceMethods.Count > 0
        || page.ModuleFunctions.Count > 0
        || page.SubNamespaceLinks.Count > 0

    if hasContent then
        generatedPages.Add page
        let sb = StringBuilder()
        sb.AppendLine "---" |> ignore
        sb.AppendLine(sprintf "uid: Polars.FSharp.%s" page.PageId) |> ignore
        let safeYamlTitle = page.Title.Replace("\"", "\\\"")
        sb.AppendLine(sprintf "title: \"%s\"" safeYamlTitle) |> ignore
        sb.AppendLine "---" |> ignore
        sb.AppendLine() |> ignore
        sb.AppendLine(sprintf "# %s" (page.Title.Replace("<", "&lt;").Replace(">", "&gt;"))) |> ignore
        sb.AppendLine() |> ignore
        sb.AppendLine(sprintf "Namespace: `%s`" page.PrimaryType.Namespace) |> ignore
        sb.AppendLine() |> ignore

        let typeDoc = lookupTypeDoc page.PrimaryType
        if not (String.IsNullOrWhiteSpace typeDoc.Summary) then
            sb.AppendLine typeDoc.Summary |> ignore
            sb.AppendLine() |> ignore

        // Section 1: Sub-namespaces (links to child pages like Expr.Str.md)
        if page.SubNamespaceLinks.Count > 0 then
            sb.AppendLine "## Sub-namespaces" |> ignore
            sb.AppendLine() |> ignore
            sb.AppendLine "| Sub-namespace | Page | Description |" |> ignore
            sb.AppendLine "| :--- | :--- | :--- |" |> ignore
            for (propName, subPageId, summary) in page.SubNamespaceLinks |> Seq.sortBy (fun (n, _, _) -> n) do
                sb.AppendLine(sprintf "| `.%s` | [%s](%s.md) | %s |" propName subPageId subPageId (cleanForTable summary)) |> ignore
            sb.AppendLine() |> ignore

        // Section 2: Properties & Indexers
        if page.Properties.Count > 0 then
            sb.AppendLine "## Properties & Indexers" |> ignore
            sb.AppendLine() |> ignore
            sb.AppendLine "| Property | Type | Description |" |> ignore
            sb.AppendLine "| :--- | :--- | :--- |" |> ignore
            let groupedProps =
                page.Properties
                |> Seq.groupBy (fun p -> p.Name)
                |> Seq.sortBy fst
            for (propName, propOverloads) in groupedProps do
                let overloadsArr = Seq.toArray propOverloads
                let displayProp =
                    if overloadsArr |> Array.exists (fun p -> p.IndexParameters.Length > 0) then
                        let idxSigs =
                            overloadsArr
                            |> Array.map (fun p ->
                                if p.IndexParameters.Length = 0 then propName
                                else
                                    let idxStr = p.IndexParameters |> Array.map formatParameterForSig |> String.concat ", "
                                    sprintf ".[%s]" idxStr)
                            |> Array.distinct
                            |> String.concat " / "
                        idxSigs
                    else propName
                let propTypes =
                    overloadsArr
                    |> Array.map (fun p -> formatFSharpType p.PropertyType)
                    |> Array.distinct
                    |> String.concat " / "
                let propDesc =
                    overloadsArr
                    |> Array.map (fun p -> p.Doc.Summary)
                    |> Array.filter (not << String.IsNullOrWhiteSpace)
                    |> Array.distinct
                    |> String.concat " "
                sb.AppendLine(sprintf "| `%s` | `%s` | %s |" displayProp propTypes (cleanForTable propDesc)) |> ignore
            sb.AppendLine() |> ignore

        // Section 3: Constructors & Static Methods
        if page.StaticMethods.Count > 0 then
            let normalStatics, operators =
                page.StaticMethods
                |> Seq.toArray
                |> Array.partition (fun m -> not (m.Name.StartsWith("op_")))

            if normalStatics.Length > 0 then
                sb.AppendLine "## Constructors & Static Methods" |> ignore
                sb.AppendLine() |> ignore
                let groups =
                    normalStatics
                    |> Array.groupBy (fun m -> m.Name)
                    |> Array.sortBy (fun (name, _) -> if name = "new" then "" else name)
                for (name, group) in groups do
                    renderMergedMethodGroup sb name group

            if operators.Length > 0 then
                sb.AppendLine "## Operators" |> ignore
                sb.AppendLine() |> ignore
                let opGroups =
                    operators
                    |> Array.groupBy (fun m -> m.Name)
                    |> Array.sortBy fst
                for (name, group) in opGroups do
                    renderMergedMethodGroup sb name group

        // Section 4: Instance Methods
        if page.InstanceMethods.Count > 0 then
            sb.AppendLine "## Instance Methods" |> ignore
            sb.AppendLine() |> ignore
            let groups =
                page.InstanceMethods
                |> Seq.toArray
                |> Array.groupBy (fun m -> m.Name)
                |> Array.sortBy fst
            for (name, group) in groups do
                renderMergedMethodGroup sb name group

        // Section 5: Pipeline Module Functions (e.g. DataFrame.*, LazyFrame.*, Series.*, pl.*)
        if page.ModuleFunctions.Count > 0 then
            let sectionTitle =
                if FSharpType.IsModule page.PrimaryType then "## Module Functions"
                else sprintf "## Pipeline Module Functions (`%s.*`)" page.PageId
            sb.AppendLine sectionTitle |> ignore
            sb.AppendLine() |> ignore
            let groups =
                page.ModuleFunctions
                |> Seq.toArray
                |> Array.groupBy (fun m -> m.Name)
                |> Array.sortBy fst
            for (name, group) in groups do
                let heading = sprintf "%s.%s" page.PageId name
                renderMergedMethodGroup sb heading group

        let outPath = Path.Combine(outputDir, sprintf "%s.md" page.PageId)
        File.WriteAllText(outPath, sb.ToString())
        printfn "  -> Generated %-28s (Props: %2d, Static: %2d, Instance: %3d, ModuleFns: %3d)"
            (page.PageId + ".md")
            page.Properties.Count
            page.StaticMethods.Count
            page.InstanceMethods.Count
            page.ModuleFunctions.Count

// Generate Enums.md aggregating all Discriminated Unions & Enums
if enumUnionTypes.Count > 0 then
    let sbEnums = StringBuilder()
    sbEnums.AppendLine "---" |> ignore
    sbEnums.AppendLine "uid: Polars.FSharp.Enums" |> ignore
    sbEnums.AppendLine "title: Enums & Discriminated Unions" |> ignore
    sbEnums.AppendLine "---" |> ignore
    sbEnums.AppendLine() |> ignore
    sbEnums.AppendLine "# Enums & Discriminated Unions" |> ignore
    sbEnums.AppendLine() |> ignore
    sbEnums.AppendLine "Namespace: `Polars.FSharp`" |> ignore
    sbEnums.AppendLine() |> ignore

    for t in enumUnionTypes |> Seq.sortBy (fun x -> x.Name) do
        sbEnums.AppendLine(sprintf "## %s" t.Name) |> ignore
        sbEnums.AppendLine() |> ignore
        let tDoc = lookupTypeDoc t
        if not (String.IsNullOrWhiteSpace tDoc.Summary) then
            sbEnums.AppendLine tDoc.Summary |> ignore
            sbEnums.AppendLine() |> ignore

        sbEnums.AppendLine "```fsharp" |> ignore
        sbEnums.AppendLine(sprintf "type %s =" t.Name) |> ignore
        if FSharpType.IsUnion t then
            for uc in FSharpType.GetUnionCases t do
                let fields = uc.GetFields()
                if fields.Length = 0 then
                    sbEnums.AppendLine(sprintf "    | %s" uc.Name) |> ignore
                else
                    let fStr =
                        fields
                        |> Array.map (fun f ->
                            if f.Name.StartsWith("Item") then formatFSharpType f.PropertyType
                            else sprintf "%s: %s" f.Name (formatFSharpType f.PropertyType))
                        |> String.concat " * "
                    sbEnums.AppendLine(sprintf "    | %s of %s" uc.Name fStr) |> ignore
        elif t.IsEnum then
            for name in Enum.GetNames(t) do
                sbEnums.AppendLine(sprintf "    | %s" name) |> ignore
        sbEnums.AppendLine "```" |> ignore
        sbEnums.AppendLine() |> ignore
        sbEnums.AppendLine "---" |> ignore
        sbEnums.AppendLine() |> ignore

    File.WriteAllText(Path.Combine(outputDir, "Enums.md"), sbEnums.ToString())
    printfn "  -> Generated %-28s (%d union/enum types)" "Enums.md" enumUnionTypes.Count

// ============================================================================
// 6. Generate Structured TOC (toc.yml) with Sub-namespace Hierarchy
// ============================================================================
let sbToc = StringBuilder()

let categoryOrder = [
    "Core Types"
    "Expr Sub-namespaces"
    "Series Sub-namespaces"
    "DataTypeExpr Sub-namespaces"
    "DSL & Modules"
    "Delta & Catalog"
    "Auxiliary Types"
]

for cat in categoryOrder do
    let pagesInCat =
        generatedPages
        |> Seq.filter (fun p -> p.Category = cat)
        |> Seq.sortBy (fun p -> p.PageId)
        |> Seq.toArray
    if pagesInCat.Length > 0 || (cat = "Auxiliary Types" && enumUnionTypes.Count > 0) then
        sbToc.AppendLine(sprintf "- name: %s" cat) |> ignore
        sbToc.AppendLine("  items:") |> ignore
        for p in pagesInCat do
            sbToc.AppendLine(sprintf "  - name: %s" p.PageId) |> ignore
            sbToc.AppendLine(sprintf "    href: %s.md" p.PageId) |> ignore
        if cat = "Auxiliary Types" && enumUnionTypes.Count > 0 then
            sbToc.AppendLine("  - name: Enums") |> ignore
            sbToc.AppendLine("    href: Enums.md") |> ignore

File.WriteAllText(Path.Combine(outputDir, "toc.yml"), sbToc.ToString())
printfn "\nDone! Generated %d documentation pages (+ Enums.md) in '%s'." generatedPages.Count outputDir
