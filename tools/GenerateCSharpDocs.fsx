#r "System.Xml.Linq"

open System
open System.IO
open System.Reflection
open System.Runtime.CompilerServices
open System.Xml.Linq
open System.Xml.XPath
open System.Text
open System.Text.RegularExpressions
open System.Collections.Generic

// ============================================================================
// 1. Configuration (Default to Debug build to avoid slow Release builds)
// ============================================================================
let repoRoot = Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, ".."))
let args = fsi.CommandLineArgs |> Array.skip 1
let buildConfig = if args.Length > 0 && not (args.[0].StartsWith "-") then args.[0] else "Debug"
let targetFramework = "net8.0"

let dllPath = Path.Combine(repoRoot, sprintf "Polars.CSharp/bin/%s/%s/Polars.CSharp.dll" buildConfig targetFramework)
let xmlPath = Path.Combine(repoRoot, sprintf "Polars.CSharp/bin/%s/%s/Polars.CSharp.xml" buildConfig targetFramework)
let outputDir = Path.Combine(repoRoot, "docs/api/csharp_generated")

if not (File.Exists dllPath) then
    eprintfn "ERROR: Assembly not found at '%s'. Run 'dotnet build Polars.CSharp/Polars.CSharp.csproj' first." dllPath
    exit 1

if not (File.Exists xmlPath) then
    eprintfn "ERROR: XML documentation not found at '%s'." xmlPath
    exit 1

let binDir = Path.GetDirectoryName(Path.GetFullPath dllPath)
let testBinDir = Path.Combine(repoRoot, sprintf "Polars.CSharp.Tests/bin/%s/%s" buildConfig targetFramework)
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
// 2. XML Documentation Parser with Full <inheritdoc> Resolution
// ============================================================================
type DocMetadata = {
    Summary: string
    Params: Dictionary<string, string>
    TypeParams: Dictionary<string, string>
    Returns: string
    Remarks: string
    Examples: string list
}

module DocMetadata =
    let empty = {
        Summary = ""
        Params = Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        TypeParams = Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        Returns = ""
        Remarks = ""
        Examples = []
    }

    let isEmpty (d: DocMetadata) =
        String.IsNullOrWhiteSpace d.Summary
        && d.Params.Count = 0
        && String.IsNullOrWhiteSpace d.Returns
        && String.IsNullOrWhiteSpace d.Remarks
        && d.Examples.IsEmpty

let dedentCode (raw: string) =
    let normalized = raw.Replace("\r\n", "\n").Replace("\r", "\n")
    let lines = normalized.Split('\n')
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
        noGeneric.Replace("Polars.CSharp.", "").Replace("Polars.FSharp.", "")

printfn "Loading XML documentation from '%s'..." xmlPath
let rawElementsById = Dictionary<string, XElement>(StringComparer.Ordinal)
let rawElementsByNoParams = Dictionary<string, ResizeArray<string * XElement>>(StringComparer.Ordinal)

let xdoc = XDocument.Load(xmlPath)
for memberEl in xdoc.Descendants(XName.Get "member") do
    let nameAttr = memberEl.Attribute(XName.Get "name")
    if not (isNull nameAttr) then
        let fullId = nameAttr.Value.Trim()
        rawElementsById.[fullId] <- memberEl
        let noParams =
            match fullId.IndexOf('(') with
            | -1 -> fullId
            | i -> fullId.Substring(0, i)
        if not (rawElementsByNoParams.ContainsKey noParams) then
            rawElementsByNoParams.[noParams] <- ResizeArray()
        rawElementsByNoParams.[noParams].Add((fullId, memberEl))

let resolveTargetElement (cref: string) : XElement option =
    if String.IsNullOrWhiteSpace cref then None
    else
        match rawElementsById.TryGetValue cref with
        | true, el -> Some el
        | false, _ ->
            let noParams =
                match cref.IndexOf('(') with
                | -1 -> cref
                | i -> cref.Substring(0, i)
            match rawElementsByNoParams.TryGetValue noParams with
            | true, list when list.Count > 0 -> Some (snd list.[0])
            | _ -> None

let rec xmlNodeToMarkdown (depth: int) (preserveWhitespace: bool) (parentParamName: string option) (node: XNode) : string =
    if depth > 6 then ""
    else
        match node with
        | :? XText as text ->
            if preserveWhitespace then text.Value
            else Regex.Replace(text.Value, @"\s+", " ").Replace("<", "&lt;").Replace(">", "&gt;")
        | :? XElement as el ->
            let tagName = el.Name.LocalName.ToLowerInvariant()
            match tagName with
            | "inheritdoc" ->
                let crefAttr = el.Attribute(XName.Get "cref")
                let pathAttr = el.Attribute(XName.Get "path")
                let crefVal = if isNull crefAttr then "" else crefAttr.Value
                match resolveTargetElement crefVal with
                | None -> ""
                | Some targetEl ->
                    if not (isNull pathAttr) && not (String.IsNullOrWhiteSpace pathAttr.Value) then
                        // e.g. path="/param[@name='windowSize']/node()" or path="/summary"
                        let xpath = pathAttr.Value.TrimStart('/')
                        let mParam = Regex.Match(xpath, @"param\[@name='([^']+)'\]")
                        if mParam.Success then
                            let pName = mParam.Groups.[1].Value
                            let pEl =
                                targetEl.Elements(XName.Get "param")
                                |> Seq.tryFind (fun p ->
                                    let a = p.Attribute(XName.Get "name")
                                    not (isNull a) && a.Value = pName)
                            match pEl with
                            | Some pNode ->
                                pNode.Nodes() |> Seq.map (xmlNodeToMarkdown (depth + 1) false (Some pName)) |> String.concat ""
                            | None -> ""
                        elif xpath.StartsWith("summary") then
                            let sNode = targetEl.Element(XName.Get "summary")
                            if isNull sNode then ""
                            else sNode.Nodes() |> Seq.map (xmlNodeToMarkdown (depth + 1) false None) |> String.concat ""
                        else ""
                    else
                        match parentParamName with
                        | Some pName ->
                            let pEl =
                                targetEl.Elements(XName.Get "param")
                                |> Seq.tryFind (fun p ->
                                    let a = p.Attribute(XName.Get "name")
                                    not (isNull a) && a.Value = pName)
                            match pEl with
                            | Some pNode ->
                                pNode.Nodes() |> Seq.map (xmlNodeToMarkdown (depth + 1) false (Some pName)) |> String.concat ""
                            | None -> ""
                        | None ->
                            let sNode = targetEl.Element(XName.Get "summary")
                            if isNull sNode then ""
                            else sNode.Nodes() |> Seq.map (xmlNodeToMarkdown (depth + 1) false None) |> String.concat ""
            | "code" ->
                let rawCode = el.Nodes() |> Seq.map (xmlNodeToMarkdown (depth + 1) true None) |> String.concat ""
                let cleaned = dedentCode rawCode
                if String.IsNullOrWhiteSpace cleaned then ""
                else sprintf "\n```csharp\n%s\n```\n" cleaned
            | "c" ->
                let inner = el.Nodes() |> Seq.map (xmlNodeToMarkdown (depth + 1) true None) |> String.concat ""
                sprintf "`%s`" (inner.Trim())
            | "para" ->
                let inner = el.Nodes() |> Seq.map (xmlNodeToMarkdown (depth + 1) false None) |> String.concat ""
                sprintf "\n\n%s\n\n" (inner.Trim())
            | "see" | "seealso" ->
                let crefAttr = el.Attribute(XName.Get "cref")
                let langwordAttr = el.Attribute(XName.Get "langword")
                if not (isNull crefAttr) then sprintf "`%s`" (cleanCref crefAttr.Value)
                elif not (isNull langwordAttr) then sprintf "`%s`" langwordAttr.Value
                else
                    let inner = el.Nodes() |> Seq.map (xmlNodeToMarkdown (depth + 1) false None) |> String.concat ""
                    if String.IsNullOrWhiteSpace inner then "" else sprintf "`%s`" (inner.Trim())
            | "paramref" | "typeparamref" ->
                let nameAttr = el.Attribute(XName.Get "name")
                if not (isNull nameAttr) then sprintf "`%s`" nameAttr.Value else ""
            | "list" ->
                let items =
                    el.Elements(XName.Get "item")
                    |> Seq.map (fun item ->
                        let desc = item.Nodes() |> Seq.map (xmlNodeToMarkdown (depth + 1) false None) |> String.concat ""
                        sprintf "- %s" (desc.Trim()))
                    |> String.concat "\n"
                sprintf "\n\n%s\n\n" items
            | _ ->
                el.Nodes() |> Seq.map (xmlNodeToMarkdown (depth + 1) preserveWhitespace parentParamName) |> String.concat ""
        | _ -> ""

let rec parseMemberWithInheritance (depth: int) (el: XElement) : DocMetadata =
    if depth > 6 then DocMetadata.empty
    else
        let getElementMarkdown (childName: string) =
            el.Elements(XName.Get childName)
            |> Seq.map (fun child ->
                child.Nodes()
                |> Seq.map (xmlNodeToMarkdown 0 false None)
                |> String.concat ""
                |> fun s -> Regex.Replace(s.Trim(), @"\n{3,}", "\n\n"))
            |> Seq.filter (not << String.IsNullOrWhiteSpace)
            |> Seq.distinct
            |> String.concat "\n\n"

        let paramDict = Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        for p in el.Elements(XName.Get "param") do
            let pName = p.Attribute(XName.Get "name")
            if not (isNull pName) && not (paramDict.ContainsKey pName.Value) then
                let desc =
                    p.Nodes()
                    |> Seq.map (xmlNodeToMarkdown 0 false (Some pName.Value))
                    |> String.concat ""
                    |> fun s -> s.Trim()
                if not (String.IsNullOrWhiteSpace desc) then
                    paramDict.Add(pName.Value, desc)

        let typeParamDict = Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        for tp in el.Elements(XName.Get "typeparam") do
            let tpName = tp.Attribute(XName.Get "name")
            if not (isNull tpName) && not (typeParamDict.ContainsKey tpName.Value) then
                let desc = tp.Nodes() |> Seq.map (xmlNodeToMarkdown 0 false None) |> String.concat "" |> fun s -> s.Trim()
                if not (String.IsNullOrWhiteSpace desc) then
                    typeParamDict.Add(tpName.Value, desc)

        let examples =
            el.Elements(XName.Get "example")
            |> Seq.map (fun ex ->
                ex.Nodes()
                |> Seq.map (xmlNodeToMarkdown 0 false None)
                |> String.concat ""
                |> fun s -> s.Trim())
            |> Seq.filter (not << String.IsNullOrWhiteSpace)
            |> Seq.toList

        let mutable summary = getElementMarkdown "summary"
        let mutable returns = getElementMarkdown "returns"
        let mutable remarks = getElementMarkdown "remarks"
        let mutable finalExamples = examples

        // Top-level <inheritdoc cref="..." />
        for inh in el.Elements(XName.Get "inheritdoc") do
            let crefAttr = inh.Attribute(XName.Get "cref")
            let pathAttr = inh.Attribute(XName.Get "path")
            let crefVal = if isNull crefAttr then "" else crefAttr.Value
            match resolveTargetElement crefVal with
            | Some targetEl ->
                let baseDoc = parseMemberWithInheritance (depth + 1) targetEl
                let pathVal = if isNull pathAttr then "" else pathAttr.Value
                if String.IsNullOrWhiteSpace pathVal then
                    if String.IsNullOrWhiteSpace summary then summary <- baseDoc.Summary
                    if String.IsNullOrWhiteSpace returns then returns <- baseDoc.Returns
                    if String.IsNullOrWhiteSpace remarks then remarks <- baseDoc.Remarks
                    if finalExamples.IsEmpty then finalExamples <- baseDoc.Examples
                    for kv in baseDoc.Params do
                        if not (paramDict.ContainsKey kv.Key) then
                            paramDict.Add(kv.Key, kv.Value)
                    for kv in baseDoc.TypeParams do
                        if not (typeParamDict.ContainsKey kv.Key) then
                            typeParamDict.Add(kv.Key, kv.Value)
                elif pathVal.Contains("summary") && String.IsNullOrWhiteSpace summary then
                    summary <- baseDoc.Summary
                elif pathVal.Contains("param") then
                    for kv in baseDoc.Params do
                        if not (paramDict.ContainsKey kv.Key) then
                            paramDict.Add(kv.Key, kv.Value)
            | None -> ()

        {
            Summary = summary
            Params = paramDict
            TypeParams = typeParamDict
            Returns = returns
            Remarks = remarks
            Examples = finalExamples
        }

let xmlExactById = Dictionary<string, DocMetadata>(StringComparer.Ordinal)
let xmlByNoParams = Dictionary<string, ResizeArray<string * DocMetadata>>(StringComparer.Ordinal)

for kvp in rawElementsById do
    let fullId = kvp.Key
    let meta = parseMemberWithInheritance 0 kvp.Value
    xmlExactById.[fullId] <- meta
    let noParams =
        match fullId.IndexOf('(') with
        | -1 -> fullId
        | i -> fullId.Substring(0, i)
    if not (xmlByNoParams.ContainsKey noParams) then
        xmlByNoParams.[noParams] <- ResizeArray()
    xmlByNoParams.[noParams].Add((fullId, meta))

printfn "Successfully parsed %d XML entries (including inheritdoc resolution)." xmlExactById.Count

// ============================================================================
// 3. .NET Reflection -> XML Documentation ID & Idiomatic C# Type Formatter
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
        let noParams =
            match propId.IndexOf('(') with
            | -1 -> propId
            | i -> propId.Substring(0, i)
        match xmlByNoParams.TryGetValue noParams with
        | true, list when list.Count > 0 -> snd list.[0]
        | _ -> DocMetadata.empty

let lookupTypeDoc (t: Type) : DocMetadata =
    let typeId = getTypeXmlId t
    match xmlExactById.TryGetValue typeId with
    | true, meta -> meta
    | false, _ -> DocMetadata.empty

// Idiomatic C# type & default value formatting
let rec formatCSharpType (t: Type) : string =
    if isNull t then "void"
    elif t.IsGenericParameter then t.Name
    elif t.IsByRef then
        formatCSharpType (t.GetElementType())
    elif t.IsArray then
        let rank = t.GetArrayRank()
        let commas = String.replicate (rank - 1) ","
        sprintf "%s[%s]" (formatCSharpType (t.GetElementType())) commas
    elif t.IsGenericType then
        let def = t.GetGenericTypeDefinition()
        let args = t.GetGenericArguments()
        if def = typedefof<Nullable<_>> then
            sprintf "%s?" (formatCSharpType args.[0])
        elif def.FullName <> null && def.FullName.StartsWith("System.ValueTuple`") then
            args |> Array.map formatCSharpType |> String.concat ", " |> sprintf "(%s)"
        else
            let rawName = t.Name
            let cleanBase =
                match rawName.IndexOf('`') with
                | -1 -> rawName
                | i -> rawName.Substring(0, i)
            let argStr = args |> Array.map formatCSharpType |> String.concat ", "
            sprintf "%s<%s>" cleanBase argStr
    else
        match t.FullName with
        | "System.Void" -> "void"
        | "System.String" -> "string"
        | "System.Boolean" -> "bool"
        | "System.Byte" -> "byte"
        | "System.SByte" -> "sbyte"
        | "System.Int16" -> "short"
        | "System.UInt16" -> "ushort"
        | "System.Int32" -> "int"
        | "System.UInt32" -> "uint"
        | "System.Int64" -> "long"
        | "System.UInt64" -> "ulong"
        | "System.Single" -> "float"
        | "System.Double" -> "double"
        | "System.Decimal" -> "decimal"
        | "System.Char" -> "char"
        | "System.Object" -> "object"
        | _ -> t.Name

let nullabilityCtx = NullabilityInfoContext()

let formatParameterTypeWithNullability (p: ParameterInfo) : string =
    let baseStr = formatCSharpType p.ParameterType
    try
        if not p.ParameterType.IsValueType then
            let info = nullabilityCtx.Create(p)
            if info.ReadState = NullabilityState.Nullable && not (baseStr.EndsWith("?")) then
                baseStr + "?"
            else baseStr
        else baseStr
    with _ -> baseStr

let formatDefaultValue (p: ParameterInfo) : string option =
    if not p.HasDefaultValue then None
    else
        let raw = p.RawDefaultValue
        if isNull raw then
            if p.ParameterType.IsValueType && Nullable.GetUnderlyingType(p.ParameterType) = null then
                Some "default"
            else
                Some "null"
        else
            match raw with
            | :? bool as b -> Some (if b then "true" else "false")
            | :? string as s -> Some (sprintf "\"%s\"" (s.Replace("\n", "\\n").Replace("\r", "\\r")))
            | :? char as c ->
                if c = '\000' then Some "'\\0'"
                elif c = '\n' then Some "'\\n'"
                elif c = '\t' then Some "'\\t'"
                else Some (sprintf "'%c'" c)
            | :? float as d when Double.IsNaN d -> Some "double.NaN"
            | :? float32 as f when Single.IsNaN f -> Some "float.NaN"
            | _ when p.ParameterType.IsEnum ->
                let enumName = Enum.GetName(p.ParameterType, raw)
                if not (isNull enumName) then Some (sprintf "%s.%s" p.ParameterType.Name enumName)
                else Some (raw.ToString())
            | _ -> Some (raw.ToString())

let formatCSharpParameter (p: ParameterInfo) : string =
    let pName = if String.IsNullOrEmpty p.Name then "arg" else p.Name
    let modifier =
        if p.IsDefined(typeof<ParamArrayAttribute>, false) then "params "
        elif p.IsOut then "out "
        elif p.IsIn then "in "
        elif p.ParameterType.IsByRef then "ref "
        else ""
    let tStr = formatParameterTypeWithNullability p
    match formatDefaultValue p with
    | Some defVal -> sprintf "%s%s %s = %s" modifier tStr pName defVal
    | None -> sprintf "%s%s %s" modifier tStr pName

// ============================================================================
// 4. Unified Member Representation & Automatic Assembly Discovery
// ============================================================================
type UnifiedProperty = {
    Name: string
    PropertyType: Type
    IndexParameters: ParameterInfo[]
    IsStatic: bool
    HasGetter: bool
    HasSetter: bool
    Doc: DocMetadata
}

type UnifiedMethod = {
    Name: string
    Method: MethodBase
    Parameters: ParameterInfo[]
    ReturnType: Type
    IsStatic: bool
    IsExtension: bool
    Doc: DocMetadata
}

type PageDescriptor = {
    PageId: string
    Title: string
    Category: string
    PrimaryType: Type
    Properties: ResizeArray<UnifiedProperty>
    StaticMethods: ResizeArray<UnifiedMethod>
    InstanceMethods: ResizeArray<UnifiedMethod>
    SubNamespaceLinks: ResizeArray<string * string * string>
}

let assembly = Assembly.LoadFrom dllPath
let exportedTypes =
    assembly.GetExportedTypes()
    |> Array.filter (fun t ->
        not (t.Name.Contains("<"))
        && not (t.Name.EndsWith("Enumerator`1"))
        && t.Name <> "Enumerator"
        && t.Name <> "ObjectArrayComparer"
        && t.Name <> "ScopedLazyFrame"
        && t.Name <> "CSharpRowCursorMaterializer")

let ignoredMethodNames =
    HashSet<string>(
        [
            "GetType"; "ToString"; "Equals"; "GetHashCode"; "Finalize"; "MemberwiseClone"
            "Dispose"; "CloneHandle"; "get_Handle"; "ToDump"; "Deconstruct"; "<Clone>$"; "PrintMembers"
        ],
        StringComparer.Ordinal
    )

let isEditorHidden (memberOrType: MemberInfo) =
    let attr = memberOrType.GetCustomAttribute<System.ComponentModel.EditorBrowsableAttribute>()
    (not (isNull attr) && attr.State = System.ComponentModel.EditorBrowsableState.Never)
    || memberOrType.IsDefined(typeof<CompilerGeneratedAttribute>, false)

let isExtensionContainer (t: Type) =
    t.IsAbstract && t.IsSealed && t.IsDefined(typeof<ExtensionAttribute>, false)

let primaryTypesByName = Dictionary<string, Type>(StringComparer.Ordinal)
let enumTypes = ResizeArray<Type>()

for t in exportedTypes do
    if t.IsEnum then
        enumTypes.Add t
    elif not (isExtensionContainer t) && not (isEditorHidden t) then
        let key = if t.DeclaringType <> null then sprintf "%s.%s" t.DeclaringType.Name t.Name else t.Name
        if not (primaryTypesByName.ContainsKey key) then
            primaryTypesByName.[key] <- t

// Detect Sub-namespace wrapper types (e.g., DtOps, StringOps, SeriesStrOps, ArrayNameSpace, etc.)
let subNamespaceMap = Dictionary<Type, string * string * string>()

for kvp in primaryTypesByName do
    let t = kvp.Value
    for p in t.GetProperties(BindingFlags.Public ||| BindingFlags.Instance ||| BindingFlags.DeclaredOnly) do
        if p.GetIndexParameters().Length = 0 then
            let pt = p.PropertyType
            if pt.Namespace = "Polars.CSharp"
               && (pt.Name.EndsWith("Ops") || pt.Name.EndsWith("NameSpace")) then
                let doc = lookupPropertyDoc p
                if not (subNamespaceMap.ContainsKey pt) then
                    subNamespaceMap.[pt] <- (t.Name, p.Name, doc.Summary)

let pagesByTypeName = Dictionary<string, PageDescriptor>(StringComparer.Ordinal)

let stripArity (name: string) =
    match name.IndexOf('`') with
    | -1 -> name
    | i -> name.Substring(0, i)

let formatTypeTitle (typeKey: string) (t: Type) =
    if t.IsGenericType then
        let cleanKey = stripArity typeKey
        let args = t.GetGenericArguments() |> Array.map (fun g -> g.Name) |> String.concat ", "
        sprintf "%s<%s>" cleanKey args
    else
        typeKey

let classifyPage (typeKey: string) (t: Type) : (string * string * string) option =
    if subNamespaceMap.ContainsKey t then
        let (parentName, propName, _) = subNamespaceMap.[t]
        let pageId = sprintf "%s.%s" parentName propName
        let title = sprintf "%s.%s (%s)" parentName propName t.Name
        let category = sprintf "%s Sub-namespaces" parentName
        Some (pageId, title, category)
    elif t.Name.StartsWith("Into") || t.Name = "BytesOrExpr" || t.Name.StartsWith("MergeMatched") || t.Name.StartsWith("MergeNotMatched") || t.Name.StartsWith("TypedMerge") || t.Name.StartsWith("MergeBuilderBase") then
        None // Implicit conversion wrappers & fluent builder states
    else
        let cleanPageId = stripArity typeKey
        let displayTitle = formatTypeTitle typeKey t
        let category =
            match cleanPageId with
            | "DataFrame" | "LazyFrame" | "Series" | "Expr" | "PolarsSchema" | "DataType" | "DataTypeExpr" -> "Core Types"
            | "Polars" | "Polars.Selectors" | "Selector" | "PolarsConfig" | "SqlContext" -> "DSL & Static Entrypoints"
            | "Delta" | "UnityCatalog" | "DeltaMergeBuilder" | "DeltaTableFeatures" | "DeltaTableProperties" -> "Delta & Catalog"
            | _ when t.Namespace = "Polars.CSharp.Linq" -> "LINQ Provider"
            | _ -> "Auxiliary Types"
        Some (cleanPageId, displayTitle, category)

for kvp in primaryTypesByName do
    let typeKey = kvp.Key
    let t = kvp.Value
    match classifyPage typeKey t with
    | None -> ()
    | Some (pageId, title, category) ->
        pagesByTypeName.[t.Name] <- {
            PageId = pageId
            Title = title
            Category = category
            PrimaryType = t
            Properties = ResizeArray()
            StaticMethods = ResizeArray()
            InstanceMethods = ResizeArray()
            SubNamespaceLinks = ResizeArray()
        }

// Also create a dedicated LINQ Extensions page for IQueryable / LinqExtensions
let linqExtType = exportedTypes |> Array.tryFind (fun t -> t.Name = "LinqExtensions")
match linqExtType with
| Some lt ->
    pagesByTypeName.["LinqExtensions"] <- {
        PageId = "LinqExtensions"
        Title = "LinqExtensions (IQueryable & Polars LINQ)"
        Category = "LINQ Provider"
        PrimaryType = lt
        Properties = ResizeArray()
        StaticMethods = ResizeArray()
        InstanceMethods = ResizeArray()
        SubNamespaceLinks = ResizeArray()
    }
| None -> ()

for kvp in subNamespaceMap do
    let subType = kvp.Key
    let (parentName, propName, summary) = kvp.Value
    if pagesByTypeName.ContainsKey parentName && pagesByTypeName.ContainsKey subType.Name then
        let subPage = pagesByTypeName.[subType.Name]
        pagesByTypeName.[parentName].SubNamespaceLinks.Add((propName, subPage.PageId, summary))

for kvp in pagesByTypeName do
    let page = kvp.Value
    let t = page.PrimaryType

    // Constructors (skip internal/handle constructors and sub-namespace wrappers)
    if not (subNamespaceMap.ContainsKey t) && not (t.IsAbstract && t.IsSealed) then
        for ctor in t.GetConstructors(BindingFlags.Public ||| BindingFlags.Instance) do
            let pars = ctor.GetParameters()
            let isHandleCtor = pars.Length = 1 && pars.[0].ParameterType.Name.EndsWith("Handle")
            if not (isEditorHidden ctor) && not isHandleCtor then
                let doc = lookupMethodDoc ctor
                page.StaticMethods.Add({
                    Name = t.Name
                    Method = ctor
                    Parameters = pars
                    ReturnType = t
                    IsStatic = true
                    IsExtension = false
                    Doc = doc
                })

    // Properties & Indexers
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
                HasGetter = not (isNull prop.GetMethod) && prop.GetMethod.IsPublic
                HasSetter = not (isNull prop.SetMethod) && prop.SetMethod.IsPublic
                Doc = doc
            })

    // Methods
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
                IsExtension = false
                Doc = doc
            }
            if m.IsStatic then page.StaticMethods.Add(unified)
            else page.InstanceMethods.Add(unified)

// Route C# Extension Methods to their target Polars.CSharp type (or LinqExtensions)
for t in exportedTypes do
    if isExtensionContainer t && t.Name <> "LinqExtensions" then
        for m in t.GetMethods(BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.DeclaredOnly) do
            let pars = m.GetParameters()
            if m.IsDefined(typeof<ExtensionAttribute>, false) && pars.Length > 0 && not (isEditorHidden m) then
                let hostType = pars.[0].ParameterType
                let doc = lookupMethodDoc m
                if pagesByTypeName.ContainsKey hostType.Name then
                    let targetPage = pagesByTypeName.[hostType.Name]
                    targetPage.InstanceMethods.Add({
                        Name = m.Name
                        Method = m
                        Parameters = pars.[1..]
                        ReturnType = m.ReturnType
                        IsStatic = false
                        IsExtension = true
                        Doc = doc
                    })
                elif pagesByTypeName.ContainsKey "LinqExtensions" && t.Namespace = "Polars.CSharp.Linq" then
                    pagesByTypeName.["LinqExtensions"].StaticMethods.Add({
                        Name = m.Name
                        Method = m
                        Parameters = pars
                        ReturnType = m.ReturnType
                        IsStatic = true
                        IsExtension = true
                        Doc = doc
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
    | "op_Addition" -> "operator + (Addition)"
    | "op_Subtraction" -> "operator - (Subtraction)"
    | "op_Multiply" -> "operator * (Multiply)"
    | "op_Division" -> "operator / (Division)"
    | "op_Modulus" -> "operator % (Modulus)"
    | "op_UnaryNegation" -> "operator - (Unary Negation)"
    | "op_Equality" -> "operator == (Equality)"
    | "op_Inequality" -> "operator != (Inequality)"
    | "op_GreaterThan" -> "operator &gt; (Greater Than)"
    | "op_GreaterThanOrEqual" -> "operator &gt;= (Greater Or Equal)"
    | "op_LessThan" -> "operator &lt; (Less Than)"
    | "op_LessThanOrEqual" -> "operator &lt;= (Less Or Equal)"
    | "op_BitwiseAnd" -> "operator &amp; (Bitwise / Logical AND)"
    | "op_BitwiseOr" -> "operator | (Bitwise / Logical OR)"
    | "op_ExclusiveOr" -> "operator ^ (Exclusive OR)"
    | "op_LogicalNot" -> "operator ! (Logical NOT)"
    | "op_OnesComplement" -> "operator ~ (Bitwise NOT)"
    | "op_LeftShift" -> "operator &lt;&lt; (Left Shift)"
    | "op_RightShift" -> "operator &gt;&gt; (Right Shift)"
    | "op_Implicit" -> "implicit operator"
    | "op_Explicit" -> "explicit operator"
    | _ -> opName

let tryGetOperatorSymbol (opName: string) =
    match opName with
    | "op_Addition" -> Some "+"
    | "op_Subtraction" -> Some "-"
    | "op_Multiply" -> Some "*"
    | "op_Division" -> Some "/"
    | "op_Modulus" -> Some "%"
    | "op_UnaryNegation" -> Some "-"
    | "op_Equality" -> Some "=="
    | "op_Inequality" -> Some "!="
    | "op_GreaterThan" -> Some ">"
    | "op_GreaterThanOrEqual" -> Some ">="
    | "op_LessThan" -> Some "<"
    | "op_LessThanOrEqual" -> Some "<="
    | "op_BitwiseAnd" -> Some "&"
    | "op_BitwiseOr" -> Some "|"
    | "op_ExclusiveOr" -> Some "^"
    | "op_LogicalNot" -> Some "!"
    | "op_OnesComplement" -> Some "~"
    | "op_LeftShift" -> Some "<<"
    | "op_RightShift" -> Some ">>"
    | _ -> None

let formatCSharpSignature (m: UnifiedMethod) : string =
    let retStr = formatCSharpType m.ReturnType
    let genArgs =
        if m.Method.IsGenericMethod then
            m.Method.GetGenericArguments()
            |> Array.map (fun g -> g.Name)
            |> String.concat ", "
            |> sprintf "<%s>"
        else ""
    let parStrs =
        m.Parameters
        |> Array.mapi (fun i p ->
            let s = formatCSharpParameter p
            if i = 0 && m.IsExtension && m.IsStatic then "this " + s else s)
    let prefix =
        if m.Method.IsConstructor then sprintf "public %s" (stripArity m.Name)
        elif m.Name = "op_Implicit" then sprintf "public static implicit operator %s" retStr
        elif m.Name = "op_Explicit" then sprintf "public static explicit operator %s" retStr
        else
            match tryGetOperatorSymbol m.Name with
            | Some sym -> sprintf "public static %s operator %s" retStr sym
            | None ->
                if m.IsStatic then sprintf "public static %s %s%s" retStr m.Name genArgs
                else sprintf "public %s %s%s" retStr m.Name genArgs

    if parStrs.Length <= 3 then
        sprintf "%s(%s)" prefix (String.concat ", " parStrs)
    else
        let indented = parStrs |> Array.map (sprintf "    %s") |> String.concat ",\n"
        sprintf "%s(\n%s\n)" prefix indented

let renderMergedMethodGroup (sb: StringBuilder) (groupName: string) (overloads: UnifiedMethod[]) =
    let displayHeading =
        if groupName.StartsWith("op_") then formatOperatorDisplayName groupName
        else groupName
    sb.AppendLine(sprintf "### %s" displayHeading) |> ignore
    sb.AppendLine() |> ignore

    // 1. Unique non-empty summaries
    let summaries =
        overloads
        |> Array.map (fun o -> o.Doc.Summary)
        |> Array.filter (not << String.IsNullOrWhiteSpace)
        |> Array.distinct
    if summaries.Length > 0 then
        for s in summaries do
            sb.AppendLine s |> ignore
            sb.AppendLine() |> ignore

    // 2. Signatures in a single csharp block
    sb.AppendLine "```csharp" |> ignore
    let signatures = overloads |> Array.map formatCSharpSignature |> Array.distinct
    for i = 0 to signatures.Length - 1 do
        sb.AppendLine signatures.[i] |> ignore
        if i < signatures.Length - 1 && signatures.[i].Contains("\n") then
            sb.AppendLine() |> ignore
    sb.AppendLine "```" |> ignore

    // 3. Merged Parameters table
    let mergedParams = ResizeArray<string * string * string>()
    let seenParams = HashSet<string>(StringComparer.OrdinalIgnoreCase)
    for o in overloads do
        for p in o.Parameters do
            let pName = if String.IsNullOrEmpty p.Name then "arg" else p.Name
            let pType = formatParameterTypeWithNullability p
            let desc =
                match o.Doc.Params.TryGetValue pName with
                | true, d when not (String.IsNullOrWhiteSpace d) -> d
                | _ -> "-"
            if not (seenParams.Contains pName) then
                seenParams.Add pName |> ignore
                mergedParams.Add((pName, pType, desc))
            else
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
                sb.AppendLine "```csharp" |> ignore
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

if Directory.Exists outputDir then Directory.Delete(outputDir, true)
Directory.CreateDirectory outputDir |> ignore

let generatedPages = ResizeArray<PageDescriptor>()

for kvp in pagesByTypeName |> Seq.sortBy (fun k -> k.Value.PageId) do
    let page = kvp.Value
    let hasContent =
        page.Properties.Count > 0
        || page.StaticMethods.Count > 0
        || page.InstanceMethods.Count > 0
        || page.SubNamespaceLinks.Count > 0

    if hasContent then
        generatedPages.Add page
        let sb = StringBuilder()
        sb.AppendLine "---" |> ignore
        sb.AppendLine(sprintf "uid: Polars.CSharp.%s" page.PageId) |> ignore
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

        // Section 1: Sub-namespaces
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
            let formatPropDisplay (p: UnifiedProperty) =
                if p.IndexParameters.Length = 0 then p.Name
                else
                    let idxStr = p.IndexParameters |> Array.map formatCSharpParameter |> String.concat ", "
                    sprintf "this[%s]" idxStr
            let groupedProps =
                page.Properties
                |> Seq.groupBy formatPropDisplay
                |> Seq.sortBy (fun (disp, _) -> if disp.StartsWith("this[") then "zzz_" + disp else disp)
            for (displayProp, propOverloads) in groupedProps do
                let overloadsArr = Seq.toArray propOverloads
                let propTypes =
                    overloadsArr
                    |> Array.map (fun p -> formatCSharpType p.PropertyType)
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
                    |> Array.sortBy (fun (name, _) -> if name = page.PrimaryType.Name then "" else name)
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

        let outPath = Path.Combine(outputDir, sprintf "%s.md" page.PageId)
        File.WriteAllText(outPath, sb.ToString())
        printfn "  -> Generated %-28s (Props: %2d, Static: %3d, Instance: %3d)"
            (page.PageId + ".md")
            page.Properties.Count
            page.StaticMethods.Count
            page.InstanceMethods.Count

// Generate Enums.md aggregating all C# Enums
if enumTypes.Count > 0 then
    let sbEnums = StringBuilder()
    sbEnums.AppendLine "---" |> ignore
    sbEnums.AppendLine "uid: Polars.CSharp.Enums" |> ignore
    sbEnums.AppendLine "title: Enums" |> ignore
    sbEnums.AppendLine "---" |> ignore
    sbEnums.AppendLine() |> ignore
    sbEnums.AppendLine "# Enums" |> ignore
    sbEnums.AppendLine() |> ignore
    sbEnums.AppendLine "Namespace: `Polars.CSharp`" |> ignore
    sbEnums.AppendLine() |> ignore

    for t in enumTypes |> Seq.sortBy (fun x -> x.Name) do
        sbEnums.AppendLine(sprintf "## %s" t.Name) |> ignore
        sbEnums.AppendLine() |> ignore
        let tDoc = lookupTypeDoc t
        if not (String.IsNullOrWhiteSpace tDoc.Summary) then
            sbEnums.AppendLine tDoc.Summary |> ignore
            sbEnums.AppendLine() |> ignore

        sbEnums.AppendLine "```csharp" |> ignore
        sbEnums.AppendLine(sprintf "public enum %s" t.Name) |> ignore
        sbEnums.AppendLine "{" |> ignore
        let names = Enum.GetNames(t)
        for i = 0 to names.Length - 1 do
            let comma = if i < names.Length - 1 then "," else ""
            sbEnums.AppendLine(sprintf "    %s%s" names.[i] comma) |> ignore
        sbEnums.AppendLine "}" |> ignore
        sbEnums.AppendLine "```" |> ignore
        sbEnums.AppendLine() |> ignore
        sbEnums.AppendLine "---" |> ignore
        sbEnums.AppendLine() |> ignore

    File.WriteAllText(Path.Combine(outputDir, "Enums.md"), sbEnums.ToString())
    printfn "  -> Generated %-28s (%d enum types)" "Enums.md" enumTypes.Count

// ============================================================================
// 6. Generate Structured TOC (toc.yml)
// ============================================================================
let sbToc = StringBuilder()

let categoryOrder = [
    "Core Types"
    "Expr Sub-namespaces"
    "Series Sub-namespaces"
    "DataTypeExpr Sub-namespaces"
    "DSL & Static Entrypoints"
    "LINQ Provider"
    "Delta & Catalog"
    "Auxiliary Types"
]

for cat in categoryOrder do
    let pagesInCat =
        generatedPages
        |> Seq.filter (fun p -> p.Category = cat)
        |> Seq.sortBy (fun p -> p.PageId)
        |> Seq.toArray
    if pagesInCat.Length > 0 || (cat = "Auxiliary Types" && enumTypes.Count > 0) then
        sbToc.AppendLine(sprintf "- name: %s" cat) |> ignore
        sbToc.AppendLine("  items:") |> ignore
        for p in pagesInCat do
            sbToc.AppendLine(sprintf "  - name: %s" p.PageId) |> ignore
            sbToc.AppendLine(sprintf "    href: %s.md" p.PageId) |> ignore
        if cat = "Auxiliary Types" && enumTypes.Count > 0 then
            sbToc.AppendLine("  - name: Enums") |> ignore
            sbToc.AppendLine("    href: Enums.md") |> ignore

File.WriteAllText(Path.Combine(outputDir, "toc.yml"), sbToc.ToString())
printfn "\nDone! Generated %d C# documentation pages (+ Enums.md) in '%s'." generatedPages.Count outputDir
