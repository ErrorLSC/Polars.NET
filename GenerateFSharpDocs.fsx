#r "System.Xml.Linq"
open System
open System.IO
open System.Reflection
open System.Xml.Linq
open System.Text.RegularExpressions
open System.Collections.Generic

// ================= Set Up =================
let dllPath = "Polars.FSharp/bin/Release/net8.0/Polars.FSharp.dll"
let xmlPath = "Polars.FSharp/bin/Release/net8.0/Polars.FSharp.xml"
let outputDir = "docs/api/fsharp_generated"
let targetTypes = ["pl"; "Series"; "DataFrame"; "Expr";"LazyFrame";"Delta";"Selector";"LazyFrameDeltaExtensions";"DataFrameDeltaExtensions";"UnityCatalog";"Config";"Merge";"Set"] 
// =========================================

let binDir = Path.GetDirectoryName(Path.GetFullPath dllPath)
AppDomain.CurrentDomain.add_AssemblyResolve(fun _ args ->
    let assemblyName = (new AssemblyName(args.Name)).Name
    let depPath = Path.Combine(binDir, assemblyName + ".dll")
    if File.Exists depPath then Assembly.LoadFrom depPath else null
)

type DocMetadata = {
    Summary: string
    Params: Dictionary<string, string> 
    Returns: string
    Remarks: string
    Example: string
}

// ---------------------------------------------------------
// Basic Repair
// ---------------------------------------------------------

// Remove XML tags
let cleanXmlText (raw: string) =
    if String.IsNullOrWhiteSpace raw then ""
    else
        raw
        |> fun s -> Regex.Replace(s, @"\s+", " ") 
        |> fun s -> Regex.Replace(s, @"<c>(.*?)</c>", "`$1`") 
        |> fun s -> Regex.Replace(s, @"<code>(.*?)</code>", "\n```\n$1\n```\n")
        |> fun s -> Regex.Replace(s, @"<para>(.*?)</para>", "\n\n$1\n\n")
        |> fun s -> Regex.Replace(s, @"<see cref=""(?:.*?\.)?([^""]+)""\s*/>", "`$1`")
        |> fun s -> Regex.Replace(s, @"<paramref name=""([^""]+)""\s*/>", "`$1`")
        |> fun s -> Regex.Replace(s, @"<[^>]+>", "") 
        |> fun s -> s.Trim()

// ---------------------------------------------------------
// Strict XML Parse
// ---------------------------------------------------------
let sanitizeFragment (xmlSnippet: string) =
    if String.IsNullOrEmpty xmlSnippet then ""
    else
        let s1 = Regex.Replace(xmlSnippet, @"&(?!(amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)", "&amp;")
        let validTags = "summary|remarks|param|returns|example|exception|see|seealso|c|code|para|list|item|paramref|typeparam|typeparamref|value|root|member|inheritdoc"
        let pattern = sprintf @"<(?!(/?(%s)\b))" validTags
        Regex.Replace(s1, pattern, "&lt;", RegexOptions.IgnoreCase)

let rec xmlNodeToMarkdown (node: XNode) : string =
    match node with
    | :? XText as text -> Regex.Replace(text.Value, @"\s+", " ")
    | :? XElement as el ->
        let innerText = el.Nodes() |> Seq.map xmlNodeToMarkdown |> String.concat ""
        match el.Name.LocalName with
        | "c" -> sprintf "`%s`" innerText
        | "code" -> sprintf "\n```\n%s\n```\n" (innerText.Trim())
        | "para" -> sprintf "\n\n%s\n\n" (innerText.Trim())
        | "see" -> 
            let cref = el.Attribute(XName.Get "cref")
            let name = if cref <> null then cref.Value.Split(':').[1] else "reference"
            sprintf "`%s`" (name.Split('.') |> Array.last)
        | "paramref" ->
            let name = el.Attribute(XName.Get "name")
            if name <> null then sprintf "`%s`" name.Value else innerText
        | _ -> innerText
    | _ -> ""

// ---------------------------------------------------------
// Regex
// ---------------------------------------------------------
let regexExtract (tag: string) (xml: string) =
    let m = Regex.Match(xml, sprintf @"<%s>(.*?)</%s>" tag tag, RegexOptions.Singleline)
    if m.Success then cleanXmlText m.Groups.[1].Value else ""

let regexExtractParams (xml: string) =
    let dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    let matches = Regex.Matches(xml, @"<param name=""([^""]+)"">(.*?)</param>", RegexOptions.Singleline)
    for m in matches do
        let name = m.Groups.[1].Value
        let desc = cleanXmlText m.Groups.[2].Value
        if not (dict.ContainsKey name) then dict.Add(name, desc)
    dict

// ---------------------------------------------------------
// Mixed Parse Entry
// ---------------------------------------------------------
let tryParseMember (rawMemberXml: string) : option<string * DocMetadata> =
    let idMatch = Regex.Match(rawMemberXml, @"name=""([^""]+)""")
    if not idMatch.Success then None
    else
        let fullId = idMatch.Groups.[1].Value
        
        try
            let safeXml = sanitizeFragment rawMemberXml
            let el = XElement.Parse safeXml
            
            let getPart name = 
                let node = el.Element(XName.Get name)
                if node <> null then 
                    node.Nodes() |> Seq.map xmlNodeToMarkdown |> String.concat "" |> fun s -> s.Trim()
                else ""

            let paramDict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            for p in el.Elements(XName.Get "param") do
                let pNameAttr = p.Attribute(XName.Get "name")
                if pNameAttr <> null then
                    let pDesc = p.Nodes() |> Seq.map xmlNodeToMarkdown |> String.concat "" |> fun s -> s.Trim()
                    if not (paramDict.ContainsKey pNameAttr.Value) then
                        paramDict.Add(pNameAttr.Value, pDesc)

            Some(fullId, {
                Summary = getPart "summary"
                Params = paramDict
                Returns = getPart "returns"
                Remarks = getPart "remarks"
                Example = getPart "example"
            })
        
        with _ ->
            
            let meta = {
                Summary = regexExtract "summary" rawMemberXml
                Params = regexExtractParams rawMemberXml
                Returns = regexExtract "returns" rawMemberXml
                Remarks = regexExtract "remarks" rawMemberXml
                Example = regexExtract "example" rawMemberXml
            }
            Some(fullId, meta)

if Directory.Exists outputDir then Directory.Delete(outputDir, true)
Directory.CreateDirectory outputDir |> ignore

if not (File.Exists xmlPath) then
    printfn "XML File not found: %s" xmlPath
    exit 1

printfn "Parsing XML (Hybrid Mode)..."
let rawFileContent = File.ReadAllText xmlPath
let memberRegex = Regex(@"<member name=""([^""]+)"">.*?</member>", RegexOptions.Singleline)
let matches = memberRegex.Matches rawFileContent

let xmlLookup = new Dictionary<string, ResizeArray<string * DocMetadata>>()
let mutable strictCount = 0
let mutable fallbackCount = 0

for m in matches do
    match tryParseMember m.Value with
    | Some (fullId, meta) ->
        let noParams = match fullId.IndexOf '(' with | -1 -> fullId | i -> fullId.Substring(0, i)
        let noGeneric = Regex.Replace(noParams, @"``\d+", "")
        let methodName = noGeneric.Split '.' |> Array.last |> fun s -> s.ToLowerInvariant()

        if not (xmlLookup.ContainsKey methodName) then
            xmlLookup.Add(methodName, new ResizeArray<string * DocMetadata>())
        xmlLookup.[methodName].Add((fullId, meta))
        strictCount <- strictCount + 1
    | None -> 
        fallbackCount <- fallbackCount + 1

printfn "Indexed %d methods." strictCount

let loadAssembly (path: string) =
    try Assembly.LoadFrom path
    with ex -> printfn "Warning: %s" ex.Message; null

let assembly = loadAssembly dllPath
if assembly = null then exit 1

let rec formatType (t: Type) =
    let cleanName (n: string) =
        match n with
        | "FSharpOption" -> "Option"
        | "FSharpValueOption" -> "ValueOption"
        | "IEnumerable" -> "Seq"
        | "FSharpList" -> "List"
        | "Unit" -> "unit"
        | "String" -> "string"
        | "Boolean" -> "bool"
        | "Int32" -> "int"
        | "Int64" -> "int64"
        | "Double" -> "float"
        | "Single" -> "float32"
        | "Object" -> "obj"
        | _ -> n

    if t.IsArray then
        let elemType = formatType (t.GetElementType())
        sprintf "%s[]" elemType
    elif t.IsGenericType then
        let name = t.Name.Substring(0, t.Name.IndexOf('`'))
        let prettyName = cleanName name
        let args = t.GetGenericArguments() |> Array.map formatType |> String.concat ", "
        sprintf "%s<%s>" prettyName args
    elif t.Name = "FSharpFunc`2" then
        let args = t.GetGenericArguments()
        sprintf "%s -> %s" (formatType args.[0]) (formatType args.[1])
    else
        cleanName t.Name

let cleanForTable (raw: string) =
    if String.IsNullOrWhiteSpace raw then "-"
    else
        raw.Replace("|", "&#124;")
           .Replace("\n", "<br>")
           .Replace("\r", "")

let generateDocForType (typeName: string) =
    try
        let typeInfo = assembly.GetTypes() |> Seq.tryFind (fun t -> t.Name = typeName)
        match typeInfo with
        | None -> printfn "Type '%s' not found in assembly" typeName
        | Some t ->
            printfn "Generating docs for: %s" typeName
            let sb = System.Text.StringBuilder()
            sb.AppendLine "---" |> ignore
            sb.AppendLine(sprintf "uid: Polars.FSharp.%s" typeName) |> ignore
            sb.AppendLine(sprintf "title: %s" typeName) |> ignore
            sb.AppendLine "---" |> ignore
            sb.AppendLine() |> ignore
            sb.AppendLine(sprintf "# %s" typeName) |> ignore
            sb.AppendLine(sprintf "Namespace: `%s`" t.Namespace) |> ignore
            sb.AppendLine() |> ignore
            
            let methods = 
                t.GetMethods(BindingFlags.Public ||| BindingFlags.Static ||| BindingFlags.Instance)
                |> Seq.filter (fun m -> 
                    not (m.Name.StartsWith "get_" || m.Name.StartsWith "set_") &&
                    m.DeclaringType.Name = typeName)
                |> Seq.sortBy (fun m -> m.Name)

            for m in methods do
                sb.AppendLine(sprintf "## %s" m.Name) |> ignore
                let mutable doc = { Summary = "*No documentation found.*"; Params = new Dictionary<string,string>(); Returns=""; Remarks=""; Example="" }
                let searchKey = m.Name.ToLowerInvariant()
                
                if xmlLookup.ContainsKey searchKey then
                    let candidates = xmlLookup.[searchKey]
                    let bestMatch = 
                        candidates 
                        |> Seq.tryFind (fun (id, _) -> 
                            let idLower = id.ToLowerInvariant()
                            let typeLower = typeName.ToLowerInvariant()
                            idLower.Contains("." + typeLower + ".") || idLower.Contains("." + typeLower + "`"))
                    match bestMatch with
                    | Some (_, d) -> doc <- d
                    | None -> if candidates.Count > 0 then doc <- snd candidates.[0]

                sb.AppendLine() |> ignore
                sb.AppendLine doc.Summary |> ignore
                sb.AppendLine() |> ignore

                // Signature
                sb.AppendLine "```fsharp" |> ignore
                let paramsInfo = m.GetParameters()
                let paramsStr = 
                    try
                        paramsInfo
                        |> Array.map (fun p -> sprintf "%s: %s" p.Name (formatType p.ParameterType))
                        |> String.concat " -> "
                    with _ -> "..."
                let returnType = formatType m.ReturnType
                let prefix = if m.IsStatic then "" else "(instance) "
                sb.AppendLine(sprintf "// Signature:") |> ignore
                sb.AppendLine(sprintf "%s%s -> %s" prefix paramsStr returnType) |> ignore
                sb.AppendLine "```" |> ignore

                // Parameters
                if paramsInfo.Length > 0 then
                    sb.AppendLine() |> ignore
                    sb.AppendLine "**Parameters**" |> ignore
                    sb.AppendLine() |> ignore
                    sb.AppendLine "| Name | Type | Description |" |> ignore
                    sb.AppendLine "| :--- | :--- | :--- |" |> ignore
                    for p in paramsInfo do
                        let pName = p.Name
                        let rawType = formatType p.ParameterType
                        let rawDesc = if doc.Params.ContainsKey pName then doc.Params.[pName] else "-"
                        let tableDesc = cleanForTable rawDesc
                        sb.AppendLine(sprintf "| `%s` | `%s` | %s |" pName rawType tableDesc) |> ignore
                
                if not (String.IsNullOrWhiteSpace doc.Returns) then
                    sb.AppendLine() |> ignore
                    sb.AppendLine "**Returns**" |> ignore
                    sb.AppendLine() |> ignore
                    sb.AppendLine doc.Returns |> ignore

                if not (String.IsNullOrWhiteSpace doc.Example) then
                    sb.AppendLine() |> ignore
                    sb.AppendLine "**Example**" |> ignore
                    sb.AppendLine() |> ignore
                    sb.AppendLine doc.Example |> ignore

                if not (String.IsNullOrWhiteSpace doc.Remarks) then
                    sb.AppendLine() |> ignore
                    sb.AppendLine "> [!NOTE]" |> ignore
                    sb.AppendLine("> " + doc.Remarks.Replace("\n", "\n> ")) |> ignore

                sb.AppendLine() |> ignore
                sb.AppendLine "---" |> ignore

            File.WriteAllText(Path.Combine(outputDir, sprintf "%s.md" typeName), sb.ToString())
    with ex ->
        printfn "Error processing %s: %s" typeName ex.Message

for t in targetTypes do
    generateDocForType t

let sbToc = System.Text.StringBuilder()
for t in targetTypes do
    sbToc.AppendLine(sprintf "- name: %s" t) |> ignore
    sbToc.AppendLine(sprintf "  href: %s.md" t) |> ignore
File.WriteAllText(Path.Combine(outputDir, "toc.yml"), sbToc.ToString())

printfn "Done! Hybrid parsing generated docs for ALL methods."