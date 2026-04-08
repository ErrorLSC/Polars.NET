#pragma warning disable CS1591
using System.Collections.Frozen;
using System.Text;
using Polars.NET.Core;
using Cs = Polars.CSharp.Polars.Selectors;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// Provides a context for referencing Source and Target columns safely during a Merge operation.
/// </summary>
public class MergeContext
{
    private readonly string _sourceSuffix;

    [Obsolete("Please use the lambda parameter (m => m.Source(...)) provided by the builder. This static context cannot protect Join Keys from suffix errors.", error: false)]
    public static readonly MergeContext Delta = new("_src_tmp");
    internal MergeContext(string sourceSuffix)
    {
        _sourceSuffix = sourceSuffix;
    }

    /// <summary>
    /// References a column from the incoming Source DataFrame.
    /// </summary>
    public Expr Source(string columnName) => Pl.Col(columnName + _sourceSuffix);
    

    /// <summary>
    /// References a column from the existing Target DataFrame.
    /// </summary>
    public Expr Target(string columnName) => Pl.Col(columnName);
}
public class MergeSetterBuilder
{
    private readonly MergeContext _ctx;
    private readonly LazyFrame _target;

    internal Dictionary<string, IntoExpr> Setters { get; } = [];

    internal MergeSetterBuilder(MergeContext ctx, LazyFrame target)
    {
        _ctx = ctx;
        _target = target;
    }

    public MergeSetterBuilder Set(string columnName, Func<MergeContext, IntoExpr> valueBuilder)
    {
        Setters[columnName] = valueBuilder(_ctx);
        return this;
    }

    public MergeSetterBuilder Set(string columnName, IntoExpr value)
    {
        Setters[columnName] = value;
        return this;
    }

    /// <summary>
    /// Update multiple columns using a Selector. The value builder provides the current column name.
    /// </summary>
    public MergeSetterBuilder Set(IntoSelector selector, Func<MergeContext, string, IntoExpr> valueBuilder)
    {
        using var sel = selector.Consume();
        
        var cols = _target.Select(sel).Schema.Names;
        
        foreach (var colName in cols)
        {
            Setters[colName] = valueBuilder(_ctx, colName);
        }
        return this;
    }
}

/// <summary>
/// A builder for Polars Merge
/// </summary>
public abstract class MergeBuilderBase<TBuilder> where TBuilder : MergeBuilderBase<TBuilder>
{
    protected readonly LazyFrame _target;
    protected readonly LazyFrame _source;
    protected FrozenDictionary<string, DataType>? _srcSchemaCache;
    protected FrozenDictionary<string, DataType>? _tgtSchemaCache;
    protected readonly string[] _on;

    protected bool _includeNulls = false;
    protected JoinMaintainOrder _maintainOrder = JoinMaintainOrder.Left;
    protected readonly string _tmpSfx = $"__TMP_{Guid.NewGuid().ToString("N")[..8]}";
    protected readonly string _actionCol = "__ACTION";
    protected readonly MergeContext _ctx;
    protected readonly List<(
        int ActionId, 
        MergeActionType Type, 
        Expr Condition, 
        Dictionary<string, IntoExpr>? Setters
    )> _actions = [];

    protected MergeBuilderBase(LazyFrame target, LazyFrame source, string[] on)
    {
        _target = target;
        _source = source;
        _on = on;
        _ctx = new MergeContext(_tmpSfx); 
    }
 
    /// <summary>
    /// Update the matched target row. Optionally filtered by a condition and targeting specific columns.
    /// </summary>
    public TBuilder WhenMatchedUpdate(
        Func<MergeContext, Expr>? condition = null,
        Action<MergeSetterBuilder>? set = null)
    {
        Expr cond = condition != null ? condition(_ctx) : Pl.Lit(true);
        
        Dictionary<string, IntoExpr>? setters = null;
        if (set != null)
        {
            var sb = new MergeSetterBuilder(_ctx,_target);
            set(sb);
            setters = sb.Setters;
        }

        _actions.Add((_actions.Count + 1, MergeActionType.MatchedUpdate, cond, setters));
        return (TBuilder)this;
    }

    /// <summary>
    /// Delete the matched target row if the condition is met.
    /// </summary>
    public TBuilder WhenMatchedDelete(Func<MergeContext, Expr>? condition = null)
    {
        Expr cond = condition != null ? condition(_ctx) : Pl.Lit(true);
        _actions.Add((_actions.Count + 1, MergeActionType.MatchedDelete, cond, null));
        return (TBuilder)this;
    }

    /// <summary>
    /// Insert a new row from the source if not matched. Optionally filtered and with specific column overrides.
    /// </summary>
    public TBuilder WhenNotMatchedInsert(
        Func<MergeContext, Expr>? condition = null,
        Action<MergeSetterBuilder>? set = null) 
    {
        Expr cond = condition != null ? condition(_ctx) : Pl.Lit(true);
        
        Dictionary<string, IntoExpr>? setters = null;
        if (set != null)
        {
            var sb = new MergeSetterBuilder(_ctx,_target);
            set(sb);
            setters = sb.Setters;
        }

        _actions.Add((_actions.Count + 1, MergeActionType.NotMatchedInsert, cond, setters));
        return (TBuilder)this;
    }

    /// <summary>
    /// Delete the target row if it does not match any row in the source. Optionally filtered by a condition.
    /// </summary>
    public TBuilder WhenNotMatchedBySourceDelete(Func<MergeContext, Expr>? condition = null)
    {
        Expr cond = condition != null ? condition(_ctx) : Pl.Lit(true);
        _actions.Add((_actions.Count + 1, MergeActionType.NotMatchedBySourceDelete, cond, null));
        return (TBuilder)this;
    }

    public TBuilder IncludeNulls(bool include = true)
    {
        _includeNulls = include;
        return (TBuilder)this;
    }

    public TBuilder MaintainOrder(JoinMaintainOrder order)
    {
        _maintainOrder = order;
        return (TBuilder)this;
    }
    /// <summary>
    /// Generates the Abstract Syntax Tree (AST) for the merge operation and returns its execution plan.
    /// </summary>
    /// <param name="optimized">Whether to show the optimized physical plan or the unoptimized logical plan.</param>
    /// <returns>A string representation of the Polars execution plan.</returns>
    public string Explain(bool optimized = true)
    {
        using var ast = BuildAst();
        
        return ast.Explain(optimized);
    }
    protected void ValidateMergePhase()
    {
        // =========================================================
        // Schema Compatibility Check (Merge Keys Only)
        // =========================================================
        foreach (var key in _on)
        {
            if (!_srcSchemaCache!.TryGetValue(key, out var srcType))
                throw new ArgumentException($"Merge Key '{key}' not found in SOURCE table schema.");
                
            if (!_tgtSchemaCache!.TryGetValue(key, out var tgtType))
                throw new ArgumentException($"Merge Key '{key}' not found in TARGET table schema.");

            if (srcType != tgtType)
                throw new ArgumentException(
                    $"Merge Key Type Mismatch for column '{key}'! \n" +
                    $"Source: {srcType} \nTarget: {tgtType} \nMerge Keys must have identical types.");
        }

        // =========================================================
        // Data Quality Check (Nulls & Duplicates) 
        // =========================================================
        var mergeKeyExprs = _on.Select(k => (IntoExpr)k);
        
        var hasNullExpr = Pl.AnyHorizontal(Cs.ByName(_on).ToExpr().IsNull()).Alias("has_null_key");

        var checkLf = _source
            .Select(
                Pl.Len().Over(mergeKeyExprs).Alias("group_count"),
                hasNullExpr
            )
            .Filter(Pl.Col("group_count") > 1 | Pl.Col("has_null_key"))
            .Limit(1); 

        using var checkDf = checkLf.Collect();

        if (checkDf.Height > 0)
        {
            bool isNullError = checkDf.Select(Pl.Col("has_null_key")).Row(0)[0] as bool? ?? false;

            if (isNullError)
            {
                throw new InvalidDataException(
                    $"CRITICAL ERROR: Null values detected in Merge Keys! \n" +
                    $"Merge Keys: [{string.Join(", ", _on)}]");
            }
            else
            {
                using var exampleDupes = _source
                    .GroupBy(mergeKeyExprs) 
                    .Agg(Pl.Len().Alias("duplicate_count")) 
                    .Filter(Pl.Col("duplicate_count") > 1) 
                    .Sort("duplicate_count", descending: true) 
                    .Limit(5) 
                    .Collect(); 

                throw new InvalidDataException(
                    $"CRITICAL ERROR: Duplicate keys detected in SOURCE table!\n" +
                    $"Merge expects unique source keys per checking round.\n" +
                    $"[Merge Keys]: [{string.Join(", ", _on)}]\n" +
                    $"--- Duplicate Key Examples (Top 5) ---\n" +
                    $"{exampleDupes}");
            }
        }
    }
    // --- AST Compile Engine  ---
    protected LazyFrame BuildAst()
    {
        if (_actions.Count == 0)
        {
            WhenMatchedUpdate();
            WhenNotMatchedInsert();
        }

        var allCols = _tgtSchemaCache!.Keys.Union(_srcSchemaCache!.Keys).ToList();

        string tgtVal = "__TGT", srcVal = "__SRC";
        
        var tgt = _target.WithColumns(Pl.Lit(true).Alias(tgtVal));
        
        var src = _source.Select(
            Pl.All().Name.Suffix(_tmpSfx),
            Pl.Lit(true).Alias(srcVal)
        );

        var leftOn = _on.Select(Pl.Col);
        var rightOn = _on.Select(c => Pl.Col(c + _tmpSfx));

        var joined = tgt.Join(
            other: src,
            leftOn: leftOn,
            rightOn: rightOn, 
            how: _actions.Any(a => a.Type == MergeActionType.NotMatchedInsert) ? JoinType.Outer : JoinType.Left,
            coalesce: JoinCoalesce.KeepColumns, 
            maintainOrder: _maintainOrder);

        var isMatched = Pl.Col(tgtVal).IsNotNull() & Pl.Col(srcVal).IsNotNull();
        var isSourceOnly = Pl.Col(tgtVal).IsNull() & Pl.Col(srcVal).IsNotNull();
        var isTargetOnly = Pl.Col(tgtVal).IsNotNull() & Pl.Col(srcVal).IsNull();

        // Row-level Action Arbitration
        Expr actionExpr = Pl.When(isTargetOnly).Then(0).When(isMatched).Then(0).Otherwise(-1);
        
        for (int i = _actions.Count - 1; i >= 0; i--)
        {
            var action = _actions[i];
            Expr cond = Pl.Lit(false);

            if (action.Type == MergeActionType.MatchedDelete) cond = isMatched & action.Condition;
            else if (action.Type == MergeActionType.MatchedUpdate) cond = isMatched & action.Condition;
            else if (action.Type == MergeActionType.NotMatchedInsert) cond = isSourceOnly & action.Condition;
            else if (action.Type == MergeActionType.NotMatchedBySourceDelete) cond = isTargetOnly & action.Condition;

            actionExpr = Pl.When(cond).Then(action.ActionId).Otherwise(actionExpr);
        }
        joined = joined.WithColumns(actionExpr.Alias(_actionCol));

        // Column-level Update Arbitration
        var updateExprs = new List<IntoExpr>(allCols.Count);

        foreach (var colName in allCols)
        {
            // 🚀 直接用 ContainsKey 查询字典缓存，O(1) 且零 FFI 交互
            var tgtCol = _tgtSchemaCache!.ContainsKey(colName) ? Pl.Col(colName) : Pl.LitNull();
            var srcColTmp = _srcSchemaCache!.ContainsKey(colName) ? Pl.Col(colName + _tmpSfx) : Pl.LitNull();

            Expr colUpdateExpr = tgtCol; 

            for (int i = _actions.Count - 1; i >= 0; i--)
            {
                var action = _actions[i];
                
                if (action.Type == MergeActionType.MatchedUpdate || action.Type == MergeActionType.NotMatchedInsert)
                {
                    // Scenario A: Set() for rows
                    if (action.Setters != null && action.Setters.TryGetValue(colName, out var userExpr))
                    {
                        colUpdateExpr = Pl.When(Pl.Col(_actionCol) == action.ActionId)
                                          .Then(userExpr) 
                                          .Otherwise(colUpdateExpr);
                    }
                    // Scenario B: Update all column
                    else if (action.Setters == null && _srcSchemaCache.ContainsKey(colName))
                    {
                        var updateCond = Pl.Col(_actionCol) == action.ActionId;
                        if (!_includeNulls) updateCond &= srcColTmp.IsNotNull();

                        colUpdateExpr = Pl.When(updateCond)
                                          .Then(srcColTmp)
                                          .Otherwise(colUpdateExpr);
                    }
                }
            }

            updateExprs.Add(colUpdateExpr.Alias(colName));
        }
        joined = joined.WithColumns(updateExprs);

        // Garbage Collection & Filtering
        var deleteIds = _actions.Where(a => a.Type == MergeActionType.MatchedDelete || a.Type == MergeActionType.NotMatchedBySourceDelete)
                                .Select(a => a.ActionId)
                                .ToList();

        Expr keepCond = Pl.Col(_actionCol) != -1;
        foreach (var dId in deleteIds)
        {
            keepCond &= Pl.Col(_actionCol) != dId;
        }

        joined = joined.Filter(keepCond);

        return joined.Drop(
            Cs.EndsWith(_tmpSfx),
            tgtVal, 
            srcVal, 
            _actionCol
        );
    }
    /// <summary>
    /// Returns a formatted string describing the high-level logical merge plan.
    /// </summary>
    /// <param name="verbose">If true, uses format tree for complex expressions. Otherwise uses inline ToString().</param>
    /// <param name="schema">Optional schema to pass to FormatTree for type resolution.</param>
    /// <returns>A string representation of the merge strategy.</returns>
    public string ToMergePlanString(bool verbose = false, PolarsSchema? schema = null)
    {
        var sb = new StringBuilder();

        // 1. MERGE ON
        sb.AppendLine($"MERGE ON: {string.Join(", ", _on)}");
        sb.AppendLine();

        // 2. MATCH STRATEGY
        sb.AppendLine("MATCH STRATEGY:");
        sb.AppendLine("  First Match Wins (Sequential Evaluation)");
        sb.AppendLine();

        // ActionId and Setters
        var actionsToPrint = _actions.Count > 0 ? _actions :
        [
            (1, MergeActionType.MatchedUpdate, Pl.Lit(true), null),
            (2, MergeActionType.NotMatchedInsert, Pl.Lit(true), null)
        ];

        var matchedActions = actionsToPrint
            .Where(a => a.Type == MergeActionType.MatchedUpdate || a.Type == MergeActionType.MatchedDelete)
            .ToList();
            
        var notMatchedActions = actionsToPrint
            .Where(a => a.Type == MergeActionType.NotMatchedInsert || a.Type == MergeActionType.NotMatchedBySourceDelete)
            .ToList();

        void PrintActions(List<(int ActionId, MergeActionType Type, Expr Condition, Dictionary<string, IntoExpr>? Setters)> acts)
        {
            for (int i = 0; i < acts.Count; i++)
            {
                var action = acts[i];
                string actionName = action.Type switch
                {
                    MergeActionType.MatchedUpdate => "UPDATE",
                    MergeActionType.MatchedDelete => "DELETE",
                    MergeActionType.NotMatchedInsert => "INSERT",
                    MergeActionType.NotMatchedBySourceDelete => "DELETE (By Source)",
                    _ => action.Type.ToString()
                };

                string inlineCond = action.Condition.ToString().Replace(_tmpSfx, ".Source"); 
                bool isAlwaysTrue = inlineCond.Equals("true", StringComparison.OrdinalIgnoreCase) || 
                                    inlineCond.Equals("Literal(true)", StringComparison.OrdinalIgnoreCase);

                // --- WHERE ---
                if (!isAlwaysTrue)
                {
                    if (verbose)
                    {
                        string tree = action.Condition.Meta.FormatTree(displayAsDot: false, schema: schema);
                        
                        string replacement = ".Source".PadRight(_tmpSfx.Length);
                        tree = tree.Replace(_tmpSfx, replacement); 
                        
                        string indentedTree = string.Join("\n", tree.Split('\n').Select(line => "      " + line)); 
                        sb.AppendLine($"  [{i + 1}] {actionName} WHERE:");
                        sb.AppendLine(indentedTree);
                    }
                    else
                    {
                        sb.AppendLine($"  [{i + 1}] {actionName} WHERE {inlineCond}");
                    }
                }
                else
                {
                    sb.AppendLine($"  [{i + 1}] {actionName}");
                }

                // --- SET ---
                if (action.Type == MergeActionType.MatchedUpdate || action.Type == MergeActionType.NotMatchedInsert)
                {
                    if (action.Setters != null && action.Setters.Count > 0)
                    {
                        sb.AppendLine($"      SET ({action.Setters.Count} overrides):");
                        foreach (var kvp in action.Setters)
                        {
                            string setterVal = kvp.Value.ToString()?.Replace(_tmpSfx, ".Source") ?? "null";
                            sb.AppendLine($"        - {kvp.Key} = {setterVal}");
                        }
                    }
                    else
                    {
                        sb.AppendLine("      SET: (All Source Columns)");
                    }
                }
            }
        }

        // 3. WHEN MATCHED
        if (matchedActions.Count > 0)
        {
            sb.AppendLine("WHEN MATCHED:");
            PrintActions(matchedActions);
            sb.AppendLine();
        }

        // 4. WHEN NOT MATCHED
        if (notMatchedActions.Count > 0)
        {
            sb.AppendLine("WHEN NOT MATCHED:");
            PrintActions(notMatchedActions);
            sb.AppendLine();
        }

        // 5. JOIN STRATEGY
        sb.AppendLine("JOIN STRATEGY:");
        bool hasInsert = actionsToPrint.Any(a => a.Type == MergeActionType.NotMatchedInsert);
        string joinReason = hasInsert ? "(Upgraded to Outer to support INSERT)" : "(Left join sufficient)";
        JoinType expectedJoinType = hasInsert ? JoinType.Outer : JoinType.Left;
        
        sb.AppendLine($"  Type: {expectedJoinType} {joinReason}");
        sb.AppendLine($"  MaintainOrder: {_maintainOrder}");

        return sb.ToString().TrimEnd();
    }
    /// <summary>
    /// Inspects the current logical merge plan by printing it, without breaking the method chain.
    /// </summary>
    /// <param name="verbose">If true, prints the detailed AST trees for conditions.</param>
    /// <param name="logger">Optional custom logger. Defaults to Console.WriteLine if null.</param>
    /// <returns>The current builder instance for further chaining.</returns>
    public TBuilder InspectPlan(bool verbose = false, Action<string>? logger = null)
    {
        string plan = ToMergePlanString(verbose);
        
        string output = $"""
            ========== POLARS.NET MERGE PLAN ==========
            {plan}
            ===========================================
            """;

        if (logger != null)
        {
            logger(output);
        }
        else
        {
            Console.WriteLine(output);
        }

        return (TBuilder)this;
    }

    /// <summary>
    /// Returns the high-level logical merge plan (Clean mode). 
    /// </summary>
    public override string ToString() => ToMergePlanString(verbose: false);
}

public class DataFrameMergeBuilder : MergeBuilderBase<DataFrameMergeBuilder>
{
    internal DataFrameMergeBuilder(LazyFrame target, LazyFrame source, string[] on) 
        : base(target, source, on) { }

    /// <summary>
    /// Executes the merge operation eagerly and returns a materialized DataFrame.
    /// </summary>
    public DataFrame Execute(Engine engine=Engine.Auto,bool streaming=false)
    {   
         _srcSchemaCache = _source.CollectSchema().ToFrozenDictionary();
        _tgtSchemaCache = _target.Schema.ToFrozenDictionary();
        ValidateMergePhase();
        return BuildAst().Collect(engine,streaming);
    }
    
}

public class LazyFrameMergeBuilder : MergeBuilderBase<LazyFrameMergeBuilder>
{
    internal LazyFrameMergeBuilder(LazyFrame target, LazyFrame source, string[] on) 
        : base(target, source, on) { }

    /// <summary>
    /// Computes the merge execution plan and returns a LazyFrame.
    /// </summary>
    public LazyFrame Execute()
    {         
        _srcSchemaCache = _source.CollectSchema().ToFrozenDictionary();
        _tgtSchemaCache = _target.Schema.ToFrozenDictionary();
        ValidateMergePhase();
        return BuildAst();
    }
}
public partial class LazyFrame
{
    /// <summary>
    /// Initiates a Merge (Upsert) builder to seamlessly apply changes from a source DataFrame.
    /// </summary>
    public LazyFrameMergeBuilder Merge(LazyFrame source, params string[] on) => new(this, source, on);
    /// <summary>
    /// Initiates a Merge builder using a Selector or Column Expression.
    /// Usage: lf.Merge(other, Cs.Numeric()) or lf.Merge(other, Pl.Col("^id_.*$"))
    /// </summary>
    public LazyFrameMergeBuilder Merge(LazyFrame source, IntoSelector on)
    {
        using var selector = on.Consume();
        
        string[] resolvedOn = Cs.ExpandSelector(this, selector);
        
        if (resolvedOn.Length == 0)
            throw new ArgumentException("The provided selector/expression did not match any columns in the target frame.");
        var missingInSource = resolvedOn.Except(source.Schema.ColumnNames).ToList();
        if (missingInSource.Count > 0)
        {
            throw new ArgumentException(
                $"The selector resolved to join keys that do not exist in the source DataFrame: [{string.Join(", ", missingInSource)}]."
            );
        }
        return new LazyFrameMergeBuilder(this, source, resolvedOn);
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Initiates a Merge (Upsert) builder to apply changes from a source DataFrame.
    /// </summary>
    /// <param name="source">The source DataFrame containing updates/inserts.</param>
    /// <param name="on">The column names to merge on.</param>
    public DataFrameMergeBuilder Merge(DataFrame source, params string[] on)
        =>new(Lazy(), source.Lazy(), on);
    /// <summary>
    /// Initiates an eager Merge builder using a Selector or Column Expression.
    /// </summary>
    public DataFrameMergeBuilder Merge(DataFrame source, IntoSelector on)
    {
        using var selector = on.Consume();
        
        string[] resolvedOn = Cs.ExpandSelector(this, selector);

        if (resolvedOn.Length == 0)
            throw new ArgumentException("The provided selector/expression did not match any columns in the target DataFrame.");
        var missingInSource = resolvedOn.Except(source.Schema.ColumnNames).ToList();
        if (missingInSource.Count > 0)
        {
            throw new ArgumentException(
                $"The selector resolved to join keys that do not exist in the source DataFrame: [{string.Join(", ", missingInSource)}]."
            );
        }
        return new DataFrameMergeBuilder(Lazy(), source.Lazy(), resolvedOn);
    }
}