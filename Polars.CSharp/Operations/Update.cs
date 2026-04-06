#pragma warning disable CS1591
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
    /// <summary>
    /// Create a delta merge context
    /// </summary>
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

public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Update the values in this Frame with the values in other.
    /// </summary>
    /// <param name="other">Frame that will be used to update the values</param>
    /// <param name="on">Column names that will be joined on. If set to Null (default), the implicit row index of each frame is used as a join key.</param>
    /// <param name="how"><para> 'Left' will keep all rows from the left table; rows may be duplicated if multiple rows in the right frame match the left row’s key.</para>
    /// <para>‘Inner’: keeps only those rows where the key exists in both frames.</para>
    /// ‘Full’: will update existing rows where the key matches while also adding any new rows contained in the given frame.</param>
    /// <param name="leftOn">Join column(s) of the left Frame.</param>
    /// <param name="rightOn">Join column(s) of the right Frame.</param>
    /// <param name="includeNulls">Overwrite values in the left frame with null values from the right frame. If set to False (default), null values in the right frame are ignored.</param>
    /// <param name="maintainOrder">Which order of rows from the inputs to preserve. See join() for details. Unlike join this function preserves the left order by default.</param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public LazyFrame Update(
        LazyFrame other,
        IEnumerable<string>? on = null,
        JoinType how = JoinType.Left,
        IEnumerable<string>? leftOn = null,
        IEnumerable<string>? rightOn = null,
        bool includeNulls = false,
        JoinMaintainOrder maintainOrder =JoinMaintainOrder.Left)
    {
        // Validate the 'how' parameter
        if (how != JoinType.Left && how != JoinType.Inner && how != JoinType.Outer)
        {
            throw new ArgumentException($"'how' must be one of {{JoinType.Left, JoinType.Inner, JoinType.Outer}}; found '{how}'");
        }

        bool rowIndexUsed = false;
        string rowIndexName = "__POLARS_ROW_INDEX";

        LazyFrame leftFrame = this;
        LazyFrame rightFrame = other;

        // Resolve join keys
        if (on == null)
        {
            if (leftOn == null && rightOn == null)
            {
                rowIndexUsed = true;
                leftFrame = leftFrame.WithRowIndex(rowIndexName);
                rightFrame = rightFrame.WithRowIndex(rowIndexName);
                leftOn = [rowIndexName];
                rightOn = [rowIndexName];
            }
            else
            {
                if (leftOn == null) throw new ArgumentException("Missing join columns for left frame.");
                if (rightOn == null) throw new ArgumentException("Missing join columns for right frame.");
            }
        }
        else
        {
            leftOn = on;
            rightOn = on;
        }

        var leftOnList = leftOn.ToList();
        var rightOnList = rightOn.ToList();

        var leftCols = leftFrame.Schema.ToList().Select(x => x.Name).ToList();
        foreach (var name in leftOnList)
        {
            if (!leftCols.Contains(name))
                throw new ArgumentException($"Left join column '{name}' not found.");
        }

        var rightCols = rightFrame.Schema.ToList().Select(x => x.Name).ToList();
        foreach (var name in rightOnList)
        {
            if (!rightCols.Contains(name))
                throw new ArgumentException($"Right join column '{name}' not found.");
        }

        // Early return optimization
        if (how != JoinType.Outer && rightCols.Count == rightOnList.Count)
        {
            if (rowIndexUsed) return leftFrame.Drop(rowIndexName);
            return leftFrame;
        }

        // Find columns to update
        var rightOther = rightCols
            .Intersect(leftCols)
            .Except(rightOnList)
            .ToList();

        // Handle Null inclusion
        string? validityCol = null;
        if (includeNulls)
        {
            validityCol = "__POLARS_VALIDITY";
            rightFrame = rightFrame.WithColumns(Pl.Lit(true).Alias(validityCol));
        }

        // Prepare temporary columns and Select expressions
        string tmpSuffix = "__POLARS_RIGHT";
        var dropColumns = rightOther.Select(name => $"{name}{tmpSuffix}").ToList();
        
        var otherSelectExprs = new IntoExpr[rightOnList.Count + rightOther.Count + (validityCol != null ? 1 : 0)];
        int selectIdx = 0;
        foreach (var col in rightOnList) otherSelectExprs[selectIdx++] = col;
        foreach (var col in rightOther) otherSelectExprs[selectIdx++] = col;
        if (validityCol != null)
        {
            dropColumns.Add(validityCol);
            otherSelectExprs[selectIdx] = validityCol;
        }

        // Execute Join 
        var leftOnExprs = leftOnList.Select(x => (IntoExpr)x).ToArray();
        var rightOnExprs = rightOnList.Select(x => (IntoExpr)x).ToArray();

        var result = leftFrame.Join(
            rightFrame.Select(otherSelectExprs),
            leftOn: leftOnExprs,   
            rightOn: rightOnExprs, 
            how: how,
            suffix: tmpSuffix,
            maintainOrder:maintainOrder,
            coalesce: JoinCoalesce.CoalesceColumns);

        // Coalesce/Update logic
        var updateExprs = new IntoExpr[rightOther.Count];
        for (int i = 0; i < rightOther.Count; i++)
        {
            string name = rightOther[i];
            string rightColName = $"{name}{tmpSuffix}";

            if (includeNulls)
            {
                updateExprs[i] = Pl.When(Pl.Col(validityCol!).IsNull())
                                       .Then(name)
                                       .Otherwise(rightColName)
                                       .Alias(name);
            }
            else
            {
                updateExprs[i] = Pl.Col(rightColName).Coalesce(name).Alias(name);
            }
        }

        // Apply updates
        if (updateExprs.Length > 0)
        {
            result = result.WithColumns(updateExprs);
        }

        if (dropColumns.Count > 0)
        {
            result = result.Drop(dropColumns);
        }

        if (rowIndexUsed)
        {
            result = result.Drop(rowIndexName);
        }

        return result;
    }
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

        return new LazyFrameMergeBuilder(this, source, resolvedOn);
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <inheritdoc cref="LazyFrame.Update"/>
    public DataFrame Update(
        DataFrame other,
        IEnumerable<string>? on = null,
        JoinType how = JoinType.Left,
        IEnumerable<string>? leftOn = null,
        IEnumerable<string>? rightOn = null,
        bool includeNulls = false,
        JoinMaintainOrder maintainOrder =JoinMaintainOrder.Left)
    {
        using var right = other.Lazy();
        return Lazy().Update(right,on,how,leftOn,rightOn,includeNulls,maintainOrder).Collect();
    }
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

        return new DataFrameMergeBuilder(Lazy(), source.Lazy(), resolvedOn);
    }
}

/// <summary>
/// A builder for Polars Merge
/// </summary>
public abstract class MergeBuilderBase<TBuilder> where TBuilder : MergeBuilderBase<TBuilder>
{
    protected readonly LazyFrame _target;
    protected readonly LazyFrame _source;
    protected readonly string[] _on;

    protected bool _includeNulls = false;
    protected JoinMaintainOrder _maintainOrder = JoinMaintainOrder.Left;

    // --- 动态上下文与防碰撞设计 ---
    protected readonly string _tmpSfx = $"__TMP_{Guid.NewGuid().ToString("N")[..8]}";
    protected readonly string _actionCol = "__ACTION";
    protected readonly MergeContext _ctx;

    // 动作队列 (只存评估好的 Expr，不用再存 Lambda)
    protected readonly List<(MergeActionType Type, Expr Condition)> _actions = [];

    protected MergeBuilderBase(LazyFrame target, LazyFrame source, string[] on)
    {
        _target = target;
        _source = source;
        _on = on;
        _ctx = new MergeContext(_tmpSfx); 
    }
    /// <summary>
    /// Update the matched target row. Optionally filtered by a condition.
    /// </summary>
    public TBuilder WhenMatchedUpdate(Func<MergeContext, Expr>? conditionBuilder = null)
    {
        Expr cond = conditionBuilder != null ? conditionBuilder(_ctx) : Pl.Lit(true);
        _actions.Add((MergeActionType.MatchedUpdate, cond));
        return (TBuilder)this;
    }
    /// <summary>
    /// Delete the matched target row if the condition is met.
    /// </summary>
    public TBuilder WhenMatchedDelete(Func<MergeContext, Expr>? conditionBuilder = null)
    {
        Expr cond = conditionBuilder != null ? conditionBuilder(_ctx) : Pl.Lit(true);
        _actions.Add((MergeActionType.MatchedDelete, cond));
        return (TBuilder)this;
    }

    /// <summary>
    /// Insert a new row from the source if not matched. Optionally filtered.
    /// </summary>
    public TBuilder WhenNotMatchedInsert(Func<MergeContext, Expr>? conditionBuilder = null)
    {
        Expr cond = conditionBuilder != null ? conditionBuilder(_ctx) : Pl.Lit(true);
        _actions.Add((MergeActionType.NotMatchedInsert, cond));
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

    // --- AST Compile Engine  ---
    protected LazyFrame BuildAst()
    {
        if (_actions.Count == 0)
        {
            WhenMatchedUpdate();
            WhenNotMatchedInsert();
        }

        string tgtVal = "__TGT", srcVal = "__SRC";
        
        var tgt = _target.WithColumns(Pl.Lit(true).Alias(tgtVal));
        
        var srcCols = _source.Schema.ColumnNames.Except(_on).ToList();
        
        var src = _source.Select(
            Cs.ByName(_on),                               
            Cs.Exclude(_on).ToExpr().Name.Suffix(_tmpSfx),          
            Pl.Lit(true).Alias(srcVal)                     
        );

        var joined = tgt.Join(src, _on, 
            how: _actions.Any(a => a.Type == MergeActionType.NotMatchedInsert) ? JoinType.Outer : JoinType.Left,
            coalesce: JoinCoalesce.CoalesceColumns, 
            maintainOrder: _maintainOrder);

        var isMatched = Pl.Col(tgtVal).IsNotNull() & Pl.Col(srcVal).IsNotNull();
        var isSourceOnly = Pl.Col(tgtVal).IsNull() & Pl.Col(srcVal).IsNotNull();
        var isTargetOnly = Pl.Col(tgtVal).IsNotNull() & Pl.Col(srcVal).IsNull();

        Expr actionExpr = Pl.When(isTargetOnly).Then(0).When(isMatched).Then(0).Otherwise(4);
        for (int i = _actions.Count - 1; i >= 0; i--)
        {
            var action = _actions[i];
            Expr cond = Pl.Lit(false);
            int actionCode = 0;

            if (action.Type == MergeActionType.MatchedDelete) { cond = isMatched & action.Condition; actionCode = 2; }
            else if (action.Type == MergeActionType.MatchedUpdate) { cond = isMatched & action.Condition; actionCode = 1; }
            else if (action.Type == MergeActionType.NotMatchedInsert) { cond = isSourceOnly & action.Condition; actionCode = 3; }

            actionExpr = Pl.When(cond).Then(actionCode).Otherwise(actionExpr);
        }
        joined = joined.WithColumns(actionExpr.Alias(_actionCol));

        var updateExprs = new List<IntoExpr>();
        var doUpdate = (Pl.Col(_actionCol) == 1) | (Pl.Col(_actionCol) == 3);

        foreach (var colName in srcCols)
        {
            var tgtCol = _target.Schema.ColumnNames.Contains(colName) ? Pl.Col(colName) : Pl.LitNull();
            var srcColTmp = Pl.Col(colName + _tmpSfx);

            var finalUpdateCond = doUpdate;
            if (!_includeNulls) finalUpdateCond &= srcColTmp.IsNotNull();

            updateExprs.Add(
                Pl.When(finalUpdateCond).Then(srcColTmp).Otherwise(tgtCol).Alias(colName)
            );
        }
        joined = joined.WithColumns(updateExprs);
        joined = joined.Filter((Pl.Col(_actionCol) != 2) & (Pl.Col(_actionCol) != 4));

        return joined.Drop(
            Cs.EndsWith(_tmpSfx),
            tgtVal, 
            srcVal, 
            _actionCol
        );
    }
}

public class DataFrameMergeBuilder : MergeBuilderBase<DataFrameMergeBuilder>
{
    internal DataFrameMergeBuilder(LazyFrame target, LazyFrame source, string[] on) 
        : base(target, source, on) { }

    /// <summary>
    /// Executes the merge operation eagerly and returns a materialized DataFrame.
    /// </summary>
    public DataFrame Execute(bool streaming=false) => BuildAst().Collect(streaming);
    
}

public class LazyFrameMergeBuilder : MergeBuilderBase<LazyFrameMergeBuilder>
{
    internal LazyFrameMergeBuilder(LazyFrame target, LazyFrame source, string[] on) 
        : base(target, source, on) { }

    /// <summary>
    /// Computes the merge execution plan and returns a LazyFrame.
    /// </summary>
    public LazyFrame Execute() => BuildAst();
}