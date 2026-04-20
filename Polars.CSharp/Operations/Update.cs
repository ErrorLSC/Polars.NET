using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

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
        
        var otherSelectExprs = new IntoExprColumn[rightOnList.Count + rightOther.Count + (validityCol != null ? 1 : 0)];
        int selectIdx = 0;
        foreach (var col in rightOnList) otherSelectExprs[selectIdx++] = col;
        foreach (var col in rightOther) otherSelectExprs[selectIdx++] = col;
        if (validityCol != null)
        {
            dropColumns.Add(validityCol);
            otherSelectExprs[selectIdx] = validityCol;
        }

        // Execute Join 
        var leftOnExprs = leftOnList.Select(x => (IntoExprColumn)x).ToArray();
        var rightOnExprs = rightOnList.Select(x => (IntoExprColumn)x).ToArray();

        var result = leftFrame.Join(
            rightFrame.Select(otherSelectExprs),
            leftOn: leftOnExprs,   
            rightOn: rightOnExprs, 
            how: how,
            suffix: tmpSuffix,
            maintainOrder:maintainOrder,
            coalesce: JoinCoalesce.CoalesceColumns);

        // Coalesce/Update logic
        var updateExprs = new IntoExprColumn[rightOther.Count];
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
}

