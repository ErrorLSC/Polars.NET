using System.Linq.Expressions;
using Polars.NET.Linq;

namespace Polars.CSharp.Tests;

public class PartialEvaluatorTests
{
    [Fact]
    [Trait("LINQ", "PartialEvaluator")]
    public void Eval_StaticLiterals_LeavesIntact()
    {
        Expression<Func<int, bool>> expr = x => x > 42;
        var evaluated = (LambdaExpression)PartialEvaluator.eval(expr);

        var binary = Assert.IsType<BinaryExpression>(evaluated.Body, exactMatch: false);
        var right = Assert.IsType<ConstantExpression>(binary.Right);
        Assert.Equal(42, right.Value);
    }

    [Fact]
    [Trait("LINQ", "PartialEvaluator")]
    public void Eval_ClosureVariable_FoldsToConstant()
    {
        var threshold = 100;
        Expression<Func<int, bool>> expr = x => x > threshold;
        var evaluated = (LambdaExpression)PartialEvaluator.eval(expr);

        var binary = Assert.IsType<BinaryExpression>(evaluated.Body, exactMatch: false);
        var right = Assert.IsType<ConstantExpression>(binary.Right);
        Assert.Equal(100, right.Value);
    }

    [Fact]
    [Trait("LINQ", "PartialEvaluator")]
    public void Extract_StaticLiterals_LeavesAsConstants()
    {
        Expression<Func<int, bool>> expr = x => x > 42;
        var result = PartialEvaluator.extract(expr);

        Assert.Empty(result.Parameters);
        var lambda = Assert.IsType<LambdaExpression>(result.NormalizedExpression, exactMatch: false);
        var binary = Assert.IsType<BinaryExpression>(lambda.Body, exactMatch: false);
        var right = Assert.IsType<ConstantExpression>(binary.Right);
        Assert.Equal(42, right.Value);
    }

    [Fact]
    [Trait("LINQ", "PartialEvaluator")]
    public void Extract_ClosureVariable_ExtractsParameter()
    {
        var container = "MED BOX";
        Expression<Func<string, bool>> expr = s => s == container;
        var result = PartialEvaluator.extract(expr);

        Assert.Single(result.Parameters);
        Assert.True(result.Parameters.ContainsKey("@p0"));
        var param = result.Parameters["@p0"];
        Assert.Equal("@p0", param.Name);
        Assert.Equal(typeof(string), param.Type);
        Assert.Equal("MED BOX", param.Value);

        var lambda = Assert.IsType<LambdaExpression>(result.NormalizedExpression, exactMatch: false);
        var binary = Assert.IsType<BinaryExpression>(lambda.Body, exactMatch: false);
        var rightParam = Assert.IsType<ParameterExpression>(binary.Right, exactMatch: false);
        Assert.Equal("@p0", rightParam.Name);
    }

    [Fact]
    [Trait("LINQ", "PartialEvaluator")]
    public void Extract_MultipleClosureVariables_ExtractsAllInOrder()
    {
        var brand = "Brand#23";
        var container = "MED BOX";
        Expression<Func<string, string, bool>> expr = (b, c) => b == brand && c == container;
        var result = PartialEvaluator.extract(expr);

        Assert.Equal(2, result.Parameters.Count);
        Assert.Equal("Brand#23", result.Parameters["@p0"].Value);
        Assert.Equal("MED BOX", result.Parameters["@p1"].Value);

        var lambda = Assert.IsType<LambdaExpression>(result.NormalizedExpression, exactMatch: false);
        var binary = Assert.IsType<BinaryExpression>(lambda.Body, exactMatch: false);
        var leftComp = Assert.IsType<BinaryExpression>(binary.Left, exactMatch: false);
        var rightComp = Assert.IsType<BinaryExpression>(binary.Right, exactMatch: false);

        Assert.Equal("@p0", Assert.IsType<ParameterExpression>(leftComp.Right, exactMatch: false).Name);
        Assert.Equal("@p1", Assert.IsType<ParameterExpression>(rightComp.Right, exactMatch: false).Name);
    }

    [Fact]
    [Trait("LINQ", "PartialEvaluator")]
    public void Extract_NestedClosure_FastReflection_ExtractsCorrectly()
    {
        var outerVar = "OuterValue";

        void Inner(string innerVar)
        {
            Expression<Func<string, bool>> expr = s => s == outerVar && s == innerVar;
            var result = PartialEvaluator.extract(expr);

            Assert.Equal(2, result.Parameters.Count);
            Assert.Equal("OuterValue", result.Parameters["@p0"].Value);
            Assert.Equal("InnerValue", result.Parameters["@p1"].Value);
        }

        Inner("InnerValue");
    }

    [Fact]
    [Trait("LINQ", "PartialEvaluator")]
    public void Extract_ComplexSubtree_EvaluatesMaximalSubtree()
    {
        var offset = 10;
        Expression<Func<int, bool>> expr = x => x > offset + 5;
        var result = PartialEvaluator.extract(expr);

        Assert.Single(result.Parameters);
        Assert.Equal(15, result.Parameters["@p0"].Value);

        var lambda = Assert.IsType<LambdaExpression>(result.NormalizedExpression, exactMatch: false);
        var binary = Assert.IsType<BinaryExpression>(lambda.Body, exactMatch: false);
        var right = Assert.IsType<ParameterExpression>(binary.Right, exactMatch: false);
        Assert.Equal("@p0", right.Name);
    }

    [Fact]
    [Trait("LINQ", "PartialEvaluator")]
    public void Substitute_RoundTrip_RestoresConstants()
    {
        var container = "MED BOX";
        Expression<Func<string, bool>> expr = s => s == container;

        var extracted = PartialEvaluator.extract(expr);
        var substituted = (LambdaExpression)PartialEvaluator.substituteExtracted(
            extracted.NormalizedExpression,
            extracted.Parameters
        );

        var binary = Assert.IsType<BinaryExpression>(substituted.Body, exactMatch: false);
        var right = Assert.IsType<ConstantExpression>(binary.Right);
        Assert.Equal("MED BOX", right.Value);
    }

    [Fact]
    [Trait("LINQ", "PartialEvaluator")]
    public void Substitute_WithNewValues_ReplacesCorrectly()
    {
        var container = "MED BOX";
        Expression<Func<string, bool>> expr = s => s == container;

        var extracted = PartialEvaluator.extract(expr);

        var newParams = new Dictionary<string, object>
        {
            ["@p0"] = "MED DRUM"
        };

        var substituted = (LambdaExpression)PartialEvaluator.substitute(
            extracted.NormalizedExpression,
            newParams
        );

        var binary = Assert.IsType<BinaryExpression>(substituted.Body, exactMatch: false);
        var right = Assert.IsType<ConstantExpression>(binary.Right);
        Assert.Equal("MED DRUM", right.Value);
    }
}
