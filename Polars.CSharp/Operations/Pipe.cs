using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Offers a structured way to apply a sequence of user-defined functions (UDFs).
    /// </summary>
    /// <typeparam name="T">The return type of the function.</typeparam>
    /// <param name="function">
    /// A function that receives the current expression and returns a value of type <typeparamref name="T"/>.
    /// Typically this function wraps several Polars API calls that operate on the given expression.
    /// </param>
    /// <returns>The result of applying <paramref name="function"/> to this LazyFrame.</returns>
    public T Pipe<T>(Func<LazyFrame, T> function) => function(this);
    /// <summary>
    /// Allows to alter the lazy frame during the plan stage with the resolved schema.
    /// <para>In contrast to pipe, this method does not execute function immediately but only during the plan stage.
    ///  This allows to use the resolved schema of the input to dynamically alter the lazy frame. 
    /// This also means that any exceptions raised by function will only be emitted during the plan stage.</para>
    /// </summary>
    /// <param name="function">Callable; will receive the frame as the first parameter and the resolved schema as the second parameter.</param>
    /// <returns></returns>
    public LazyFrame PipeWithSchema(Func<LazyFrame, PolarsSchema, LazyFrame> function)
        => function(this, this.Schema);
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Offers a structured way to apply a sequence of user-defined functions (UDFs).
    /// </summary>
    /// <typeparam name="T">The return type of the function.</typeparam>
    /// <param name="function">
    /// A function that receives the current expression and returns a value of type <typeparamref name="T"/>.
    /// Typically this function wraps several Polars API calls that operate on the given expression.
    /// </param>
    /// <returns>The result of applying <paramref name="function"/> to this DataFrame.</returns>
    public T Pipe<T>(Func<DataFrame, T> function) => function(this);
}