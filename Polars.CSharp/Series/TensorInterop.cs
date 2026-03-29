using System.Numerics.Tensors;
using Polars.NET.Core;
using Polars.NET.Core.TensorInterop;

namespace Polars.CSharp;

public partial class Series : IDisposable,IPolarsSeries
{
    /// <summary>
    /// Generate zero-copy ReadOnlySpan from a numeric Series.
    /// </summary>
    /// <typeparam name="T">Unmanaged Type Only (e.g., int, float, double)</typeparam>
    /// <returns>A continuous span over the underlying memory.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the Series is not a numeric type or contains nulls.</exception>
    public ReadOnlySpan<T> AsReadOnlySpan<T>() where T : unmanaged
    {
        if (DataType == DataType.String || 
            DataType == DataType.Categorical)
        {
            throw new InvalidOperationException(
                $"Cannot create Tensor/Span from a {DataType} Series. " +
                "Machine learning models and Spans require numeric inputs. " +
                "Please use Polars string manipulation (.Str) or categorical casting to encode your strings into numbers first."
            );
        }

        return ArrowTensorInterop.AsReadOnlySpan<T>(Handle);
    }
    /// <summary>
    /// Generates a zero-copy Tensor representation from the Series.
    /// Automatically infers the shape: 
    /// - 1D [Rows] for flat numeric columns.
    /// - 2D [Rows, ListSize] for List/Array columns (Perfect for Embeddings or Feature Matrices).
    /// </summary>
    /// <typeparam name="T">Unmanaged Type Only (e.g., float, int)</typeparam>
    /// <returns>A ReadOnlyTensorSpan representing the underlying Arrow memory.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the Series contains nulls or is not numeric.</exception>
    public ReadOnlyTensorSpan<T> AsTensorSpan<T>() where T : unmanaged
        => ArrowTensorInterop.AsTensorSpan<T>(Handle);
    
    /// <summary>
    /// Generates a zero-copy N-Dimensional Tensor representation from the Series.
    /// Allows explicit reshaping of the underlying memory (e.g., for 3D/4D image matrices).
    /// </summary>
    /// <typeparam name="T">Unmanaged Type Only</typeparam>
    /// <param name="shape">The desired dimensions. Total elements must match the underlying memory.</param>
    /// <returns>A reshaped ReadOnlyTensorSpan representing the underlying Arrow memory.</returns>
    /// <exception cref="ArgumentException">Thrown if the shape's total elements do not match the Series data.</exception>
    public ReadOnlyTensorSpan<T> AsTensorSpan<T>(ReadOnlySpan<nint> shape) where T : unmanaged
        => ArrowTensorInterop.AsTensorSpan<T>(Handle, shape);

    /// <summary>
    /// Generates a zero-copy, transposed 2D Tensor representation from a List or Array Series.
    /// By manipulating memory strides, it returns an (N x M) view of an (M x N) matrix without moving bytes.
    /// </summary>
    /// <remarks>
    /// This is useful when bridging with external Machine Learning libraries 
    /// (like ONNX or native C++ math backends) that expect Column-Major order or specifically require a transposed matrix.
    /// </remarks>
    /// <typeparam name="T">Unmanaged Type Only (e.g., float, double, int).</typeparam>
    /// <returns>A transposed <see cref="ReadOnlyTensorSpan{T}"/> pointing to the original Arrow memory.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the Series is not a 2D List/Array, contains null values, or if the generic type <typeparamref name="T"/> 
    /// does not match the underlying physical Arrow memory type.
    /// </exception>
    public ReadOnlyTensorSpan<T> AsTransposedTensorSpan<T>() where T : unmanaged
        => ArrowTensorInterop.AsTransposedTensorSpan<T>(Handle);

    /// <summary>
    /// Deep copies the underlying Arrow memory into a managed .NET <see cref="Tensor{T}"/> on the garbage-collected heap.
    /// Automatically infers the rank and dimensions: 1D data is promoted to a [N, 1] column vector, 
    /// while 2D nested data becomes an [N, M] matrix.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="AsTensorSpan{T}()"/>, this method allocates memory and performs a safe physical copy. 
    /// The returned Tensor is completely independent of the Polars engine, making it safe to pass across threads 
    /// or await inside asynchronous (async/Task) workflows.
    /// </remarks>
    /// <typeparam name="T">The unmanaged primitive type of the tensor (e.g., float, int).</typeparam>
    /// <returns>A new managed <see cref="Tensor{T}"/> containing the copied data.</returns>
    public Tensor<T> AsTensor<T>() where T : unmanaged
        => ArrowTensorInterop.AsTensor<T>(Handle);

    /// <summary>
    /// Deep copies the underlying Arrow memory into a managed .NET <see cref="Tensor{T}"/> with an explicit N-dimensional shape.
    /// </summary>
    /// <remarks>
    /// This method performs a safe physical copy to the managed heap. It is highly useful when reshaping 
    /// flat or nested Arrow arrays into higher-dimensional structures (e.g., 3D/4D image batches like [Batch, Channel, Height, Width]) 
    /// required by Machine Learning models.
    /// </remarks>
    /// <typeparam name="T">The unmanaged primitive type of the tensor.</typeparam>
    /// <param name="shape">The target dimensions. The total number of elements must match the underlying Arrow memory length.</param>
    /// <returns>A new managed <see cref="Tensor{T}"/> reshaped to the specified dimensions.</returns>
    /// <exception cref="ArgumentException">Thrown if the total elements required by the shape do not match the data length.</exception>
    public Tensor<T> AsTensor<T>(ReadOnlySpan<nint> shape) where T : unmanaged
        => ArrowTensorInterop.AsTensor<T>(Handle, shape);

    /// <summary>
    /// Extracts the raw unmanaged pointer and shape metadata of the underlying physical Arrow memory.
    /// Designed for zero-copy integrations with native C++ Machine Learning backends like ONNX Runtime or TorchSharp.
    /// </summary>
    /// <remarks>
    /// DANGER (LIFECYCLE WARNING): This is a zero-copy operation. The returned <see cref="IntPtr"/> points 
    /// directly to native memory managed by the Rust Polars engine. It is ONLY valid as long as this <see cref="Series"/> 
    /// instance remains alive. You MUST ensure the Series is not garbage collected or explicitly disposed 
    /// while the native pointer is still in use by an external FFI library, otherwise it will result in a fatal segmentation fault.
    /// </remarks>
    /// <typeparam name="T">The unmanaged primitive type.</typeparam>
    /// <returns>A tuple containing the raw <see cref="IntPtr"/> to the first element, and a <c>long[]</c> representing the tensor shape.</returns>
    public (IntPtr DataPointer, long[] Shape) AsDangerousUnmanagedTensor<T>() where T : unmanaged
    {
        if (!this.IsContiguous)
        {
            throw new InvalidOperationException(
                $"Cannot extract a contiguous native pointer because the Series is fragmented into {this.NChunks} chunks. " +
                "You MUST call .Rechunk() on this Series to merge the memory before exporting it to an unmanaged Tensor."
            );
        }
        return ArrowTensorInterop.GetNativePointers<T>(Handle);
    }
    /// <summary>
    /// Extracts the raw unmanaged pointer and reshapes the metadata for native ML backends.
    /// </summary>
    /// <param name="shape">The target tensor shape. Total elements must strictly match the memory length.</param>
    public (IntPtr DataPointer, long[] Shape) AsDangerousUnmanagedTensor<T>(ReadOnlySpan<nint> shape) where T : unmanaged
    {
        if (!IsContiguous)
        {
            throw new InvalidOperationException(
                $"Cannot extract a contiguous native pointer because the Series is fragmented into {NChunks} chunks. " +
                "You MUST call .Rechunk() on this Series to merge the memory before exporting it to an unmanaged Tensor."
            );
        }
        return ArrowTensorInterop.GetNativePointers<T>(Handle, shape);
    }
     
    /// <summary>
    /// Converts an N-Dimensional .NET Tensor into a Polars Series.
    /// Automatically infers the rank and dynamically wraps the data into Polars native types:
    /// - 1D Tensors map to flat primitive columns (e.g., Float32, Int32).
    /// - N-D Tensors map to nested Array columns (e.g., Array[f32, (H, W)]).
    /// </summary>
    /// <typeparam name="T">The unmanaged primitive type of the tensor (e.g., float, int).</typeparam>
    /// <param name="name">The column name for the newly created Series.</param>
    /// <param name="tensor">The source TensorSpan. Supports sliced and transposed views safely.</param>
    /// <returns>A new Polars Series instance encapsulating the tensor data.</returns>
    /// <remarks>
    /// For memory safety against non-contiguous tensor views (like transpositions), 
    /// this method performs a memory materialization (Flatten) before mapping to Arrow's C Data Interface. 
    /// </remarks>
    public static Series FromTensor<T>(string name, ReadOnlyTensorSpan<T> tensor) where T : unmanaged
    {
        SeriesHandle handle = ArrowTensorInterop.ImportTensor(name, tensor);
        return new Series(handle);
    }
}