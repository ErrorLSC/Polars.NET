---
uid: Polars.CSharp.Series
title: Series
---

# Series

Namespace: `Polars.CSharp`

Represents a single column of data (1-dimensional array). 

A Series is backed by Apache Arrow arrays and supports eager execution. Operations on Series are generally performed immediately.

## Sub-namespaces

| Sub-namespace | Page | Description |
| :--- | :--- | :--- |
| `.Array` | [Series.Array](Series.Array.md) | Access Fixed-Size List (Array) operations. |
| `.Bin` | [Series.Bin](Series.Bin.md) | Access binary operations. |
| `.Cat` | [Series.Cat](Series.Cat.md) | Access categorical operations. |
| `.Dt` | [Series.Dt](Series.Dt.md) | Date Ops |
| `.Ext` | [Series.Ext](Series.Ext.md) | Access extension datatype operations. |
| `.List` | [Series.List](Series.List.md) | Access list operations. |
| `.Str` | [Series.Str](Series.Str.md) | String Ops |
| `.Struct` | [Series.Struct](Series.Struct.md) | Access struct operations. |

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `DataType` | `DataType` | Gets the DataType of the Series. |
| `DataTypeName` | `string` | Get the string representation of the Series data type (e.g. "i64", "str", "datetime(μs)"). |
| `IsContiguous` | `bool` | Determines if the Series memory is physically contiguous (i.e., consists of a single chunk). |
| `Length` | `long` | Length of the Series. |
| `NChunks` | `long` | Gets the number of underlying Arrow memory chunks. |
| `Name` | `string` | Name of the Series. |
| `NullCount` | `long` | Get the number of null values in the Series. |
| `Shape` | `long[]` | Shape of this Series. In Polars, a Series is always 1D, so this returns an array of length 1. |
| `SortedFlags` | `SortStateFlags` | Gets the current metadata sortedness flags of this Series. (O(1) operation) |
| `this[Range range]` | `Series` | Range Indexer (__getitem__ for slices) e.g. s[1..5] / s[..^1] |
| `this[Series key]` | `object` | e.g. s[s > 5] = 10; |
| `this[int index]` | `object` | Get an item at the specified index as object (boxed). |
| `this[int[] indices]` | `object` | e.g. s[0, 2, 5] = 99; |

## Constructors & Static Methods

### Series

This will create an empty series, if dtype is also null, datatype will be null.

```csharp
public Series(string name, ReadOnlySpan<sbyte> data)
public Series(string name, ReadOnlySpan<sbyte?> data)
public Series(string name, ReadOnlySpan<short> data)
public Series(string name, ReadOnlySpan<short?> data)
public Series(string name, ReadOnlySpan<int> data)
public Series(string name, ReadOnlySpan<int?> data)
public Series(string name, ReadOnlySpan<long> data)
public Series(string name, ReadOnlySpan<long?> data)
public Series(string name, ReadOnlySpan<Int128> data)
public Series(string name, ReadOnlySpan<Int128?> data)
public Series(string name, ReadOnlySpan<byte> data)
public Series(string name, ReadOnlySpan<byte?> data)
public Series(string name, ReadOnlySpan<ushort> data)
public Series(string name, ReadOnlySpan<ushort?> data)
public Series(string name, ReadOnlySpan<uint> data)
public Series(string name, ReadOnlySpan<uint?> data)
public Series(string name, ReadOnlySpan<ulong> data)
public Series(string name, ReadOnlySpan<ulong?> data)
public Series(string name, ReadOnlySpan<UInt128> data)
public Series(string name, ReadOnlySpan<UInt128?> data)
public Series(string name, ReadOnlySpan<Half> data)
public Series(string name, ReadOnlySpan<Half?> data)
public Series(string name, ReadOnlySpan<float> data)
public Series(string name, ReadOnlySpan<float?> data)
public Series(string name, ReadOnlySpan<double> data)
public Series(string name, ReadOnlySpan<double?> data)
public Series(string name, ReadOnlySpan<bool> data)
public Series(string name, ReadOnlySpan<bool?> data)
public Series(string name, ReadOnlySpan<string> data)
public Series(string name, ReadOnlySpan<decimal> data)
public Series(string name, ReadOnlySpan<decimal?> data)
public Series(string name, ReadOnlySpan<DateTime> data)
public Series(string name, ReadOnlySpan<DateTime?> data)
public Series(string name, ReadOnlySpan<DateTimeOffset> data)
public Series(string name, ReadOnlySpan<DateTimeOffset?> data)
public Series(string name, ReadOnlySpan<DateOnly> data)
public Series(string name, ReadOnlySpan<DateOnly?> data)
public Series(string name, ReadOnlySpan<TimeOnly> data)
public Series(string name, ReadOnlySpan<TimeOnly?> data)
public Series(string name, ReadOnlySpan<TimeSpan> data)
public Series(string name, ReadOnlySpan<TimeSpan?> data)
public Series(string name, sbyte[,] data)
public Series(string name, byte[,] data)
public Series(string name, short[,] data)
public Series(string name, ushort[,] data)
public Series(string name, int[,] data)
public Series(string name, uint[,] data)
public Series(string name, long[,] data)
public Series(string name, ulong[,] data)
public Series(string name, Half[,] data)
public Series(string name, float[,] data)
public Series(string name, double[,] data)
public Series(string name, decimal[,] data)
public Series(string name, Int128[,] data)
public Series(string name, UInt128[,] data)
public Series(string name = "", DataType? dtype = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | - |
| `data` | `ReadOnlySpan<sbyte> / ReadOnlySpan<sbyte?> / ReadOnlySpan<short> / ReadOnlySpan<short?> / ReadOnlySpan<int> / ReadOnlySpan<int?> / ReadOnlySpan<long> / ReadOnlySpan<long?> / ReadOnlySpan<Int128> / ReadOnlySpan<Int128?> / ReadOnlySpan<byte> / ReadOnlySpan<byte?> / ReadOnlySpan<ushort> / ReadOnlySpan<ushort?> / ReadOnlySpan<uint> / ReadOnlySpan<uint?> / ReadOnlySpan<ulong> / ReadOnlySpan<ulong?> / ReadOnlySpan<UInt128> / ReadOnlySpan<UInt128?> / ReadOnlySpan<Half> / ReadOnlySpan<Half?> / ReadOnlySpan<float> / ReadOnlySpan<float?> / ReadOnlySpan<double> / ReadOnlySpan<double?> / ReadOnlySpan<bool> / ReadOnlySpan<bool?> / ReadOnlySpan<string> / ReadOnlySpan<decimal> / ReadOnlySpan<decimal?> / ReadOnlySpan<DateTime> / ReadOnlySpan<DateTime?> / ReadOnlySpan<DateTimeOffset> / ReadOnlySpan<DateTimeOffset?> / ReadOnlySpan<DateOnly> / ReadOnlySpan<DateOnly?> / ReadOnlySpan<TimeOnly> / ReadOnlySpan<TimeOnly?> / ReadOnlySpan<TimeSpan> / ReadOnlySpan<TimeSpan?> / sbyte[,] / short[,] / ushort[,] / int[,] / uint[,] / long[,] / ulong[,] / Half[,] / float[,] / double[,] / decimal[,] / Int128[,] / UInt128[,]` | - |
| `dtype` | `DataType?` | - |

---

### From

Create a Series from a list of objects, primitives, or nested lists. Uses SeriesFactory to automatically select the fastest path (SIMD for Arrays, Reflection for Lists).

Create Series from array

Create Series from 2D matrix

```csharp
public static Series From<T>(string name, IEnumerable<T> data)
public static Series From<T>(string name, T[] data)
public static Series From<T>(string name, T[,] data)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | - |
| `data` | `IEnumerable<T> / T[] / T[,]` | - |

---

### FromArrow

Low-level entry point: Create Series from existing Arrow Array.

```csharp
public static Series FromArrow(string name, IArrowArray arrowArray)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | - |
| `arrowArray` | `IArrowArray` | - |

---

### FromExpr

Create Series From single column expression.

```csharp
public static Series FromExpr(Expr expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | - |

---

### FromSpan

Create Series from ReadOnlySpan

```csharp
public static Series FromSpan<T>(string name, ReadOnlySpan<T> data)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | - |
| `data` | `ReadOnlySpan<T>` | - |

---

### FromTensor

Converts an N-Dimensional .NET Tensor into a Polars Series. Automatically infers the rank and dynamically wraps the data into Polars native types: - 1D Tensors map to flat primitive columns (e.g., Float32, Int32). - N-D Tensors map to nested Array columns (e.g., Array[f32, (H, W)]).

```csharp
public static Series FromTensor<T>(string name, ReadOnlyTensorSpan<T> tensor)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | The column name for the newly created Series. |
| `tensor` | `ReadOnlyTensorSpan<T>` | The source TensorSpan. Supports sliced and transposed views safely. |

**Returns:** A new Polars Series instance encapsulating the tensor data.

> [!NOTE]
> For memory safety against non-contiguous tensor views (like transpositions), this method performs a memory materialization (Flatten) before mapping to Arrow's C Data Interface.

---

## Operators

### operator + (Addition)

Add Series

```csharp
public static Series operator +(Series left, Series right)
public static Series operator +(Series left, IntoExpr right)
public static Series operator +(IntoExpr left, Series right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series / IntoExpr` | - |
| `right` | `Series / IntoExpr` | - |

---

### operator / (Division)

Divide Series

```csharp
public static Series operator /(Series left, Series right)
public static Series operator /(Series left, IntoExpr right)
public static Series operator /(IntoExpr left, Series right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series / IntoExpr` | - |
| `right` | `Series / IntoExpr` | - |

---

### operator == (Equality)

```csharp
public static Series operator ==(Series left, Series right)
public static Series operator ==(Series left, IntoExpr right)
public static Series operator ==(IntoExpr left, Series right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series / IntoExpr` | - |
| `right` | `Series / IntoExpr` | - |

---

### operator ^ (Exclusive OR)

Exponentiation Series

```csharp
public static Series operator ^(Series left, Series right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series` | - |
| `right` | `Series` | - |

---

### operator > (Greater Than)

Compare whether left series is greater than right series

```csharp
public static Series operator >(Series left, Series right)
public static Series operator >(Series left, IntoExpr right)
public static Series operator >(IntoExpr left, Series right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series / IntoExpr` | - |
| `right` | `Series / IntoExpr` | - |

---

### operator >= (Greater Or Equal)

Compare whether left series is greater than or equal to right series

```csharp
public static Series operator >=(Series left, Series right)
public static Series operator >=(Series left, IntoExpr right)
public static Series operator >=(IntoExpr left, Series right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series / IntoExpr` | - |
| `right` | `Series / IntoExpr` | - |

---

### operator != (Inequality)

```csharp
public static Series operator !=(Series left, Series right)
public static Series operator !=(Series left, IntoExpr right)
public static Series operator !=(IntoExpr left, Series right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series / IntoExpr` | - |
| `right` | `Series / IntoExpr` | - |

---

### operator << (Left Shift)

Bitwise left shift operation.

```csharp
public static Series operator <<(Series left, int right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series` | - |
| `right` | `int` | - |

---

### operator < (Less Than)

Compare whether left series is less than right series

```csharp
public static Series operator <(Series left, Series right)
public static Series operator <(Series left, IntoExpr right)
public static Series operator <(IntoExpr left, Series right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series / IntoExpr` | - |
| `right` | `Series / IntoExpr` | - |

---

### operator <= (Less Or Equal)

Compare whether left series is less than or equal to right series

```csharp
public static Series operator <=(Series left, Series right)
public static Series operator <=(Series left, IntoExpr right)
public static Series operator <=(IntoExpr left, Series right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series / IntoExpr` | - |
| `right` | `Series / IntoExpr` | - |

---

### operator ! (Logical NOT)

```csharp
public static Series operator !(Series booleanSeries)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `booleanSeries` | `Series` | - |

---

### operator % (Modulus)

Mod Series

```csharp
public static Series operator %(Series left, Series right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series` | - |
| `right` | `Series` | - |

---

### operator * (Multiply)

Multiple Series

```csharp
public static Series operator *(Series left, Series right)
public static Series operator *(Series left, IntoExpr right)
public static Series operator *(IntoExpr left, Series right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series / IntoExpr` | - |
| `right` | `Series / IntoExpr` | - |

---

### operator ~ (Bitwise NOT)

```csharp
public static Series operator ~(Series booleanSeries)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `booleanSeries` | `Series` | - |

---

### operator >> (Right Shift)

Bitwise right shift operation. 

For signed integers, this is arithmetic shift. For unsigned integers, this is logical shift.

```csharp
public static Series operator >>(Series left, int right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series` | - |
| `right` | `int` | - |

---

### operator - (Subtraction)

Minus Series

```csharp
public static Series operator -(Series left, Series right)
public static Series operator -(Series left, IntoExpr right)
public static Series operator -(IntoExpr left, Series right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Series / IntoExpr` | - |
| `right` | `Series / IntoExpr` | - |

---

## Instance Methods

### Abs

Calculate absolute value.

```csharp
public Series Abs()
```

---

### Alias

Rename Series. Same as Rename.

```csharp
public Series Alias(string newName)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `newName` | `string` | - |

---

### All

Check if all values in the boolean expression are `true`. 

This is a boolean aggregation.

```csharp
public bool? All(bool ignoreNulls = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | If `true`, null values are ignored. If `false` (default), the result propagates nulls (i.e., if there is a null and no false, the result might be null). |

---

### Any

Check if any value in the boolean expression is `true`. 

This is a boolean aggregation.

```csharp
public bool? Any(bool ignoreNulls = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | If `true`, null values are ignored. If `false` (default), the result propagates nulls. |

---

### Append

Append a Series to this one. The resulting series will consist of multiple chunks.

```csharp
public Series Append(Series other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Series` | Series to append. |

---

### ApproxNUnique

Get an approximation of the number of unique values in this Series. Uses HyperLogLog algorithm for fast, memory-efficient counting.

```csharp
public long ApproxNUnique()
```

**Returns:** Approximate count of unique values.

---

### ArcCos

Compute the element-wise inverse cosine.

```csharp
public Series ArcCos()
```

---

### ArcCosh

Compute the element-wise inverse hyperbolic cosine.

```csharp
public Series ArcCosh()
```

---

### ArcSin

Compute the element-wise inverse sine.

```csharp
public Series ArcSin()
```

---

### ArcSinh

Compute the element-wise inverse hyperbolic sine.

```csharp
public Series ArcSinh()
```

---

### ArcTan

Compute the element-wise inverse tangent.

```csharp
public Series ArcTan()
```

---

### ArcTanh

Compute the element-wise inverse hyperbolic tangent.

```csharp
public Series ArcTanh()
```

---

### ArgMax

Get the index of the maximum value. Returns null if the Series is empty or contains only null values.

```csharp
public long? ArgMax()
```

---

### ArgMin

Get the index of the minimum value. Returns null if the Series is empty or contains only null values.

```csharp
public long? ArgMin()
```

---

### ArgSort

Get the index values that would sort this expression.

```csharp
public Series ArgSort(bool descending = false, bool nullsLast = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `descending` | `bool` | If true, sort in descending order. Default is false. |
| `nullsLast` | `bool` | If true, place null values last. Default is false. |

---

### ArgTrue

Get index values where Boolean Series evaluate True.

```csharp
public Series ArgTrue()
```

**Returns:** Series of data type UInt32.

---

### ArgUnique

Get the index of the unique values.

```csharp
public Series ArgUnique()
```

---

### As

Returns a zero-allocation stack enumerator for strongly-typed iteration. Usage: foreach (int val in series.As{int}()) { ... }

```csharp
public SeriesEnumerator<T> As<T>()
```

---

### AsDangerousUnmanagedTensor

Extracts the raw unmanaged pointer and shape metadata of the underlying physical Arrow memory. Designed for zero-copy integrations with native C++ Machine Learning backends like ONNX Runtime or TorchSharp.

Extracts the raw unmanaged pointer and reshapes the metadata for native ML backends.

```csharp
public (IntPtr, long[]) AsDangerousUnmanagedTensor<T>()
public (IntPtr, long[]) AsDangerousUnmanagedTensor<T>(ReadOnlySpan<IntPtr> shape)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `shape` | `ReadOnlySpan<IntPtr>` | The target tensor shape. Total elements must strictly match the memory length. |

**Returns:** A tuple containing the raw `System.IntPtr` to the first element, and a `long[]` representing the tensor shape.

> [!NOTE]
> DANGER (LIFECYCLE WARNING): This is a zero-copy operation. The returned `System.IntPtr` points directly to native memory managed by the Rust Polars engine. It is ONLY valid as long as this `Series` instance remains alive. You MUST ensure the Series is not garbage collected or explicitly disposed. Considering use GC.KeepAlive(series) while the native pointer is still in use by an external FFI library, otherwise it will result in a fatal segmentation fault.

---

### AsReadOnlySpan

Generate zero-copy ReadOnlySpan from a numeric Series.

```csharp
public ReadOnlySpan<T> AsReadOnlySpan<T>()
```

**Returns:** A continuous span over the underlying memory.

---

### AsTensor

Deep copies the underlying Arrow memory into a managed .NET `System.Numerics.Tensors.Tensor` on the garbage-collected heap. Automatically infers the rank and dimensions: 1D data is promoted to a [N, 1] column vector, while 2D nested data becomes an [N, M] matrix.

Deep copies the underlying Arrow memory into a managed .NET `System.Numerics.Tensors.Tensor` with an explicit N-dimensional shape.

```csharp
public Tensor<T> AsTensor<T>()
public Tensor<T> AsTensor<T>(ReadOnlySpan<IntPtr> shape)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `shape` | `ReadOnlySpan<IntPtr>` | The target dimensions. The total number of elements must match the underlying Arrow memory length. |

**Returns:** A new managed `System.Numerics.Tensors.Tensor` containing the copied data. / A new managed `System.Numerics.Tensors.Tensor` reshaped to the specified dimensions.

> [!NOTE]
> Unlike `Series.AsTensorSpan`, this method allocates memory and performs a safe physical copy. The returned Tensor is completely independent of the Polars engine, making it safe to pass across threads or await inside asynchronous (async/Task) workflows.
> This method performs a safe physical copy to the managed heap. It is highly useful when reshaping flat or nested Arrow arrays into higher-dimensional structures (e.g., 3D/4D image batches like [Batch, Channel, Height, Width]) required by Machine Learning models.

---

### AsTensorSpan

Generates a zero-copy Tensor representation from the Series. Automatically infers the shape: - 1D [Rows] for flat numeric columns. - 2D [Rows, ListSize] for List/Array columns (Perfect for Embeddings or Feature Matrices).

Generates a zero-copy N-Dimensional Tensor representation from the Series. Allows explicit reshaping of the underlying memory (e.g., for 3D/4D image matrices).

```csharp
public ReadOnlyTensorSpan<T> AsTensorSpan<T>()
public ReadOnlyTensorSpan<T> AsTensorSpan<T>(ReadOnlySpan<IntPtr> shape)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `shape` | `ReadOnlySpan<IntPtr>` | The desired dimensions. Total elements must match the underlying memory. |

**Returns:** A ReadOnlyTensorSpan representing the underlying Arrow memory. / A reshaped ReadOnlyTensorSpan representing the underlying Arrow memory.

---

### AsTransposedTensorSpan

Generates a zero-copy, transposed 2D Tensor representation from a List or Array Series. By manipulating memory strides, it returns an (N x M) view of an (M x N) matrix without moving bytes.

```csharp
public ReadOnlyTensorSpan<T> AsTransposedTensorSpan<T>()
```

**Returns:** A transposed `System.Numerics.Tensors.ReadOnlyTensorSpan` pointing to the original Arrow memory.

> [!NOTE]
> This is useful when bridging with external Machine Learning libraries (like ONNX or native C++ math backends) that expect Column-Major order or specifically require a transposed matrix.

---

### BackwardFill

Fill null values with a specific strategy (Backward).

```csharp
public Series BackwardFill(uint limit = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `limit` | `uint` | Max number of consecutive nulls to fill. (Default null = infinite) |

---

### BitwiseAnd

Perform an aggregation of bitwise ANDs.

Perform an aggregation of bitwise ANDs across all elements.

```csharp
public Series BitwiseAnd()
public T? BitwiseAnd<T>()
```

---

### BitwiseCountOnes

Evaluate the number of set bits.

```csharp
public Series BitwiseCountOnes()
```

---

### BitwiseCountZeros

Evaluate the number of unset bits.

```csharp
public Series BitwiseCountZeros()
```

---

### BitwiseLeadingOnes

Evaluate the number most-significant set bits before seeing an unset bit.

```csharp
public Series BitwiseLeadingOnes()
```

---

### BitwiseLeadingZeros

Evaluate the number most-significant unset bits before seeing a set bit.

```csharp
public Series BitwiseLeadingZeros()
```

---

### BitwiseOr

Perform an aggregation of bitwise Ors.

Perform an aggregation of bitwise ORs across all elements.

```csharp
public Series BitwiseOr()
public T? BitwiseOr<T>()
```

---

### BitwiseTrailingOnes

Evaluate the number least-significant set bits before seeing an unset bit.

```csharp
public Series BitwiseTrailingOnes()
```

---

### BitwiseTrailingZeros

Evaluate the number least-significant unset bits before seeing a set bit.

```csharp
public Series BitwiseTrailingZeros()
```

---

### BitwiseXor

Perform an aggregation of bitwise Xors.

Perform an aggregation of bitwise XORs across all elements.

```csharp
public Series BitwiseXor()
public T? BitwiseXor<T>()
```

---

### BottomK

Get the bottom k values.

```csharp
public Series BottomK(int k)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `k` | `int` | - |

---

### BottomKBy

Get the bottom `k` rows according to the sorting criteria defined by `by`.

Get the bottom k values sorted by another Series.

```csharp
public Series BottomKBy(int k, Expr[] by, bool[] reverse)
public Series BottomKBy(int k, Expr by, bool reverse = false)
public Series BottomKBy(int k, Series by, bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `k` | `int` | Number of rows to return. |
| `by` | `Expr[] / Series` | The expressions (columns) to sort by. |
| `reverse` | `bool[]` | Controls the sorting direction for each expression in `by`. <br>For BottomK: - `false` (default): Sorts ascending (picks smallest values). - `true`: Sorts descending (picks largest values, acting like TopK for this column).<br> Length must match `by`. |

**Returns:** A new `Series` containing the bottom k elements.

---

### Cast

Cast Series to another DataType.

Cast the series to a specific .NET type.

```csharp
public Series Cast(DataType dtype, bool strict = true, bool wrapNumerical = false)
public Series Cast<T>(bool strict = true, bool wrapNumerical = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dtype` | `DataType` | The target type (can be Polars DataType or .NET Type) |
| `strict` | `bool` | Throws an error if conversion had overflows. |
| `wrapNumerical` | `bool` | Allows wrapping numerical overflow. |

**Returns:** A new Series of the target type.

---

### Cbrt

Calculate the cube root of the expression.

```csharp
public Series Cbrt()
```

---

### Ceil

Rounds up to the nearest integer.

```csharp
public Series Ceil()
```

---

### ChunkLengths

Get the length of each individual chunk.

```csharp
public long[] ChunkLengths()
```

---

### Clear

Create an empty copy of the current Series, with zero to ‘n’ elements. The copy has an identical name/dtype, but no data.

```csharp
public Series Clear(uint n = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `uint` | - |

---

### Clip

Set values outside the given boundaries to the boundary value.

```csharp
public Series Clip(IntoExprColumn? lowerBound = null, IntoExprColumn? upperBound = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `lowerBound` | `IntoExprColumn?` | Lower bound. Accepts expression input. Non-expression inputs are parsed as literals. Strings are parsed as column names. |
| `upperBound` | `IntoExprColumn?` | Upper bound. Accepts expression input. Non-expression inputs are parsed as literals. Strings are parsed as column names. |

---

### Clone

Clone the Series

```csharp
public Series Clone()
```

---

### Cos

Compute the element-wise cosine.

```csharp
public Series Cos()
```

---

### Cosh

Compute the element-wise hyperbolic cosine.

```csharp
public Series Cosh()
```

---

### Cot

Compute the element-wise value for the cotangent.

```csharp
public Series Cot()
```

---

### Count

Return the number of non-null elements in the column.

```csharp
public long Count()
```

**Returns:** A new `Series` containing the count of non-null values.

---

### CumCount

Get an array with the cumulative count computed at every element.

```csharp
public Series CumCount(bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `reverse` | `bool` | Reverse the operation. |

**Returns:** A new `Series` with the cumulative count.

---

### CumMax

Get an array with the cumulative max computed at every element.

```csharp
public Series CumMax(bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `reverse` | `bool` | Reverse the operation. |

**Returns:** A new `Series` with the cumulative maximum.

---

### CumMin

Get an array with the cumulative min computed at every element.

```csharp
public Series CumMin(bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `reverse` | `bool` | Reverse the operation. |

**Returns:** A new `Series` with the cumulative minimum.

---

### CumProd

Get an array with the cumulative prod computed at every element.

```csharp
public Series CumProd(bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `reverse` | `bool` | Reverse the operation. |

**Returns:** A new `Series` with the cumulative product.

---

### CumSum

Get an array with the cumulative sum computed at every element.

```csharp
public Series CumSum(bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `reverse` | `bool` | Reverse the operation. |

**Returns:** A new `Series` with the cumulative sum.

---

### CumulativeEval

Run an expression over a sliding window that increases 1 slot every iteration. This can be really slow as it can have O(n^2) complexity. Don’t use this for operations that visit all elements.

```csharp
public Series CumulativeEval(Expr expr, int minSamples = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | Expression to evaluate |
| `minSamples` | `int` | Number of valid values there should be in the window before the expression is evaluated. valid values = length - null_count |

---

### Cut

Bin continuous values into discrete categories.

```csharp
public Series Cut(
    ReadOnlySpan<double> breaks,
    string[]? labels = null,
    bool leftClosed = false,
    bool includeBreaks = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `breaks` | `ReadOnlySpan<double>` | List of unique cut points. |
| `labels` | `string[]?` | Names of the categories. The number of labels must be equal to the number of cut points plus one. |
| `leftClosed` | `bool` | Set the intervals to be left-closed instead of right-closed. |
| `includeBreaks` | `bool` | Include a column with the right endpoint of the bin each observation falls in. This will change the data type of the output from an Enum to a Struct. |

**Returns:** Expression/Series of data type Enum if include_breaks is set to False (default), otherwise an expression of data type Struct.

---

### Describe

Generate a summary statistics DataFrame (count, mean, std, min, 25%, 50%, 75%, max). Similar to pandas/polars describe().

```csharp
public DataFrame Describe()
```

---

### Diff

Calculate the difference with the previous value (n-th lag).

```csharp
public Series Diff(long n = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `long` | - |

---

### Dot

Compute the dot/inner product between two Series. 

The behavior is equivalent to `(this * other).Sum()`.

Compute the dot/inner product and return the scalar value directly.

```csharp
public Series Dot(Series other)
public T Dot<T>(Series other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Series` | The other Series to compute the dot product with. |

**Returns:** A Series of length 1 containing the result. / The dot product value.

---

### DropNans

Drop Nan Values

```csharp
public Series DropNans()
```

---

### DropNulls

Drop Null Values

```csharp
public Series DropNulls()
```

---

### Entropy

Computes the entropy. Uses the formula -sum(pk * log(pk)) where pk are discrete probabilities.

```csharp
public double? Entropy(double baseVal = 2.718281828459045, bool normalize = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `baseVal` | `double` | Given base, defaults to e |
| `normalize` | `bool` | Normalize pk if it doesn’t sum to 1. |

---

### Eq

Compare whether two Series is equal

```csharp
public Series Eq(Series other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Series` | - |

---

### EqMissing

Compare whether two Series is equal,This differs from the standard eq where null values are propagated.

```csharp
public Series EqMissing(Series other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Series` | - |

---

### EstimatedSize

Return an estimation of the total (heap) allocated size of the Series/DataFrame. Estimated size is given in the specified unit (bytes by default).

```csharp
public double EstimatedSize(SizeUnit unit = SizeUnit.Bytes)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `unit` | `SizeUnit` | Scale the returned size to the given unit (uses 1024 base). |

**Returns:** The estimated size as a double.

---

### EwmMean

Compute exponentially-weighted moving average.

```csharp
public Series EwmMean(
    double alpha,
    bool adjust = true,
    bool bias = true,
    int minPeriods = 1,
    bool ignoreNulls = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `alpha` | `double` | Specify smoothing factor alpha directly. <br>Constraint: `0 < alpha <= 1` |
| `adjust` | `bool` | If `true`, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). If `false`, assume infinite history. |
| `bias` | `bool` | If `true`, use a biased estimator (Standard deviation uses `N` in denominator). If `false`, use an unbiased estimator (Standard deviation uses `N-1`). <br>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true. |
| `minPeriods` | `int` | Minimum number of observations in window required to have a value (otherwise result is null). |
| `ignoreNulls` | `bool` | Ignore missing values when calculating weights. |

**Returns:** A new `Series` with the EWM mean.

---

### EwmMeanBy

Compute exponentially-weighted moving average based on a temporal or index column.

```csharp
public Series EwmMeanBy(Expr by, string halfLife)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `Expr` | The column used to determine the distance between observations. <br>Supported data types: `Date`, `DateTime`, `UInt64`, `UInt32`, `Int64`, or `Int32`. |
| `halfLife` | `string` | The unit over which an observation decays to half its value. <br>Supported string formats:<br><br>- Time units`ns` (nanosecond), `us` (microsecond), `ms` (millisecond), `s` (second), `m` (minute), `h` (hour), `d` (day), `w` (week). - Index units`i` (index count). Example: `"2i"` means decay by half every 2 index steps. - CompoundExample: `"3d12h4m25s"`.<br><br>Warning:`halfLife` is treated as a constant duration. Calendar durations such as months (`mo`) or years (`y`) are NOT supported because they vary in length. Please express such durations in hours (e.g. use `'730h'` instead of `'1mo'`). |

**Returns:** A new `Series` with the time/index-based EWM mean.

---

### EwmStd

Compute exponentially-weighted moving standard deviation.

```csharp
public Series EwmStd(
    double alpha,
    bool adjust = true,
    bool bias = true,
    int minPeriods = 1,
    bool ignoreNulls = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `alpha` | `double` | Specify smoothing factor alpha directly. <br>Constraint: `0 < alpha <= 1` |
| `adjust` | `bool` | If `true`, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). If `false`, assume infinite history. |
| `bias` | `bool` | If `true`, use a biased estimator (Standard deviation uses `N` in denominator). If `false`, use an unbiased estimator (Standard deviation uses `N-1`). <br>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true. |
| `minPeriods` | `int` | Minimum number of observations in window required to have a value (otherwise result is null). |
| `ignoreNulls` | `bool` | Ignore missing values when calculating weights. |

**Returns:** A new `Series` with the EWM standard deviation.

---

### EwmSum

Compute exponentially-weighted moving sum.

```csharp
public Series EwmSum(
    double alpha,
    bool adjust = true,
    bool bias = true,
    int minPeriods = 1,
    bool ignoreNulls = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `alpha` | `double` | Specify smoothing factor alpha directly. <br>Constraint: `0 < alpha <= 1` |
| `adjust` | `bool` | If `true`, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). If `false`, assume infinite history. |
| `bias` | `bool` | If `true`, use a biased estimator (Standard deviation uses `N` in denominator). If `false`, use an unbiased estimator (Standard deviation uses `N-1`). <br>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true. |
| `minPeriods` | `int` | Minimum number of observations in window required to have a value (otherwise result is null). |
| `ignoreNulls` | `bool` | Ignore missing values when calculating weights. |

**Returns:** A new `Series` with the EWM sum.

---

### EwmSumBy

Compute time-based exponentially-weighted moving sum.

```csharp
public Series EwmSumBy(Expr by, string halfLife)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `Expr` | Column to use as reference for the time decay. |
| `halfLife` | `string` | Half-life of the exponential decay. |

**Returns:** A new `Series` with the time/index-based EWM sum.

---

### EwmVar

Compute exponentially-weighted moving variance.

```csharp
public Series EwmVar(
    double alpha,
    bool adjust = true,
    bool bias = true,
    int minPeriods = 1,
    bool ignoreNulls = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `alpha` | `double` | Specify smoothing factor alpha directly. <br>Constraint: `0 < alpha <= 1` |
| `adjust` | `bool` | If `true`, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). If `false`, assume infinite history. |
| `bias` | `bool` | If `true`, use a biased estimator (Standard deviation uses `N` in denominator). If `false`, use an unbiased estimator (Standard deviation uses `N-1`). <br>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true. |
| `minPeriods` | `int` | Minimum number of observations in window required to have a value (otherwise result is null). |
| `ignoreNulls` | `bool` | Ignore missing values when calculating weights. |

**Returns:** A new `Series` with the EWM variance.

---

### Exp

Calculate the power of the Euler's number.

```csharp
public Series Exp()
```

---

### Explode

Explode a list column into multiple rows. The resulting Series will be longer than the original.

```csharp
public Series Explode(bool emptyAsNull = true, bool keepNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `emptyAsNull` | `bool` | - |
| `keepNulls` | `bool` | - |

---

### Extend

Extend the memory backed by this Series with the values from another. Different from append, which adds the chunks from other to the chunks of this series, extend appends the data from other to the underlying memory locations and thus may cause a reallocation (which is expensive). If this does not cause a reallocation, the resulting data structure will not have any extra chunks and thus will yield faster queries.

```csharp
public Series Extend(Series other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Series` | Series to extend the series with. |

---

### ExtendConstant

Extremely fast method for extending the Series with ‘n’ copies of a value.

```csharp
public Series ExtendConstant(IntoExpr value, Expr n)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `value` | `IntoExpr` | A constant literal value or a unit expression with which to extend the expression result Series; can pass None to extend with nulls. |
| `n` | `Expr` | The number of additional values that will be added. |

---

### FillNan

Fill floating point NaN values with a specified value. This is different from FillNull. It only handles IEEE 754 NaN.

```csharp
public Series FillNan(IntoExpr value)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `value` | `IntoExpr` | - |

---

### FillNull

Fill null values using the specified value

Fill null values using the specified strategy

```csharp
public Series FillNull(IntoExpr value)
public Series FillNull(FillNullStrategy strategy, uint? limit = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `value` | `IntoExpr` | Value used to fill null values. |
| `strategy` | `FillNullStrategy` | Strategy used to fill null values. |
| `limit` | `uint?` | Number of consecutive null values to fill when using the ‘forward’ or ‘backward’ strategy. |

---

### Filter

Filter a series using a boolean series.

```csharp
public Series Filter(Series predicate)
public Series Filter(IEnumerable<bool> predicate)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `predicate` | `Series / IEnumerable<bool>` | - |

---

### First

Get the first value of the group/series.

First series into scalar

```csharp
public Series First()
public T? First<T>()
```

**Returns:** A new `Series` containing the first value (length 1).

---

### Floor

Rounds down to the nearest integer.

```csharp
public Series Floor()
```

---

### ForwardFill

Fill null values with a specific strategy (Forward).

```csharp
public Series ForwardFill(uint limit = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `limit` | `uint` | Max number of consecutive nulls to fill. (Default null = infinite) |

---

### FromEpoch

Parses an integer Series into a Datetime or Date Series.

```csharp
public Series FromEpoch(EpochTimeUnit timeUnit = EpochTimeUnit.Second)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `timeUnit` | `EpochTimeUnit` | - |

---

### Gather

Gather values by an index expression.

Take elements by physical integer indices. Negative indices are not supported. All values must be >= 0.

```csharp
public Series Gather(IntoExprColumn indices)
public Series Gather(ReadOnlySpan<int> indices)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `indices` | `IntoExprColumn / ReadOnlySpan<int>` | - |

---

### GatherEvery

Take every nth value starting from an offset.

```csharp
public Series GatherEvery(ulong n, ulong offset = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `ulong` | - |
| `offset` | `ulong` | - |

---

### Get

Get a single value by index. Returns a scalar.

```csharp
public Series Get(Expr index, bool nullOnOutOfBounds = false)
public Series Get(ulong index, bool nullOnOutOfBounds = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `Expr / ulong` | The index expression. |
| `nullOnOutOfBounds` | `bool` | If true, returns Null when the index is out of bounds instead of raising an error. |

---

### GetValue

Get an item at the specified index.

```csharp
public T GetValue<T>(long index, bool uncheck = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `long` | - |
| `uncheck` | `bool` | - |

---

### Gt

Compare whether left series is greater than right series

```csharp
public Series Gt(Series other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Series` | - |

---

### GtEq

Compare whether left series is greater than or equal to right series

```csharp
public Series GtEq(Series other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Series` | - |

---

### HasNulls

Check whether the Series contains one or more null values.

```csharp
public bool HasNulls()
```

---

### Hash

Hash the elements in the selection.The hash value is of type UInt64.

```csharp
public Series Hash(
    ulong seed = 0,
    ulong? seed1 = null,
    ulong? seed2 = null,
    ulong? seed3 = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `seed` | `ulong` | Random seed parameter. Defaults to 0. |
| `seed1` | `ulong?` | Random seed parameter. Defaults to seed if not set. |
| `seed2` | `ulong?` | Random seed parameter. Defaults to seed if not set. |
| `seed3` | `ulong?` | Random seed parameter. Defaults to seed if not set. |

---

### Hist

Bin values into buckets and count their occurrences.

```csharp
public DataFrame Hist(
    IntoExpr? bins = null,
    int? binCount = null,
    bool includeCategory = true,
    bool includeBreakPoint = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `bins` | `IntoExpr?` | Bin edges. If None given, we determine the edges based on the data. |
| `binCount` | `int?` | If bins is not provided, binCount uniform bins are created that fully encompass the data. |
| `includeCategory` | `bool` | Include a column that indicates the upper breakpoint. |
| `includeBreakPoint` | `bool` | Include a column that shows the intervals as categories. |

---

### Implode

Aggregate values into a list. Result is a Series with 1 row containing a List of all values.

```csharp
public Series Implode()
```

---

### IndexOf

Find the index of the first occurrence of a specific value.

```csharp
public int? IndexOf(IntoExpr element)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `element` | `IntoExpr` | The element expression to search for. |

---

### Interpolate

Interpolate intermediate values.

```csharp
public Series Interpolate(InterpolationMethod method = InterpolationMethod.Linear)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `method` | `InterpolationMethod` | Interpolation method (Linear or Nearest). |

---

### InterpolateBy

Interpolate intermediate values based on the values of another Series. 

Useful for linear interpolation across unevenly spaced data.

```csharp
public Series InterpolateBy(IntoExprColumn by)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IntoExprColumn` | The Series to use for interpolation (e.g. timestamps). |

**Returns:** A new Series with interpolated values.

---

### IsBetween

Check if values are between lower and upper bounds.

```csharp
public Series IsBetween(object lower, object upper, ClosedInterval closedInterval = ClosedInterval.Both)
public Series IsBetween(Expr lower, Expr upper, ClosedInterval closedInterval = ClosedInterval.Both)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `lower` | `object / Expr` | - |
| `upper` | `object / Expr` | - |
| `closedInterval` | `ClosedInterval` | - |

---

### IsClose

Check if this expression is close, i.e. almost equal, to the other expression.

```csharp
public Series IsClose(
    IntoExprColumn other,
    double absTol = 0,
    double relTol = 1E-09,
    bool nansEqual = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExprColumn` | A literal or expression value to compare with. |
| `absTol` | `double` | Absolute tolerance. This is the maximum allowed absolute difference betweentwo values. Must be non-negative. |
| `relTol` | `double` | Relative tolerance. This is the maximum allowed difference between two values, relative to the larger absolute value. Must be non-negative. |
| `nansEqual` | `bool` | Whether NaN values should be considered equal. |

**Returns:** Expression/Series of data type Boolean.

---

### IsDuplicated

Get a boolean mask indicating which values are duplicated. 

Implemented via DataFrame expression composition.

```csharp
public Series IsDuplicated()
```

---

### IsEmpty

True if the Series is empty.

```csharp
public bool IsEmpty(bool ignoreNulls = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | - |

---

### IsFinite

Check whether this series is finite

```csharp
public Series IsFinite()
```

---

### IsFirstDistinct

Return a boolean mask indicating the first occurrence of each distinct value.

```csharp
public Series IsFirstDistinct()
```

---

### IsIn

Check if elements of this Series are in the other Series.

Check if elements of this Series are in the collections.

```csharp
public Series IsIn(Series other, bool nullsEqual = false)
public Series IsIn<T>(IEnumerable<T> collection, bool nullsEqual = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Series` | - |
| `nullsEqual` | `bool` | - |
| `collection` | `IEnumerable<T>` | - |

---

### IsInfinite

Check whether this series is infinite

```csharp
public Series IsInfinite()
```

---

### IsLastDistinct

Return a boolean mask indicating the last occurrence of each distinct value.

```csharp
public Series IsLastDistinct()
```

---

### IsNan

Check whether this series is NaN

```csharp
public Series IsNan()
```

---

### IsNotNan

Check whether this series is not NaN

```csharp
public Series IsNotNan()
```

---

### IsNotNull

Return a Boolean series, where null value will be masked as false.

```csharp
public Series IsNotNull()
```

---

### IsNull

Return a Boolean series, where null value will be masked as true.

```csharp
public Series IsNull()
```

---

### IsNullAt

Check whether indexed value is null。

```csharp
public bool IsNullAt(long index, bool uncheck = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `long` | - |
| `uncheck` | `bool` | - |

---

### IsSorted

Checks if the Series is sorted according to the given rules. If the metadata flag matches, returns O(1). Otherwise, scans the data O(N).

```csharp
public bool IsSorted(bool descending = false, bool nullsLast = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `descending` | `bool` | - |
| `nullsLast` | `bool` | - |

---

### IsUnique

Get a boolean mask indicating which values are unique. 

Implemented via DataFrame expression composition.

```csharp
public Series IsUnique()
```

---

### Kurtosis

Compute the kurtosis (Fisher or Pearson) of a dataset.

```csharp
public double? Kurtosis(bool fisher = true, bool bias = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fisher` | `bool` | If True, Fisher’s definition is used (normal ==> 0.0). If False, Pearson’s definition is used (normal ==> 3.0). |
| `bias` | `bool` | If False, the calculations are corrected for statistical bias. |

**Returns:** A series which length is 1

---

### Last

Get the last value of the group/series.

Last series into scalar

```csharp
public Series Last()
public T? Last<T>()
```

**Returns:** A new `Series` containing the last value (length 1).

---

### Len

Return the length of the Series.

```csharp
public long Len()
```

---

### Ln

```csharp
public Series Ln()
```

---

### Log

Compute the logarithm to a given base,defaults to e.

```csharp
public Series Log(Expr baseVal)
public Series Log()
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `baseVal` | `Expr` | Given base, defaults to e |

---

### Log10

Compute the base 10 logarithm of the input array, element-wise.

```csharp
public Series Log10()
```

---

### Log1p

Compute the natural logarithm of each element plus one.This computes log(1 + x) but is more numerically stable for x close to zero.

```csharp
public Series Log1p()
```

---

### LowerBound

Return the lower bound of this Series’ dtype as a unit Series.

```csharp
public Series LowerBound()
```

---

### Lt

Compare whether left series is less than right series

```csharp
public Series Lt(Series other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Series` | - |

---

### LtEq

Compare whether left series is less than or equal to right series

```csharp
public Series LtEq(Series other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Series` | - |

---

### Map

Apply a custom C# function to the series (element-wise). 

Warning: This is slower than native expressions because it runs in the .NET runtime.

Apply a raw Arrow-to-Arrow UDF.

```csharp
public Series Map<TInput, TOutput>(Func<TInput, TOutput> function)
public Series Map(Func<IArrowArray, IArrowArray> function, DataType outputType)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `function` | `Func<TInput, TOutput> / Func<IArrowArray, IArrowArray>` | - |
| `outputType` | `DataType` | - |

---

### Max

Max series into 1 length series(Scalar)

Max series into scalar

```csharp
public Series Max()
public T? Max<T>()
```

---

### MaxBy

Get the maximum value in this Series, ordered by an expression.

```csharp
public Series MaxBy(IntoExprColumn by)
public T? MaxBy<T>(IntoExprColumn by)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IntoExprColumn` | Column used to determine the largest element. Accepts expression input. |

---

### Mean

Mean series into 1 length series(Scalar)

Mean series into scalar

```csharp
public Series Mean()
public T? Mean<T>()
```

---

### Median

Get the median value.

```csharp
public T? Median<T>()
```

---

### Min

Min series into 1 length series(Scalar)

Min series into scalar

```csharp
public Series Min()
public T? Min<T>()
```

---

### MinBy

Get the minimum value in this Series, ordered by an expression.

```csharp
public Series MinBy(IntoExprColumn by)
public T? MinBy<T>(IntoExprColumn by)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IntoExprColumn` | Column used to determine the largest element. Accepts expression input. |

---

### Mode

Compute the most occurring value(s).Can return multiple Values.

```csharp
public Series Mode(bool maintainOrder = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `maintainOrder` | `bool` | Maintain order of data. This requires more work. |

---

### NUnique

Count the number of unique values in this Series.

```csharp
public long NUnique()
```

---

### NanMax

Get maximum value, but propagate/poison encountered NaN values.

```csharp
public T? NanMax<T>()
```

---

### NanMaxString

Get maximum value, but propagate/poison encountered NaN values.

```csharp
public string NanMaxString()
```

---

### NanMin

Get minimum value, but propagate/poison encountered NaN values.

```csharp
public T? NanMin<T>()
```

---

### NanMinString

Get minimum value, but propagate/poison encountered NaN values.

```csharp
public string NanMinString()
```

---

### Neq

Compare whether two Series is not equal

```csharp
public Series Neq(Series other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Series` | - |

---

### NeqMissing

Compare whether two Series is not equal,This differs from the standard neq where null values are propagated.

```csharp
public Series NeqMissing(Series other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Series` | - |

---

### NewFromIndex

Create a new Series filled with values from the given index.

```csharp
public Series NewFromIndex(long index, long length)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `long` | - |
| `length` | `long` | - |

---

### Not

Bitwise NOT operator

```csharp
public Series Not()
```

---

### PctChange

Computes percentage change between values. Percentage change (as fraction) between current element and most-recent non-null element at least n period(s) before the current element. Computes the change from the previous row by default.

```csharp
public Series PctChange(int n = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | periods to shift for forming percent change. |

**Returns:** A new `Series` with the percentage change.

---

### PeakMax

Get a boolean mask of the local maximum peaks.

```csharp
public Series PeakMax()
```

---

### PeakMin

Get a boolean mask of the local minimum peaks.

```csharp
public Series PeakMin()
```

---

### Pow

Calculate exponent value.

Calculate the power of the expression with a given numeric exponent.

Calculate the power of the expression with a given exponent expression.

```csharp
public Series Pow(double exponent)
public Series Pow(int exponent)
public Series Pow(long exponent)
public Series Pow(float exponent)
public Series Pow(Series exponent)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exponent` | `double / int / long / float / Series` | - |

---

### Product

Product series into 1 length series(Scalar)

Product series into scalar

```csharp
public Series Product()
public T? Product<T>()
```

---

### QCut

Bin continuous values into discrete categories based on their quantiles.

```csharp
public Series QCut(
    ReadOnlySpan<double> quantiles,
    string[]? labels = null,
    bool leftClosed = false,
    bool allowDuplicates = false,
    bool includeBreaks = false
)

public Series QCut(
    int quantiles,
    string[]? labels = null,
    bool leftClosed = false,
    bool allowDuplicates = false,
    bool includeBreaks = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `quantiles` | `ReadOnlySpan<double> / int` | Either a list of quantile probabilities between 0 and 1 or a positive integer determining the number of bins with uniform probability. |
| `labels` | `string[]?` | Names of the categories. The number of labels must be equal to the number of categories. |
| `leftClosed` | `bool` | Set the intervals to be left-closed instead of right-closed. |
| `allowDuplicates` | `bool` | If set to True, duplicates in the resulting quantiles are dropped, rather than raising a DuplicateError. This can happen even with unique probabilities, depending on the data. |
| `includeBreaks` | `bool` | Include a column with the right endpoint of the bin each observation falls in. This will change the data type of the output from a Categorical to a Struct. |

**Returns:** Expression/Series of data type Categorical if include_breaks is set to False (default), otherwise an expression of data type Struct.

---

### Quantile

Get the quantile value.

```csharp
public Series Quantile(double quantile, QuantileMethod method = QuantileMethod.Linear)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `quantile` | `double` | Quantile between 0.0 and 1.0. |
| `method` | `QuantileMethod` | ['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method. |

**Returns:** A new `Series` containing the quantile value (length 1).

---

### Rank

Assign ranks to data, dealing with ties appropriately.

```csharp
public Series Rank(RankMethod method = RankMethod.Average, bool descending = false, ulong? seed = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `method` | `RankMethod` | The method used to assign ranks to tied elements. See `RankMethod` for details. Default is `RankMethod.Average`. |
| `descending` | `bool` | Rank in descending order. |
| `seed` | `ulong?` | If method="random", use this as seed. |

**Returns:** A new `Series` with the ranks.

---

### Rechunk

Reallocates the Series to ensure that all its underlying memory is physically contiguous.

```csharp
public Series Rechunk()
```

**Returns:** A new Series instance backed by contiguous memory.

> [!NOTE]
> Polars Operations like Appending or Filtering can create fragmented memory chunks. Calling Rechunk() merges these chunks into a single contiguous Arrow array. This is CRITICAL before zero-copy extracting native pointers for Tensors or FFI.

---

### Reinterpret

Reinterpret the underlying bits as a signed/unsigned integer. This operation is only allowed for numeric types of the same size. For lower bits numbers, you can safely use the cast operation.

Reinterpret the underlying bits as a signed/unsigned integer or float.

```csharp
public Series Reinterpret(bool signed = true)
public Series Reinterpret(DataType dtype)
public Series Reinterpret<T>()
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `signed` | `bool` | If True, reinterpret as signed integer. Otherwise, reinterpret as unsigned integer. |
| `dtype` | `DataType` | DataType to reinterpret to. |

---

### Rename

Rename Series

```csharp
public Series Rename(string newName)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `newName` | `string` | - |

---

### RepeatBy

Repeat the elements in this Series as specified in the given expression. The repeated elements are expanded into a List.

```csharp
public Series RepeatBy(IntoExpr by)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IntoExpr` | Numeric column that determines how often the values will be repeated. The column will be coerced to UInt32. Give this dtype to make the coercion a no-op. |

**Returns:** Expression/Series of data type List, where the inner data type is equal to the original data type.

---

### Replace

Replace the given values by different values of the same data type.

Replace values using a mapping (Dictionary/IEnumerable of KeyValuePair).

Replace values strictly using two sequences (IEnumerable).

```csharp
public Series Replace(IntoExpr old, IntoExpr newExpr)
public Series Replace<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> mapping)
public Series Replace<TOld, TNew>(IEnumerable<TOld> oldValues, IEnumerable<TNew> newValues)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `old` | `IntoExpr` | Value or sequence of values to replace. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals. |
| `newExpr` | `IntoExpr` | Value or sequence of values to replace by. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals. |
| `mapping` | `IEnumerable<KeyValuePair<TKey, TValue>>` | - |
| `oldValues` | `IEnumerable<TOld>` | - |
| `newValues` | `IEnumerable<TNew>` | - |

---

### ReplaceStrict

Replace all values by different values.

Replace values strictly using a mapping.

Replace values strictly using two sequences (IEnumerable).

```csharp
public Series ReplaceStrict(
    IntoExpr old,
    IntoExpr newExpr,
    IntoExpr? defaultExpr = null,
    IntoDataTypeExpr? returnDataType = null
)

public Series ReplaceStrict<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> mapping, IntoExpr? defaultExpr = null, IntoDataTypeExpr? returnDataType = null)
public Series ReplaceStrict<TOld, TNew>(
    IEnumerable<TOld> oldValues,
    IEnumerable<TNew> newValues,
    IntoExpr? defaultExpr = null,
    IntoDataTypeExpr? returnDataType = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `old` | `IntoExpr` | Value or sequence of values to replace. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals. |
| `newExpr` | `IntoExpr` | Value or sequence of values to replace by. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals. Length must match the length of old or have length 1. |
| `defaultExpr` | `IntoExpr?` | Set values that were not replaced to this value. If no default is specified, (default), an error is raised if any values were not replaced. Accepts expression input. Non-expression inputs are parsed as literals. |
| `returnDataType` | `IntoDataTypeExpr?` | The data type of the resulting expression. If set to null (default), the data type is determined automatically based on the other inputs. |
| `mapping` | `IEnumerable<KeyValuePair<TKey, TValue>>` | - |
| `oldValues` | `IEnumerable<TOld>` | - |
| `newValues` | `IEnumerable<TNew>` | - |

---

### Reshape

Reshape this Series to a flat Series or an Array Series.

```csharp
public Series Reshape(ReadOnlySpan<long> dimensions)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dimensions` | `ReadOnlySpan<long>` | Tuple of the dimension sizes. If a -1 is used in any of the dimensions, that dimension is inferred. |

**Returns:** Tuple of the dimension sizes. If a -1 is used in any of the dimensions, that dimension is inferred.

---

### Reverse

Reverse the selection. 

This is useful in a GroupBy context to reverse the order of the group.

```csharp
public Series Reverse()
```

**Returns:** A new `Series` with the order reversed.

---

### Rle

Compress the column data using run-length encoding. Run-length encoding (RLE) encodes data by storing each run of identical values as a single value and its length.

```csharp
public Series Rle()
```

**Returns:** Expression/Series of data type Struct with fields len of data type UInt32 and value of the original data type.

---

### RleId

Get a distinct integer ID for each run of identical values. The ID starts at 0 and increases by one each time the value of the column changes.

```csharp
public Series RleId()
```

**Returns:** Expression/Series of data type UInt32.

---

### RollingKurtosis

Apply a rolling kurtosis (moving kurtosis) over a window.

```csharp
public Series RollingKurtosis(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false,
    bool fisher = true,
    bool bias = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |
| `fisher` | `bool` | - |
| `bias` | `bool` | - |

**Returns:** A new `Series` with the rolling kurtosis.

---

### RollingMax

Apply a rolling max (moving max) over a window.

```csharp
public Series RollingMax(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new `Series` with the rolling maximum.

---

### RollingMaxBy

Apply the rolling maximum over a dynamic window defined by the values in the `by` column.

```csharp
public Series RollingMaxBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new `Series` with the dynamic rolling maximum.

---

### RollingMean

Apply a rolling mean (moving average) over a window.

```csharp
public Series RollingMean(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new `Series` with the rolling mean.

---

### RollingMeanBy

Apply a rolling mean (moving average) over a dynamic window defined by the values in the `by` column. 

Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).

```csharp
public Series RollingMeanBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new `Series` with the dynamic rolling mean.

---

### RollingMedian

Apply a rolling median (moving median) over a window.

```csharp
public Series RollingMedian(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new `Series` with the rolling median.

---

### RollingMedianBy

Apply the rolling median over a dynamic window defined by the values in the `by` column.

```csharp
public Series RollingMedianBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new `Series` with the dynamic rolling median.

---

### RollingMin

Apply a rolling min (moving min) over a window.

```csharp
public Series RollingMin(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new `Series` with the rolling minimum.

---

### RollingMinBy

Apply the rolling minimum over a dynamic window defined by the values in the `by` column.

```csharp
public Series RollingMinBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new `Series` with the dynamic rolling minimum.

---

### RollingQuantile

Apply a rolling quantile over a fixed window.

```csharp
public Series RollingQuantile(
    double quantile,
    QuantileMethod method,
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `quantile` | `double` | Quantile between 0.0 and 1.0 (e.g., 0.5 for median). |
| `method` | `QuantileMethod` | Interpolation method when the quantile lies between two data points. |
| `windowSize` | `IntoDuration` | The size of the window. <br>Format: `"3i"` (3 rows) or just a number string `"3"`.<br><br>For time-based windows (e.g. "2h"), use `Expr.RollingQuantileBy` instead. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights for the window. The length must match the parsed window size. <br>If `null`, equal weights are used. |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new `Series` with the rolling quantile.

---

### RollingQuantileBy

Compute the rolling quantile over a dynamic window defined by the values in the `by` column.

```csharp
public Series RollingQuantileBy(
    double quantile,
    QuantileMethod method,
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `quantile` | `double` | Quantile between 0.0 and 1.0 (e.g., 0.5 for median). |
| `method` | `QuantileMethod` | Interpolation method when the quantile lies between two data points. |
| `windowSize` | `IntoDuration` | The size of the time window as a `System.TimeSpan`. <br>This will be automatically converted to a Polars duration string (e.g., `01:30:00` -> `"1h30m"`). |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new `Series` with the dynamic rolling quantile.

---

### RollingRank

Apply a rolling rank (moving rank) over a window.

```csharp
public Series RollingRank(
    IntoDuration windowSize,
    int minPeriods = 1,
    RankMethod method = RankMethod.Average,
    ulong? seed = null,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `method` | `RankMethod` | The method used to assign ranks to tied elements. See `RankMethod` for details. Default is `RankMethod.Average`. |
| `seed` | `ulong?` | If method="random", use this as seed. |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new `Series` with the rolling rank.

---

### RollingRankBy

Compute the rolling rank over a dynamic window defined by the values in the `by` column.

```csharp
public Series RollingRankBy(
    IntoDuration windowSize,
    Expr by,
    RollingRankMethod method = RollingRankMethod.Average,
    ulong? seed = null,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `method` | `RollingRankMethod` | The method used to assign ranks to tied elements. |
| `seed` | `ulong?` | Seed for the random method (only relevant when method is Random). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new `Series` with the dynamic rolling rank.

---

### RollingSkew

Apply a rolling skew (moving skew) over a window.

```csharp
public Series RollingSkew(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false,
    bool bias = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |
| `bias` | `bool` | - |

**Returns:** A new `Series` with the rolling skew.

---

### RollingStd

Apply a rolling standard deviation (moving std) over a window.

```csharp
public Series RollingStd(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new `Series` with the rolling standard deviation.

---

### RollingStdBy

Apply the rolling standard deviation over a dynamic window defined by the values in the `by` column.

```csharp
public Series RollingStdBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new `Series` with the dynamic rolling standard deviation.

---

### RollingSum

Apply a rolling sum (moving sum) over a window.

```csharp
public Series RollingSum(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new `Series` with the rolling sum.

---

### RollingSumBy

Apply a rolling sum over a dynamic window defined by the values in the `by` column.

```csharp
public Series RollingSumBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new `Series` with the dynamic rolling sum.

---

### RollingVar

Apply a rolling variance (moving var) over a window.

```csharp
public Series RollingVar(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false,
    byte ddof = 1
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. <br>By default ddof is 1. |

**Returns:** A new `Series` with the rolling variance.

---

### RollingVarBy

Apply the rolling variance over a dynamic window defined by the values in the `by` column.

```csharp
public Series RollingVarBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left,
    byte ddof = 1
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. <br>By default ddof is 1. |

**Returns:** A new `Series` with the dynamic rolling variance.

---

### Round

Round underlying floating point data by decimals digits.

```csharp
public Series Round(uint decimals = 0, RoundMode mode = RoundMode.HalfToEven)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `decimals` | `uint` | Number of decimals to round by. |
| `mode` | `RoundMode` | The rounding strategy used. A “rounded value” is a value with at most decimals decimal places (e.g. integers when decimals=0, multiples of 0.1 when decimals=1, 0.01 when decimals=2, and so on). Strategies that start with half_ round all values to the nearest rounded value, only using the strategy to break ties when a value falls exactly between two rounded values (e.g. 0.5 when decimals=0, 0.05 when decimals=1). Other rounding strategies specify explicitly which rounded value is chosen and always apply (not just for tiebreaks). |

---

### RoundSigFigs

Round to a number of significant figures.

```csharp
public Series RoundSigFigs(int digits)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `digits` | `int` | Number of significant figures to round to. |

---

### Sample

Sample from this expression.

```csharp
public Series Sample(
    ulong n = 1,
    bool withReplacement = false,
    bool shuffle = false,
    ulong? seed = null
)

public Series Sample(
    double fraction,
    bool withReplacement = false,
    bool shuffle = false,
    ulong? seed = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `ulong` | Number of items to return. Default to 1 |
| `withReplacement` | `bool` | Allow values to be sampled more than once. |
| `shuffle` | `bool` | Shuffle the order of sampled data points. |
| `seed` | `ulong?` | Seed for the random number generator. If set to None (default), a random seed is generated for each sample operation. |
| `fraction` | `double` | Fraction of items to return. |

---

### SearchSorted

Find indices where elements should be inserted to maintain order (Binary Search).

```csharp
public Series SearchSorted(IntoExpr? element, SearchSortedSide side = SearchSortedSide.Any, bool descending = false)
public Series SearchSorted<T>(IEnumerable<T> element, SearchSortedSide side = SearchSortedSide.Any, bool descending = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `element` | `IntoExpr? / IEnumerable<T>` | The element expression to insert/search. |
| `side` | `SearchSortedSide` | The insertion side (Any, Left, Right). Default is Any. |
| `descending` | `bool` | Whether the target column is sorted in descending order. Default is false. |

---

### SearchSortedIndex

Searches for a single scalar value and returns its insertion index.

```csharp
public int SearchSortedIndex(object? scalar, SearchSortedSide side = SearchSortedSide.Any, bool descending = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `scalar` | `object?` | - |
| `side` | `SearchSortedSide` | - |
| `descending` | `bool` | - |

---

### SetSorted

Flags the Series as ‘sorted’. Enables downstream code to user fast paths for sorted arrays.

```csharp
public Series SetSorted(bool descending = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `descending` | `bool` | If the Series order is descending. |

---

### Shift

Shift values by the given number of indices.

```csharp
public Series Shift(long n = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `long` | - |

---

### Show

Print the DataFrame to Console.

```csharp
public void Show()
```

---

### ShrinkToFit

Shrink Series memory usage.

```csharp
public Series ShrinkToFit()
```

**Returns:** A new Series

---

### ShrinkToFitInplace

Shrink Series memory usage.This won't return a new Series

```csharp
public void ShrinkToFitInplace()
```

---

### Sign

Compute the element-wise sign (-1, 0, 1).

```csharp
public Series Sign()
```

---

### Sin

Compute the element-wise sine.

```csharp
public Series Sin()
```

---

### Sinh

Compute the element-wise hyperbolic sine.

```csharp
public Series Sinh()
```

---

### Skew

Compute the sample skewness of a data set.

```csharp
public double? Skew(bool bias = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `bias` | `bool` | If False, the calculations are corrected for statistical bias. |

**Returns:** A series which length is 1

---

### Slice

Get a slice of this Series.

```csharp
public Series Slice(long offset, ulong? length = null)
public Series Slice(Range range)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `offset` | `long` | Start index. Negative values count from the end. |
| `length` | `ulong?` | Length of the slice. |
| `range` | `Range` | - |

---

### Sort

Sort this Series.

```csharp
public Series Sort(
    bool descending = false,
    bool nullsLast = false,
    bool maintainOrder = false,
    bool multithreaded = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `descending` | `bool` | Sort in descending order. |
| `nullsLast` | `bool` | Place null values last (default behavior depends on ascending/descending). |
| `maintainOrder` | `bool` | Use stable sort (maintain order of equal elements) (default: false). |
| `multithreaded` | `bool` | Use parallel sorting (default: true). |

---

### Sqrt

Calculate square value.

```csharp
public Series Sqrt()
```

---

### Std

Get the standard deviation of this Series.

```csharp
public T? Std<T>(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | Delta Degrees of Freedom. The divisor used in calculations is N - ddof. |

---

### Sum

Sum series into 1 length series(Scalar)

Sum series into scalar

```csharp
public Series Sum()
public T? Sum<T>()
```

---

### Take

LINQ-like alias for Gather.

Take elements by physical integer indices. Negative indices are not supported. All values must be >= 0.

```csharp
public Series Take(IntoExprColumn indices)
public Series Take(ReadOnlySpan<int> indices)
public Series Take(Series indices)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `indices` | `IntoExprColumn / ReadOnlySpan<int> / Series` | - |

---

### Tan

Compute the element-wise tangent.

```csharp
public Series Tan()
```

---

### Tanh

Compute the element-wise hyperbolic tangent.

```csharp
public Series Tanh()
```

---

### ToArray

Convert Series to Array

```csharp
public T[] ToArray<T>()
```

---

### ToArrow

Zero-copy convert to Apache Arrow Array.

```csharp
public IArrowArray ToArrow()
```

---

### ToDataFrame

Convert this single Series into a DataFrame. Functional alias for `Series.ToFrame`.

```csharp
public DataFrame ToDataFrame()
```

---

### ToDummies

Get dummy/indicator variables.

```csharp
public DataFrame ToDummies(string? separator = "_", bool dropFirst = false, bool dropNulls = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `separator` | `string?` | Separator/delimiter used when generating column names. |
| `dropFirst` | `bool` | Remove the first category from the variable being encoded. |
| `dropNulls` | `bool` | If there are None values in the series, a null column is not generated. Null values in the input are represented by zero vectors. |

---

### ToFrame

Convert this single Series into a DataFrame.

```csharp
public DataFrame ToFrame()
```

---

### ToPhysical

Cast to physical representation of the logical dtype.

```csharp
public Series ToPhysical()
```

---

### TopK

Get the top k values.

```csharp
public Series TopK(int k)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `k` | `int` | - |

---

### TopKBy

Get the top `k` rows according to the sorting criteria defined by `by`.

Get the top k values sorted by another Series.

```csharp
public Series TopKBy(int k, Expr[] by, bool[] reverse)
public Series TopKBy(int k, Expr by, bool reverse = false)
public Series TopKBy(int k, Series by, bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `k` | `int` | Number of rows to return. |
| `by` | `Expr[] / Series` | The expressions (columns) to sort by. |
| `reverse` | `bool[]` | Controls the sorting direction for each expression in `by`. <br>For TopK: - `false` (default): Sorts descending (picks largest values). - `true`: Sorts ascending (picks smallest values, acting like BottomK for this column).<br> Length must match `by`. |

**Returns:** A new `Series` containing the top k elements.

---

### Truncate

Truncate numeric data toward zero to decimals number of decimal places.

```csharp
public Series Truncate(uint decimals = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `decimals` | `uint` | Number of decimal places to truncate to. |

---

### Unique

Get unique values of this series.

```csharp
public Series Unique(bool maintainOrder = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `maintainOrder` | `bool` | Maintain order of data. This requires more work. |

---

### UniqueCounts

Return a count of the unique values in the order of appearance.

```csharp
public Series UniqueCounts()
```

---

### Unnest

Unnest a Struct column into a DataFrame. Shortcut for `SeriesStructOps.Unnest`.

```csharp
public DataFrame Unnest()
```

---

### UpperBound

Return the upper bound of this Series’ dtype as a unit Series.

```csharp
public Series UpperBound()
```

---

### ValueCounts

Count the occurrences of unique values. 

Similar to SQL `GROUP BY val COUNT(*)`.

```csharp
public DataFrame ValueCounts(
    bool sort = true,
    bool parallel = true,
    string? name = null,
    bool normalize = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `sort` | `bool` | Sort the output by count in descending order. Default is true. |
| `parallel` | `bool` | Execute in parallel. Default is true. |
| `name` | `string?` | Give the resulting count column a specific name; if normalize is True this defaults to “proportion”, otherwise defaults to “count”.. |
| `normalize` | `bool` | If true, the count column will contain probabilities (fractions) instead of absolute counts. Default is false. |

**Returns:** A DataFrame with the series values and their counts.

**Example**

```csharp
var s = Series.From("fruit", ["apple", "apple", "banana"]);

// Default: sorted, absolute counts
s.ValueCounts().Show();

// Normalized (percentage)
s.ValueCounts(normalize: true, name: "prob").Show();
// Result
┌────────┬───────┐
│ fruit  ┆ prob  │
│ ---    ┆ ---   │
│ str    ┆ u32   │
╞════════╪═══════╡
│ apple  ┆ 3     │
│ orange ┆ 2     │
│ banana ┆ 1     │
└────────┴───────┘
```

---

### Var

Get the variance of this Series.

```csharp
public T? Var<T>(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | Delta Degrees of Freedom. The divisor used in calculations is N - ddof. |

---

### ZipWith

Take values from self or other based on the given mask. Where mask evaluates true, take values from self. Where mask evaluates false, take values from other.

```csharp
public Series ZipWith(Series mask, Series other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `mask` | `Series` | Boolean Series. |
| `other` | `Series` | Series of same type. |

**Returns:** Series

---

