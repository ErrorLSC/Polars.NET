using Polars.CSharp;
using static Polars.CSharp.Polars;
using Polars.NET.ML.CSharpExtensions;
using Polars.NET.ML.DataView;
using Microsoft.ML;
using Microsoft.ML.Data;
using GraphQL;

namespace Polars.Integration.Tests;

public class DataViewConversionTests
{
    [Fact]
    [Trait("ML","DataView")]
    public void PrimitiveTypes_RoundTrip_ShouldMaintainAbsoluteFidelity()
    {

        int[] expectedIds = [1, 2, 3, 4, 5];
        float[] expectedScores = [1.1f, 2.2f, float.NaN, 4.4f, float.PositiveInfinity];
        bool[] expectedFlags = [true, false, true, true, false];
        string[] expectedNames = ["Alice", "Bob", "", "Dave", "Eve-Super-Long-String-To-Test-Memory-Pool"];

        var originalDf = DataFrame.FromSeries(
            Series.From("Id", expectedIds),
            Series.From("Score", expectedScores),
            Series.From("IsActive", expectedFlags),
            Series.From("Name", expectedNames)
        );

        // ==========================================
        // Polars -> ML.NET
        // ==========================================
        var dataView = originalDf.AsDataView();

        Assert.Equal(4, dataView.Schema.Count);
        Assert.Equal(5, dataView.GetRowCount());

        using (var cursor = dataView.GetRowCursor(dataView.Schema))
        {
            var idGetter = cursor.GetGetter<int>(dataView.Schema["Id"]);
            var scoreGetter = cursor.GetGetter<float>(dataView.Schema["Score"]);
            var flagGetter = cursor.GetGetter<bool>(dataView.Schema["IsActive"]);
            var nameGetter = cursor.GetGetter<ReadOnlyMemory<char>>(dataView.Schema["Name"]);

            int rowIndex = 0;
            int id = 0;
            float score = 0;
            bool flag = false;
            ReadOnlyMemory<char> name = default;

            while (cursor.MoveNext())
            {
                idGetter(ref id);
                scoreGetter(ref score);
                flagGetter(ref flag);
                nameGetter(ref name);

                Assert.Equal(expectedIds[rowIndex], id);
                
                if (float.IsNaN(expectedScores[rowIndex]))
                    Assert.True(float.IsNaN(score));
                else
                    Assert.Equal(expectedScores[rowIndex], score);

                Assert.Equal(expectedFlags[rowIndex], flag);
                Assert.Equal(expectedNames[rowIndex], name.Span.ToString());

                rowIndex++;
            }
            Assert.Equal(5, rowIndex); 
        }

        // ==========================================
        // ML.NET -> Polars
        // ==========================================
        using var roundTripDf = dataView.ToDataFrame(batchSize: 2);

        Assert.NotNull(roundTripDf);
        Assert.Equal(originalDf.Height, roundTripDf.Height);
        Assert.Equal(originalDf.Width, roundTripDf.Width);

        var roundTripSchema = roundTripDf.Schema;
        Assert.Contains("Id", roundTripSchema.ColumnNames);
        Assert.Contains("Score", roundTripSchema.ColumnNames);
        Assert.Contains("IsActive", roundTripSchema.ColumnNames);
        Assert.Contains("Name", roundTripSchema.ColumnNames);

        Assert.Equal(5, roundTripDf.Height);
    }
    [Fact]
    [Trait("ML", "GCPerformance")]
    public void DataView_Bidirectional_ShouldNotTriggerGarbageCollection()
    {
        int rowCount = 500_000;
        
        int[] ids = new int[rowCount];
        float[] scores = new float[rowCount];
        bool[] flags = new bool[rowCount];
        string[] names = new string[rowCount];

        Array.Fill(ids, 42);
        Array.Fill(scores, 3.14159f);
        Array.Fill(flags, true);
        Array.Fill(names, "Zero-Allocation-GC-Probe-Test");

        var originalDf = DataFrame.FromSeries(
            Series.From("Id", ids),
            Series.From("Score", scores),
            Series.From("IsActive", flags),
            Series.From("Name", names)
        );

        var dataView = originalDf.AsDataView();

        // ==========================================
        // Warm-up
        // ==========================================
        RunCursorHotLoop(dataView, assertZeroAlloc: false);
        using (var tempDf = dataView.ToDataFrame(batchSize: 64_000)) { }

        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // ==========================================
        // Polars -> ML.NET
        // ==========================================
        RunCursorHotLoop(dataView, assertZeroAlloc: true);

        // ==========================================
        // ML.NET -> Polars
        // ==========================================
        int beforeGen0Pump = GC.CollectionCount(0);

        using var finalDf = dataView.ToDataFrame(batchSize: 64_000);

        int afterGen0Pump = GC.CollectionCount(0);
        int gen0Collections = afterGen0Pump - beforeGen0Pump;

        Assert.True(gen0Collections < 10, 
            $"Gen 0 GC {gen0Collections} times. Memory Leak detected.");
        Assert.Equal(rowCount, finalDf.Height);
    }

    private static void RunCursorHotLoop(IDataView dataView, bool assertZeroAlloc)
    {
        using var cursor = dataView.GetRowCursor(dataView.Schema);
        
        var idGetter = cursor.GetGetter<int>(dataView.Schema["Id"]);
        var scoreGetter = cursor.GetGetter<float>(dataView.Schema["Score"]);
        var flagGetter = cursor.GetGetter<bool>(dataView.Schema["IsActive"]);
        var nameGetter = cursor.GetGetter<ReadOnlyMemory<char>>(dataView.Schema["Name"]);

        int id = 0;
        float score = 0;
        bool flag = false;
        ReadOnlyMemory<char> name = default;

        long startBytes = GC.GetAllocatedBytesForCurrentThread();

        while (cursor.MoveNext())
        {
            idGetter(ref id);
            scoreGetter(ref score);
            flagGetter(ref flag);
            nameGetter(ref name);
        }

        long endBytes = GC.GetAllocatedBytesForCurrentThread();
        long allocatedBytesInHotLoop = endBytes - startBytes;

        if (assertZeroAlloc)
        {
            Assert.True(allocatedBytesInHotLoop < 5000, 
                $"Hot Path allocated {allocatedBytesInHotLoop} bytes,memory leak detected");
        }
    }
    [Fact]
    [Trait("ML", "Tensor")]
    public void TensorTypes_RoundTrip_ShouldMaintainShapeAndValues()
    {
        int vectorSize = 4;
        
        float[,] expectedFloatVectors = {
            { 1.1f, 2.2f, 3.3f, 4.4f },
            { 0.0f, -1.5f, float.NaN, 9.9f },
            { 0.0f, 0.0f, 0.0f, 0.0f }
        };

        int[,] expectedIntVectors = {
            { 101, 2056, 3001, 102 },
            { 0, 0, 0, 0 },
            { -1, 999, 888, 777 }
        };

        var originalDf = DataFrame.FromSeries(
            Series.From("FloatFeatures", expectedFloatVectors),
            Series.From("TokenIds", expectedIntVectors)
        );
        originalDf.Show();
        // ==========================================
        // Polars -> ML.NET
        // ==========================================
        var dataView = originalDf.AsDataView();

        Assert.Equal(2, dataView.Schema.Count);
        Assert.Equal(3, dataView.GetRowCount());

        var floatColType = dataView.Schema["FloatFeatures"].Type as VectorDataViewType;
        var intColType = dataView.Schema["TokenIds"].Type as VectorDataViewType;

        Assert.NotNull(floatColType);
        Assert.Equal(4, floatColType.Size);
        Assert.Equal(NumberDataViewType.Single, floatColType.ItemType);

        Assert.NotNull(intColType);
        Assert.Equal(4, intColType.Size);
        Assert.Equal(NumberDataViewType.Int32, intColType.ItemType);

        using (var cursor = dataView.GetRowCursor(dataView.Schema))
        {
            var floatGetter = cursor.GetGetter<VBuffer<float>>(dataView.Schema["FloatFeatures"]);
            var intGetter = cursor.GetGetter<VBuffer<int>>(dataView.Schema["TokenIds"]);

            int rowIndex = 0;
            VBuffer<float> floatVal = default;
            VBuffer<int> intVal = default;

            while (cursor.MoveNext())
            {
                floatGetter(ref floatVal);
                intGetter(ref intVal);

                Assert.Equal(4, floatVal.Length);
                Assert.Equal(4, intVal.Length);

                var floatSpan = floatVal.GetValues();
                var intSpan = intVal.GetValues();

                for (int i = 0; i < vectorSize; i++)
                {
                    if (float.IsNaN(expectedFloatVectors[rowIndex, i]))
                        Assert.True(float.IsNaN(floatSpan[i]));
                    else
                        Assert.Equal(expectedFloatVectors[rowIndex, i], floatSpan[i]);

                    Assert.Equal(expectedIntVectors[rowIndex, i], intSpan[i]);
                }

                rowIndex++;
            }
            Assert.Equal(3, rowIndex);
        }

        // ==========================================
        // ML.NET -> Polars
        // ==========================================
        using var roundTripDf = dataView.ToDataFrame(batchSize: 2);

        Assert.NotNull(roundTripDf);
        Assert.Equal(originalDf.Height, roundTripDf.Height);
        Assert.Equal(originalDf.Width, roundTripDf.Width);

        var roundTripSchema = roundTripDf.Schema;
        Assert.Contains("FloatFeatures", roundTripSchema.ColumnNames);
        Assert.Contains("TokenIds", roundTripSchema.ColumnNames);
        roundTripDf.Show();
    }
    // public record Iris(long Id, float SepalLengthCm, float SepalWidthCm,float PetalLengthCm,float PetalWidthCm, string Species);
    [Fact]
    [Trait("ML", "E2E")]
    public void HuggingFace_IrisDataset_KMeansClustering_ShouldTrainAndPredict()
    {
        // ==========================================
        // Data Loading
        // ==========================================
        var hfUrl = "https://huggingface.co/datasets/scikit-learn/iris/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet";
        var options = CloudOptions.Http(new Dictionary<string, string>
        {
            { "User-Agent", "Polars.NET-Test" }
        });
        // var explicitSchema = PolarsSchema.From<Iris>();
        using var lf = LazyFrame.ScanParquet(hfUrl, cloudOptions: options);
        using var df = lf.Collect(useStreaming: true);

        Assert.Equal(150, df.Height);
        
        // sepal length (cm), sepal width (cm), petal length (cm), petal width (cm)
        string[] featureCols = ["SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm"];

        // Polars Data Cleaning
        var exprs = featureCols.Select(name => Col(name).Cast(DataType.Float32)).ToArray();
        
        using var cleanDf = df.WithColumns(exprs);
        cleanDf.Show();
        // ==========================================
        // Polars -> ML.NET
        // ==========================================
        var dataView = cleanDf.AsDataView();
        
        var mlContext = new MLContext(seed: 42);

        // ==========================================
        // ML.NET Pipeline
        // ==========================================
        // Form VBuffer<float> tensor
        var pipeline = mlContext.Transforms.Concatenate("Features", 
                featureCols[0], featureCols[1], featureCols[2], featureCols[3])
            // K-Means for 3 categories
            .Append(mlContext.Clustering.Trainers.KMeans("Features", numberOfClusters: 3));

        var model = pipeline.Fit(dataView);

        // ==========================================
        // ML.NET Transform and Read Back
        // ==========================================
        var predictions = model.Transform(dataView);

        // ML.NET -> Polars
        using var resultDf = predictions.ToDataFrame();

        Assert.NotNull(resultDf);
        Assert.Equal(150, resultDf.Height);
        resultDf.Show();
        
        var resultSchema = resultDf.Schema;
        Assert.Contains("PredictedLabel", resultSchema.ColumnNames);
        Assert.Contains("Score", resultSchema.ColumnNames);

        var scoreDataType = resultSchema["Score"];
        
        Assert.Equal(scoreDataType, DataType.Array(DataType.Float32,3));
    }
}

