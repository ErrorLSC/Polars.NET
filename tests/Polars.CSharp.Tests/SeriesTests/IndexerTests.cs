using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class SeriesIndexerTests
{
    [Fact]
    [Trait("Series", "Indexer")]
    public void Test_SetItem_By_SingleIndex()
    {
        using var s = Series.From("test_single", [1, 2, 3, 4, 5]);
        s[2] = 99; // Act

        int[] expected = [1, 2, 99, 4, 5];
        Assert.Equal(expected, s.ToArray<int>());
    }

    [Fact]
    [Trait("Series", "Indexer")]
    public void Test_SetItem_By_BooleanMask()
    {
        using var s = Series.From("test_mask", [10, 20, 30, 40, 50]);
        using var mask = s > 25;
        
        s[mask] = 0; // Act

        int[] expected = [10, 20, 0, 0, 0];
        Assert.Equal(expected, s.ToArray<int>());
    }

    [Fact]
    [Trait("Series", "Indexer")]
    public void Test_SetItem_By_IndicesArray()
    {
        using var s = Series.From("test_indices", [1, 2, 3, 4, 5]);
        int[] indices = [0, 3];
        
        s[indices] = 88; // Act

        int[] expected = [88, 2, 3, 88, 5];
        Assert.Equal(expected, s.ToArray<int>());
    }
    [Fact]
    [Trait("Series", "Indexer")]
    public void Test_GetItem_By_SingleIndex()
    {
        using var s = Series.From("test_get_single", [10, 20, 30]);
        
        // Act
        var val = s[1]; 
        
        // Assert
        Assert.Equal(20, val);
    }
    [Fact]
    [Trait("Series", "Indexer")]
    public void Test_GetItem_By_BooleanMask()
    {
        using var s = Series.From("test_get_mask", [10, 20, 30, 40, 50]);
        
        using var result = (Series)s[s>=30]!;

        // Assert
        int[] expected = [30, 40, 50];
        Assert.Equal(expected, result.ToArray<int>());
    }
    [Fact]
    [Trait("Series", "Indexer")]
    public void Test_GetItem_By_IndicesArray()
    {
        using var s = Series.From("test_get_array", ["apple", "banana", "cherry", "date"]);
        int[] indices = [1, 3];
        
        using var result = (Series)s[indices]!;

        // Assert
        string[] expected = ["banana", "date"];
        Assert.Equal(expected, result.ToArray<string>());
    }
    [Fact]
    [Trait("Series", "Indexer")]
    public void Test_GetItem_By_UInt32Series()
    {
        using var s = Series.From("test_get_u32", [1.1, 2.2, 3.3, 4.4, 5.5]);
        
        using var indices = Series.From("idx", [0u, 2u, 4u]);
        
        // Act
        using var result = (Series)s[indices]!;

        // Assert
        double[] expected = [1.1, 3.3, 5.5];
        Assert.Equal(expected, result.ToArray<double>());
    }
    [Fact]
    [Trait("Series", "Indexer")]
    public void Test_SetItem_Throws_On_OutOfBounds()
    {
        using var s = Series.From("test_bounds", [1, 2, 3]);

        Assert.Throws<IndexOutOfRangeException>(() => {
            s[5] = 99;
        });
    }
    [Fact]
    [Trait("Series", "Indexer")]
    public void Test_GetItem_Throws_On_InvalidType()
    {
        using var s = Series.From("test_invalid", [1, 2, 3]);
        using var invalidKey = Series.From("invalid_key", [1.5, 2.5]); 

        Assert.Throws<NotSupportedException>(() => {
            var _ = s[invalidKey];
        });
    }
    [Fact]
    [Trait("Series", "Indexer")]
    public void Test_Series_Range_Indexer_Get()
    {
        // Arrange
        using var s = Series.From("A", [10, 20, 30, 40, 50, 60]);

        using var slice1 = s[1..4];
        Assert.Equal(3, slice1.Length);
        Assert.Equal([20, 30, 40], slice1.ToArray<int>());

        using var slice2 = s[..^2];
        Assert.Equal(4, slice2.Length);
        Assert.Equal([10, 20, 30, 40], slice2.ToArray<int>());
    }
}