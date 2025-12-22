using System.Collections;
using System.Data;
using System.Data.Common;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.NET.Core.Data
{
    /// <summary>
    /// 将 Arrow RecordBatch 流伪装成 IDataReader。
    /// 用于将 Polars/Arrow 数据流式写入数据库 (配合 SqlBulkCopy 等)。
    /// </summary>
    public class ArrowToDbStream : DbDataReader
    {
        private readonly IEnumerator<RecordBatch> _batchEnumerator;
        private Schema? _schema; // 允许延迟初始化
        private RecordBatch? _currentBatch;
        private int _currentRowIndex;
        private bool _isClosed;
        // 类型覆盖字典 (列名 -> 强行指定的类型)
        private readonly Dictionary<string, Type>? _typeOverrides;

        public ArrowToDbStream(IEnumerable<RecordBatch> stream,Dictionary<string, Type>? typeOverrides = null)
        {
            ArgumentNullException.ThrowIfNull(stream);
            _batchEnumerator = stream.GetEnumerator();
            _currentRowIndex = -1;
            _typeOverrides = typeOverrides ?? new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);
        }

        private bool EnsureSchema()
        {
            if (_schema != null) return true;
            
            // 尝试读取第一个 Batch 以获取 Schema
            if (_batchEnumerator.MoveNext())
            {
                _currentBatch = _batchEnumerator.Current;
                _schema = _currentBatch.Schema;
                return true;
            }
            
            _isClosed = true;
            return false;
        }

        public override bool Read()
        {
            if (_isClosed) return false;

            // 确保 Schema 已加载 (处理刚初始化的情况)
            if (_schema == null)
            {
                if (!EnsureSchema()) return false; // 流是空的
            }

            _currentRowIndex++;

            // 1. 当前 Batch 还有数据
            if (_currentBatch != null && _currentRowIndex < _currentBatch.Length)
            {
                return true;
            }

            // 2. 需要读取下一个 Batch
            // 注意：如果是刚初始化且 EnsureSchema 已经读了第一个 Batch，
            // 且第一个 Batch 为空或者刚读完，这里逻辑要小心处理。
            // 简单起见：如果 _hasReadFirstBatch 为 true，说明 currentBatch 已经是新的了，
            // 但如果 currentBatch 读完了，我们需要再次 MoveNext
            
            // 为了逻辑简单，我们重置标记，强制 MoveNext
            if (_batchEnumerator.MoveNext())
            {
                _currentBatch?.Dispose();
                _currentBatch = _batchEnumerator.Current;
                _schema = _currentBatch.Schema; // Schema 可能会变 (但在 SQL 场景不应变)
                _currentRowIndex = 0;

                // 跳过空 Batch (Arrow 允许空 Batch)
                if (_currentBatch.Length == 0) return Read();

                return true;
            }

            // 3. 没数据了
            _currentBatch = null!;
            _isClosed = true;
            return false;
        }

        public override object GetValue(int ordinal)
        {
            // 1. 获取原始值 (可能是退化后的 long 或 int)
            // 先通过 base 获取，确保做了基础的 null 检查等
            // 但这里我们要复用 GetValueFromArray 的逻辑，所以直接调
            if (_currentBatch == null) throw new InvalidOperationException("No batch");
            var col = _currentBatch.Column(ordinal);
            if (col.IsNull(_currentRowIndex)) return DBNull.Value;
            
            var val = GetValueFromArray(col, _currentRowIndex);
            
            // 2. 获取目标类型 (可能是用户 Override 的，也可能是 Schema 自带的)
            var targetType = GetFieldType(ordinal);

            // 如果原始值类型和目标类型一致，直接返回 (性能优化)
            if (val.GetType() == targetType) return val;

            // -------------------------------------------------------------
            // 魔法转换区域：物理类型 -> 逻辑类型
            // -------------------------------------------------------------

            // A. 处理 Long (Int64) 的退化 -> Timestamp, Time, Duration
            if (val is long lVal)
            {
                // 1. long -> DateTime (Timestamp)
                if (targetType == typeof(DateTime))
                {
                    // 假设 Microseconds (Polars 默认)
                    return DateTime.UnixEpoch.AddTicks(lVal * 10); 
                }

                // 2. [新增] long -> DateTimeOffset (带时区支持)
                if (targetType == typeof(DateTimeOffset))
                {
                    // 返回 UTC 时间点 (+00:00)
                    // 数据库通常能处理 UTC 的 DateTimeOffset
                    return new DateTimeOffset(DateTime.UnixEpoch.Ticks + (lVal * 10), TimeSpan.Zero);
                }

                // 3. [新增] long -> TimeSpan (Duration)
                if (targetType == typeof(TimeSpan))
                {
                    // 1 us = 10 ticks
                    return new TimeSpan(lVal * 10);
                }

                // 4. [新增] long -> TimeOnly (Time64)
                if (targetType == typeof(TimeOnly))
                {
                    return new TimeOnly(lVal * 10);
                }
            }

            // B. 处理 Int (Int32) 的退化 -> Date32
            else if (val is int iVal)
            {
                // 5. [新增] int -> DateOnly (Date32)
                if (targetType == typeof(DateOnly))
                {
                    // Arrow Date32 是 "Days since Unix Epoch"
                    return DateOnly.FromDateTime(DateTime.UnixEpoch.AddDays(iVal));
                }
            }

            // C. 其他情况 (比如 float -> decimal 之类的，如果需要也可以加)
            
            return val;
        }
        private static object GetValueFromArray(IArrowArray array, int index)
        {
            switch (array)
            {
                case Int32Array arr: return arr.GetValue(index).GetValueOrDefault();
                case Int64Array arr: return arr.GetValue(index).GetValueOrDefault();
                case FloatArray arr: return arr.GetValue(index).GetValueOrDefault();
                case DoubleArray arr: return arr.GetValue(index).GetValueOrDefault();
                case BooleanArray arr: return arr.GetValue(index).GetValueOrDefault();
                case StringArray arr: return arr.GetString(index);
                case LargeStringArray arr: return arr.GetString(index);
                case StringViewArray arr: return arr.GetString(index);
                case BinaryArray arr: return arr.GetBytes(index).ToArray();
                case BinaryViewArray arr: return arr.GetBytes(index).ToArray();
                // [修复] Date32Array
                case Date32Array arr:
                    // arr.GetDateTime(index) 返回的是 DateTime?
                    var dt = arr.GetDateTime(index);
                    return dt.HasValue ? DateOnly.FromDateTime(dt.Value) : DBNull.Value;

                // [修复] TimestampArray
                // Arrow C# 的 TimestampArray 通常返回 DateTimeOffset?
                // 而 DataTable/SQL 需要 DateTime，所以这里保留 .DateTime
                case TimestampArray arr: 
                    return arr.GetTimestamp(index)?.DateTime ?? (object)DBNull.Value;
                case Date64Array arr:
                     return arr.GetDateTime(index) ?? (object)DBNull.Value;                
                case Time64Array arr:
                    var micros = arr.GetMicroSeconds(index);
                    if (!micros.HasValue) return DBNull.Value;
                    // 1微秒 = 10 Ticks
                    return TimeOnly.FromTimeSpan(TimeSpan.FromTicks(micros.Value * 10));
                case DurationArray arr:
                {
                    // DurationArray 通常也是基于 long 存储的
                    // 注意：这里假设 Unit 是 Microsecond (和 GetArrowType 保持一致)
                    // 如果 Arrow C# 的 DurationArray API 不同，可能需要 arr.GetValue(index)
                    var val = arr.GetValue(index); 
                    if (!val.HasValue) return DBNull.Value;

                    // 1 us = 10 ticks
                    return new TimeSpan(val.Value * 10);
                }
                // 复杂类型兜底
                case ListArray arr: return "List<...>"; 
                case StructArray arr: return "{Struct}";
                
                default:
                    // 尝试通用获取 (性能较差)
                    // return array.GetValue(index) ?? DBNull.Value;
                    throw new NotSupportedException($"ArrowToDbStream does not yet support type: {array.GetType().Name}");
            }
        }
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            if (_currentBatch == null) return 0;
            var column = _currentBatch.Column(ordinal);
            
            if (column.IsNull(_currentRowIndex)) return 0;

            // 获取完整的字节数组
            ReadOnlySpan<byte> bytes;
            
            if (column is BinaryArray binArr)
                bytes = binArr.GetBytes(_currentRowIndex);
            // else if (column is LargeBinaryArray largeBinArr) ... // 如果用了 LargeBinary
            else
                throw new InvalidCastException($"Column {ordinal} is not a BinaryArray.");

            // 如果 buffer 为 null，返回总长度
            if (buffer == null)
            {
                return bytes.Length;
            }

            // 复制数据到 buffer
            int available = bytes.Length - (int)dataOffset;
            int toCopy = Math.Min(available, length);

            if (toCopy > 0)
            {
                bytes.Slice((int)dataOffset, toCopy).CopyTo(buffer.AsSpan(bufferOffset));
            }

            return toCopy;
        }

        // [修复 3] 实现 GetChars (用于读取超长字符串流)
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            if (_currentBatch == null) return 0;
            
            // 我们复用 GetValue 拿到的 string
            string val = GetString(ordinal);
            
            if (val == null) return 0;

            if (buffer == null)
            {
                return val.Length;
            }

            int available = val.Length - (int)dataOffset;
            int toCopy = Math.Min(available, length);

            if (toCopy > 0)
            {
                val.CopyTo((int)dataOffset, buffer, bufferOffset, toCopy);
            }

            return toCopy;
        }
        // --- 元数据接口 ---

        public override int FieldCount 
        { 
            get 
            {
                EnsureSchema();
                return _schema?.FieldsList.Count ?? 0;
            }
        }

        public override string GetName(int ordinal)
        {
            EnsureSchema();
            return _schema?.GetFieldByIndex(ordinal).Name ?? string.Empty;
        }

        public override int GetOrdinal(string name)
        {
            EnsureSchema();
            return _schema?.GetFieldIndex(name) ?? -1;
        }

        public override Type GetFieldType(int ordinal)
        {
            EnsureSchema();
            if (_schema == null) return typeof(object);
            var field = _schema.GetFieldByIndex(ordinal);
            // [新增] 如果用户指定了覆盖类型，优先返回用户指定的！
            if (_typeOverrides!.TryGetValue(field.Name, out var targetType))
            {   
                return targetType;
            }
            return field.DataType switch
            {
                Int32Type => typeof(int),
                Int64Type => typeof(long),
                FloatType => typeof(float),
                DoubleType => typeof(double),
                BooleanType => typeof(bool),
                StringType => typeof(string),
                LargeStringType => typeof(string),
                StringViewType => typeof(string), 
                TimestampType => typeof(DateTime),
                Date32Type => typeof(DateOnly),
                Date64Type => typeof(DateTime), // 毫秒级日期
                Time32Type => typeof(TimeOnly), // 或 TimeOnly
                Time64Type => typeof(TimeOnly),
                DurationType => typeof(TimeSpan),
                BinaryType => typeof(byte[]),
                _ => typeof(object)
            };
        }

        public override bool IsDBNull(int ordinal)
        {
            if (_currentBatch == null) return true;
            return _currentBatch.Column(ordinal).IsNull(_currentRowIndex);
        }

        // --- Boilerplate (直接代理) ---
        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(GetOrdinal(name));
        public override bool HasRows => EnsureSchema();
        public override bool NextResult() => false; 
        public override int Depth => 0;
        public override int RecordsAffected => -1;

        public override bool IsClosed => _isClosed;

        public override void Close()
        {
            if (!_isClosed)
            {
                _isClosed = true;
                _batchEnumerator.Dispose();
                _currentBatch = null;
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();
            }
            base.Dispose(disposing);
        }
        public override int GetValues(object[] values)
        {
            // 确保有数据
            if (_isClosed || _currentBatch == null) return 0;

            // 确定要拷贝多少列 (取 Buffer 长度和 实际列数 的最小值)
            int copyCount = Math.Min(values.Length, FieldCount);

            for (int i = 0; i < copyCount; i++)
            {
                // 直接复用我们写好的 GetValue，它已经处理了 Arrow -> C# 的转换
                values[i] = GetValue(i);
            }

            return copyCount;
        }
        public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);
        public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;
        
        public override bool GetBoolean(int ordinal) => (bool)GetValue(ordinal);
        public override byte GetByte(int ordinal) => (byte)GetValue(ordinal);
        public override char GetChar(int ordinal) => (char)GetValue(ordinal);
        public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);
        public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(GetValue(ordinal));
        public override double GetDouble(int ordinal) => (double)GetValue(ordinal);
        public override float GetFloat(int ordinal) => (float)GetValue(ordinal);
        public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);
        public override short GetInt16(int ordinal) => (short)GetValue(ordinal);
        public override int GetInt32(int ordinal) => (int)GetValue(ordinal);
        public override long GetInt64(int ordinal) => (long)GetValue(ordinal);
        public override string GetString(int ordinal) => (string)GetValue(ordinal);

        public override DataTable GetSchemaTable()
        {
            EnsureSchema();
            if (_schema == null) return null!;

            var table = new DataTable("SchemaTable");

            // 1. 定义标准 Schema Table 的列
            // 参考: https://learn.microsoft.com/en-us/dotnet/api/system.data.idatareader.getschematable
            table.Columns.Add("ColumnName", typeof(string));
            table.Columns.Add("ColumnOrdinal", typeof(int));
            table.Columns.Add("ColumnSize", typeof(int));
            table.Columns.Add("NumericPrecision", typeof(short));
            table.Columns.Add("NumericScale", typeof(short));
            table.Columns.Add("DataType", typeof(Type));
            table.Columns.Add("ProviderType", typeof(Type)); // 可选
            table.Columns.Add("IsLong", typeof(bool));
            table.Columns.Add("AllowDBNull", typeof(bool));
            table.Columns.Add("IsReadOnly", typeof(bool));
            table.Columns.Add("IsRowVersion", typeof(bool));
            table.Columns.Add("IsUnique", typeof(bool));
            table.Columns.Add("IsKey", typeof(bool));
            table.Columns.Add("IsAutoIncrement", typeof(bool));
            table.Columns.Add("BaseSchemaName", typeof(string));
            table.Columns.Add("BaseCatalogName", typeof(string));
            table.Columns.Add("BaseTableName", typeof(string));
            table.Columns.Add("BaseColumnName", typeof(string));

            // 2. 填充数据
            for (int i = 0; i < _schema.FieldsList.Count; i++)
            {
                var field = _schema.GetFieldByIndex(i);
                var row = table.NewRow();

                row["ColumnName"] = field.Name;
                row["ColumnOrdinal"] = i;
                row["DataType"] = GetFieldType(i); // 复用我们实现的 GetFieldType
                row["ColumnSize"] = -1; // 未知或可变
                row["AllowDBNull"] = field.IsNullable;
                
                // 默认值
                row["NumericPrecision"] = DBNull.Value;
                row["NumericScale"] = DBNull.Value;
                row["IsLong"] = false;
                row["IsReadOnly"] = true; // Arrow 流通常是只读的
                row["IsRowVersion"] = false;
                row["IsUnique"] = false;
                row["IsKey"] = false;
                row["IsAutoIncrement"] = false;
                row["BaseSchemaName"] = DBNull.Value;
                row["BaseCatalogName"] = DBNull.Value;
                row["BaseTableName"] = DBNull.Value;
                row["BaseColumnName"] = field.Name;

                table.Rows.Add(row);
            }

            return table;
        }
    }
}