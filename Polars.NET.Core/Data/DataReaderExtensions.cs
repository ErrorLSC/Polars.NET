using System.Data;
using Apache.Arrow;

namespace Polars.NET.Core.Data
{
    public static class DataReaderExtensions
    {
        /// <summary>
        /// 从 IDataReader 的 SchemaTable 推断 Arrow Schema。
        /// </summary>
        public static Schema GetArrowSchema(this IDataReader reader)
        {
            // 直接调用 Core 层的实现
            return DbToArrowStream.GetArrowSchema(reader);
        }

        /// <summary>
        /// 将 IDataReader 转换为 Arrow RecordBatch 流。
        /// </summary>
        public static IEnumerable<RecordBatch> ToArrowBatches(this IDataReader reader, int batchSize = 50_000)
        {
            // 直接返回 Core 层的流式包装器
            return DbToArrowStream.ToArrowBatches(reader,batchSize);
        }
    }
}