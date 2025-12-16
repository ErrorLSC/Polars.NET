using System;
using System.Collections;
using System.Collections.Generic;
using Apache.Arrow;

namespace Polars.NET.Core.Arrow
{
    /// <summary>
    /// 缝合枚举器：将预读的 Head (First Batch) 与剩余的 Tail 重新组合成一个完整的流。
    /// </summary>
    public class PrependEnumerator(RecordBatch head, IEnumerator<RecordBatch> tail) : IEnumerator<RecordBatch>
    {
        private bool _isFirst = true;
        private readonly RecordBatch _head = head;
        private readonly IEnumerator<RecordBatch> _tail = tail;

        public RecordBatch Current => _isFirst ? _head : _tail.Current;
        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (_isFirst)
            {
                _isFirst = false;
                return true;
            }
            return _tail.MoveNext();
        }

        public void Reset() => throw new NotSupportedException();

        public void Dispose()
        {
            // 确保头尾都被释放
            _head.Dispose();
            _tail.Dispose();
        }
    }
}