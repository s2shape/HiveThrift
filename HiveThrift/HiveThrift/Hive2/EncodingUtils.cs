using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Thrift.Transport
{
    public static class EncodingUtils
    {
        public static void encodeBigEndian(int integer, byte[] buf)
        {
            encodeBigEndian(integer, buf, 0);
        }

        public static void encodeBigEndian(int integer, byte[] buf, int offset)
        {
            buf[offset] = (byte)(0xff & (integer >> 24));
            buf[offset + 1] = (byte)(0xff & (integer >> 16));
            buf[offset + 2] = (byte)(0xff & (integer >> 8));
            buf[offset + 3] = (byte)(0xff & (integer));
        }

        public static int decodeBigEndian(byte[] buf)
        {
            return decodeBigEndian(buf, 0);
        }

        public static int decodeBigEndian(byte[] buf, int offset)
        {
            return ((buf[offset] & 0xff) << 24) | ((buf[offset + 1] & 0xff) << 16)
                | ((buf[offset + 2] & 0xff) << 8) | ((buf[offset + 3] & 0xff));
        }

        public static void encodeFrameSize(int frameSize, byte[] buf)
        {
            buf[0] = (byte)(0xff & (frameSize >> 24));
            buf[1] = (byte)(0xff & (frameSize >> 16));
            buf[2] = (byte)(0xff & (frameSize >> 8));
            buf[3] = (byte)(0xff & (frameSize));
        }

        public static int decodeFrameSize(byte[] buf)
        {
            return
              ((buf[0] & 0xff) << 24) |
              ((buf[1] & 0xff) << 16) |
              ((buf[2] & 0xff) << 8) |
              ((buf[3] & 0xff));
        }
    }
}

namespace Hive2
{
    public static class Utils
    {
        public static bool IsEmpty(this IEnumerable enumerable)
        {
            var enumerator = enumerable != null ? enumerable.GetEnumerator() : null;
            return enumerator == null || !enumerator.MoveNext();
        }

        public static IEnumerable<List<T>> SplitByCount<T>(this IEnumerable<T> list, int count)
        {
            if (list == null || count == 0) throw new ArgumentNullException("list can't be null or count can't be 0");
            int sendCount = 0;
            List<T> result = null;
            while (true)
            {
                result = list.Skip(sendCount).Take(count).ToList();
                if (result.IsEmpty())
                    break;
                sendCount += count;
                yield return result;
            }
        }

        public static TRowSet CombineColumnValues(TRowSet rowSet1, TRowSet rowSet2)
        {
            if (rowSet1 == null)
            {
                return rowSet2;
            }
            if (rowSet2 == null)
            {
                return rowSet1;
            }
            if(rowSet1.Columns.Count != rowSet2.Columns.Count)
            {
                throw new ArgumentException("two rowset should have same columns");
            }
            TRowSet result = new TRowSet();
            result.Rows = new List<TRow>();
            
            result.Columns = new List<TColumn>();
            for (int i = 0; i < rowSet1.Columns.Count; i++)
            {
                TColumn combinedColumn = new TColumn();
                
                TColumn col1 = rowSet1.Columns[i];
                TColumn col2 = rowSet2.Columns[i];
                Trace.Assert(col1 != null && col2 != null);

                if (col1.__isset.binaryVal || col2.__isset.binaryVal)
                {
                    combinedColumn.BinaryVal = new TBinaryColumn() { Values = new List<byte[]>()};
                    combinedColumn.BinaryVal.Values.AddRange((col1.BinaryVal?.Values)??new List<byte[]>());
                    combinedColumn.BinaryVal.Values.AddRange((col2.BinaryVal?.Values)??new List<byte[]>());
                    combinedColumn.__isset.binaryVal = true;
                }

                if (col1.__isset.boolVal || col2.__isset.boolVal)
                {
                    combinedColumn.BoolVal = new TBoolColumn() { Values = new List<bool>() };
                    combinedColumn.BoolVal.Values.AddRange((col1.BoolVal?.Values)??new List<bool>());
                    combinedColumn.BoolVal.Values.AddRange((col2.BoolVal?.Values)??new List<bool>());
                    combinedColumn.__isset.boolVal = true;
                }

                if (col1.__isset.byteVal || col2.__isset.byteVal)
                {
                    combinedColumn.ByteVal = new TByteColumn() { Values = new List<sbyte>() };
                    combinedColumn.ByteVal.Values.AddRange((col1.ByteVal?.Values)??new List<sbyte>());
                    combinedColumn.ByteVal.Values.AddRange((col2.ByteVal?.Values)??new List<sbyte>());
                    combinedColumn.__isset.binaryVal = true;
                }
                if (col1.__isset.doubleVal || col2.__isset.doubleVal)
                {
                    combinedColumn.DoubleVal = new TDoubleColumn() { Values = new List<double>() };
                    combinedColumn.DoubleVal.Values.AddRange((col1.DoubleVal?.Values)??new List<double>());
                    combinedColumn.DoubleVal.Values.AddRange((col2.DoubleVal?.Values)??new List<double>());
                    combinedColumn.__isset.doubleVal = true;
                }
                if (col1.__isset.i16Val || col2.__isset.i16Val)
                {
                    combinedColumn.I16Val = new TI16Column() { Values = new List<short>() };
                    combinedColumn.I16Val.Values.AddRange((col1.I16Val?.Values)??new List<short>());
                    combinedColumn.I16Val.Values.AddRange((col2.I16Val?.Values)??new List<short>());
                    combinedColumn.__isset.i16Val = true;
                }
                if (col1.__isset.i32Val|| col2.__isset.i32Val)
                {
                    combinedColumn.I32Val = new TI32Column() { Values = new List<int>() };
                    combinedColumn.I32Val.Values.AddRange((col1.I32Val?.Values)??new List<int>());
                    combinedColumn.I32Val.Values.AddRange((col2.I32Val?.Values)??new List<int>());
                    combinedColumn.__isset.i32Val = true;
                }
                if (col1.__isset.i64Val || col1.__isset.i64Val)
                {
                    combinedColumn.I64Val = new TI64Column() { Values = new List<long>() };
                    combinedColumn.I64Val.Values.AddRange((col1.I64Val?.Values)??new List<long>());
                    combinedColumn.I64Val.Values.AddRange((col2.I64Val?.Values)??new List<long>());
                    combinedColumn.__isset.i64Val = true;
                }
                if (col1.__isset.stringVal || col2.__isset.stringVal)
                {
                    combinedColumn.StringVal = new TStringColumn() { Values = new List<string>() };
                    combinedColumn.StringVal.Values.AddRange((col1.StringVal?.Values)??new List<string>());
                    combinedColumn.StringVal.Values.AddRange((col2.StringVal?.Values)??new List<string>());
                    combinedColumn.__isset.stringVal = true;
                }
                
                result.Columns.Add(combinedColumn);
            }
            return result;
        }
    }
}