/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Internal.Table.Serialization
{
    using System;
    using Ignite.Sql;
    using Ignite.Table;
    using Proto;
    using Proto.BinaryTuple;
    using Proto.MsgPack;

    /// <summary>
    /// Serializer handler for <see cref="IIgniteTuple"/>.
    /// </summary>
    internal class TupleSerializerHandler : IRecordSerializerHandler<IIgniteTuple>
    {
        /// <summary>
        /// Singleton instance.
        /// </summary>
        public static readonly IRecordSerializerHandler<IIgniteTuple> Instance = new TupleSerializerHandler();

        /// <summary>
        /// Initializes a new instance of the <see cref="TupleSerializerHandler"/> class.
        /// </summary>
        private TupleSerializerHandler()
        {
            // No-op.
        }

        /// <inheritdoc/>
        public IIgniteTuple Read(ref MsgPackReader reader, Schema schema, bool keyOnly = false)
        {
            var columns = schema.Columns;
            var count = keyOnly ? schema.KeyColumnCount : columns.Count;
            var tuple = new IgniteTuple(count);
            var tupleReader = new BinaryTupleReader(reader.ReadBinary(), count);

            for (var index = 0; index < count; index++)
            {
                var column = columns[index];
                tuple[column.Name] = tupleReader.GetObject(index, column.Type, column.Scale);
            }

            return tuple;
        }

        /// <inheritdoc/>
        public void Write(ref BinaryTupleBuilder tupleBuilder, IIgniteTuple record, Schema schema, int columnCount, Span<byte> noValueSet)
        {
            for (var index = 0; index < columnCount; index++)
            {
                var col = schema.Columns[index];
                var colIdx = record.GetOrdinal(col.Name);

                if (colIdx >= 0)
                {
                    tupleBuilder.AppendObject(record[colIdx], col.Type, col.Scale, col.Precision);
                }
                else
                {
                    tupleBuilder.AppendNoValue(noValueSet);
                }
            }
        }

        /// <inheritdoc/>
        public int GetColocationHash(IIgniteTuple record, Schema schema)
        {
            // _hash = HashUtils.Hash32((sbyte)0, _hash);
            var hash = 0;

            for (int index = 0; index < schema.KeyColumnCount; index++)
            {
                if (!schema.IsHashedColumnIndex(index))
                {
                    continue;
                }

                var col = schema.Columns[index];
                var colIdx = record.GetOrdinal(col.Name);

                if (colIdx < 0)
                {
                    throw new InvalidOperationException($"Key column '{col.Name}' is missing in the tuple.");
                }

                object? val = record[colIdx];

                if (val == null)
                {
                    throw new InvalidOperationException($"Key column '{col.Name}' is null.");
                }

                hash = col.Type switch
                {
                    ColumnType.Boolean => HashUtils.Hash32((bool)val ? (sbyte)1 : (sbyte)0, hash),
                    ColumnType.Int8 => HashUtils.Hash32((sbyte)val, hash),
                    ColumnType.Int16 => HashUtils.Hash32((short)val, hash),
                    ColumnType.Int32 => HashUtils.Hash32((int)val, hash),
                    ColumnType.Int64 => HashUtils.Hash32((long)val, hash),
                    ColumnType.Float => expr,
                    ColumnType.Double => expr,
                    ColumnType.Decimal => expr,
                    ColumnType.Date => expr,
                    ColumnType.Time => expr,
                    ColumnType.Datetime => expr,
                    ColumnType.Timestamp => expr,
                    ColumnType.Uuid => expr,
                    ColumnType.Bitmask => expr,
                    ColumnType.String => expr,
                    ColumnType.ByteArray => expr,
                    ColumnType.Period => expr,
                    ColumnType.Duration => expr,
                    ColumnType.Number => expr,
                    _ => throw new ArgumentOutOfRangeException()
                };
            }

            return hash;
        }
    }
}
