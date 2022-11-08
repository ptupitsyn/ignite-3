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

namespace Apache.Ignite.Internal.Table
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Buffers;
    using Common;
    using Ignite.Table;
    using Ignite.Transactions;
    using Linq;
    using Proto;
    using Serialization;
    using Sql;
    using Transactions;

    /// <summary>
    /// Generic record view.
    /// </summary>
    /// <typeparam name="T">Record type.</typeparam>
    internal sealed class RecordView<T> : IRecordView<T>
        where T : notnull
    {
        /** Table. */
        private readonly Table _table;

        /** Serializer. */
        private readonly RecordSerializer<T> _ser;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordView{T}"/> class.
        /// </summary>
        /// <param name="table">Table.</param>
        /// <param name="ser">Serializer.</param>
        public RecordView(Table table, RecordSerializer<T> ser)
        {
            _table = table;
            _ser = ser;
        }

        /// <summary>
        /// Gets the record serializer.
        /// </summary>
        public RecordSerializer<T> RecordSerializer => _ser;

        /// <inheritdoc/>
        public async Task<Option<T>> GetAsync(ITransaction? transaction, T key)
        {
            IgniteArgumentCheck.NotNull(key, nameof(key));

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleGet, transaction, key, keyOnly: true).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return _ser.ReadValue(resBuf, resSchema, key);
        }

        /// <inheritdoc/>
        public async Task<IList<Option<T>>> GetAllAsync(ITransaction? transaction, IEnumerable<T> keys) =>
            await GetAllAsync(
                transaction: transaction,
                keys: keys,
                resultFactory: static count => count == 0
                    ? (IList<Option<T>>)Array.Empty<Option<T>>()
                    : new List<Option<T>>(count),
                addAction: static (res, item) => res.Add(item))
                .ConfigureAwait(false);

        /// <summary>
        /// Gets multiple records by keys.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="keys">Collection of records with key columns set.</param>
        /// <param name="resultFactory">Result factory.</param>
        /// <param name="addAction">Add action.</param>
        /// <typeparam name="TRes">Result type.</typeparam>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains matching records with all columns filled from the table. The order of collection
        /// elements is guaranteed to be the same as the order of <paramref name="keys"/>. If a record does not exist,
        /// the element at the corresponding index of the resulting collection will be empty <see cref="Option{T}"/>.
        /// </returns>
        public async Task<TRes> GetAllAsync<TRes>(
            ITransaction? transaction,
            IEnumerable<T> keys,
            Func<int, TRes> resultFactory,
            Action<TRes, Option<T>> addAction)
        {
            IgniteArgumentCheck.NotNull(keys, nameof(keys));

            using var iterator = keys.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return resultFactory(0);
            }

            var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = transaction.ToInternal();

            using var writer = ProtoCommon.GetMessageWriter();
            var colocationHash = _ser.WriteMultiple(writer, tx, schema, iterator, keyOnly: true);
            var preferredNode = await _table.GetPreferredNode(colocationHash, transaction).ConfigureAwait(false);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleGetAll, tx, writer, preferredNode).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            // TODO: Read value parts only (IGNITE-16022).
            return _ser.ReadMultipleNullable(resBuf, resSchema, resultFactory, addAction);
        }

        /// <inheritdoc/>
        public async Task UpsertAsync(ITransaction? transaction, T record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleUpsert, transaction, record).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task UpsertAllAsync(ITransaction? transaction, IEnumerable<T> records)
        {
            IgniteArgumentCheck.NotNull(records, nameof(records));

            using var iterator = records.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return;
            }

            var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = transaction.ToInternal();

            using var writer = ProtoCommon.GetMessageWriter();
            var colocationHash = _ser.WriteMultiple(writer, tx, schema, iterator);
            var preferredNode = await _table.GetPreferredNode(colocationHash, transaction).ConfigureAwait(false);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleUpsertAll, tx, writer, preferredNode).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<Option<T>> GetAndUpsertAsync(ITransaction? transaction, T record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleGetAndUpsert, transaction, record).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return _ser.ReadValue(resBuf, resSchema, record);
        }

        /// <inheritdoc/>
        public async Task<bool> InsertAsync(ITransaction? transaction, T record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleInsert, transaction, record).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<IList<T>> InsertAllAsync(ITransaction? transaction, IEnumerable<T> records)
        {
            IgniteArgumentCheck.NotNull(records, nameof(records));

            using var iterator = records.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return Array.Empty<T>();
            }

            var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = transaction.ToInternal();

            using var writer = ProtoCommon.GetMessageWriter();
            var colocationHash = _ser.WriteMultiple(writer, tx, schema, iterator);
            var preferredNode = await _table.GetPreferredNode(colocationHash, transaction).ConfigureAwait(false);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleInsertAll, tx, writer, preferredNode).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            // TODO: Read value parts only (IGNITE-16022).
            return _ser.ReadMultiple(
                buf: resBuf,
                schema: resSchema,
                keyOnly: false,
                resultFactory: static count => count == 0
                    ? (IList<T>)Array.Empty<T>()
                    : new List<T>(count),
                addAction: static (res, item) => res.Add(item));
        }

        /// <inheritdoc/>
        public async Task<bool> ReplaceAsync(ITransaction? transaction, T record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleReplace, transaction, record).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> ReplaceAsync(ITransaction? transaction, T record, T newRecord)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = transaction.ToInternal();

            using var writer = ProtoCommon.GetMessageWriter();
            var colocationHash = _ser.WriteTwo(writer, tx, schema, record, newRecord);
            var preferredNode = await _table.GetPreferredNode(colocationHash, transaction).ConfigureAwait(false);

            using var resBuf = await DoOutInOpAsync(ClientOp.TupleReplaceExact, tx, writer, preferredNode).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<Option<T>> GetAndReplaceAsync(ITransaction? transaction, T record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleGetAndReplace, transaction, record).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return _ser.ReadValue(resBuf, resSchema, record);
        }

        /// <inheritdoc/>
        public async Task<bool> DeleteAsync(ITransaction? transaction, T key)
        {
            IgniteArgumentCheck.NotNull(key, nameof(key));

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleDelete, transaction, key, keyOnly: true).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> DeleteExactAsync(ITransaction? transaction, T record)
        {
            IgniteArgumentCheck.NotNull(record, nameof(record));

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleDeleteExact, transaction, record).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        /// <inheritdoc/>
        public async Task<Option<T>> GetAndDeleteAsync(ITransaction? transaction, T key)
        {
            IgniteArgumentCheck.NotNull(key, nameof(key));

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleGetAndDelete, transaction, key, keyOnly: true).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            return _ser.ReadValue(resBuf, resSchema, key);
        }

        /// <inheritdoc/>
        public async Task<IList<T>> DeleteAllAsync(ITransaction? transaction, IEnumerable<T> keys) =>
            await DeleteAllAsync(transaction, keys, exact: false).ConfigureAwait(false);

        /// <inheritdoc/>
        public async Task<IList<T>> DeleteAllExactAsync(ITransaction? transaction, IEnumerable<T> records) =>
            await DeleteAllAsync(transaction, records, exact: true).ConfigureAwait(false);

        /// <inheritdoc/>
        public IQueryable<T> AsQueryable(ITransaction? transaction = null)
        {
            // TODO: AsQueryable should be present in all kinds of views - extract a separate interface?
            // TODO: Accept options with PageSize, Timeout.
#pragma warning disable CA2000 // TODO: Fix this

            var sql = new Sql(_table.Socket); // TODO: Reuse existing SQL from Ignite
            var executor = new CacheFieldsQueryExecutor(_table, sql);
            var ignite = new IgniteClientInternal(_table.Socket); // TODO: Reuse existing client.
            var cacheValueType = typeof(T); // TODO: ???
            var provider = new CacheFieldsQueryProvider(CacheQueryParser.Instance, executor, ignite, _table.Name, cacheValueType);

#pragma warning restore CA2000

            return new CacheFieldsQueryable<T>(provider);
        }

        /// <summary>
        /// Deletes multiple records. If one or more keys do not exist, other records are still deleted.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="records">Record keys to delete.</param>
        /// <param name="exact">Whether to match on both key and value.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains records from <paramref name="records"/> that did not exist.
        /// </returns>
        public async Task<IList<T>> DeleteAllAsync(ITransaction? transaction, IEnumerable<T> records, bool exact) =>
            await DeleteAllAsync(
                transaction,
                records,
                resultFactory: static count => count == 0
                    ? (IList<T>)Array.Empty<T>()
                    : new List<T>(count),
                addAction: static (res, item) => res.Add(item),
                exact: exact).ConfigureAwait(false);

        /// <summary>
        /// Deletes multiple records. If one or more keys do not exist, other records are still deleted.
        /// </summary>
        /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
        /// <param name="records">Record keys to delete.</param>
        /// <param name="resultFactory">Result factory.</param>
        /// <param name="addAction">Add action.</param>
        /// <param name="exact">Whether to match on both key and value.</param>
        /// <typeparam name="TRes">Result type.</typeparam>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains records from <paramref name="records"/> that did not exist.
        /// </returns>
        public async Task<TRes> DeleteAllAsync<TRes>(
            ITransaction? transaction,
            IEnumerable<T> records,
            Func<int, TRes> resultFactory,
            Action<TRes, T> addAction,
            bool exact)
        {
            IgniteArgumentCheck.NotNull(records, nameof(records));

            using var iterator = records.GetEnumerator();

            if (!iterator.MoveNext())
            {
                return resultFactory(0);
            }

            var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = transaction.ToInternal();

            using var writer = ProtoCommon.GetMessageWriter();
            var colocationHash = _ser.WriteMultiple(writer, tx, schema, iterator, keyOnly: !exact);
            var preferredNode = await _table.GetPreferredNode(colocationHash, transaction).ConfigureAwait(false);

            var clientOp = exact ? ClientOp.TupleDeleteAllExact : ClientOp.TupleDeleteAll;
            using var resBuf = await DoOutInOpAsync(clientOp, tx, writer, preferredNode).ConfigureAwait(false);
            var resSchema = await _table.ReadSchemaAsync(resBuf).ConfigureAwait(false);

            // TODO: Read value parts only (IGNITE-16022).
            return _ser.ReadMultiple(
                buf: resBuf,
                schema: resSchema,
                keyOnly: !exact,
                resultFactory: resultFactory,
                addAction: addAction);
        }

        /// <summary>
        /// Determines if the table contains an entry for the specified key.
        /// </summary>
        /// <param name="transaction">Transaction.</param>
        /// <param name="key">Key.</param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous operation.
        /// The task result contains a value indicating whether a record with the specified key exists in the table.
        /// </returns>
        internal async Task<bool> ContainsKey(ITransaction? transaction, T key)
        {
            IgniteArgumentCheck.NotNull(key, nameof(key));

            using var resBuf = await DoRecordOutOpAsync(ClientOp.TupleContainsKey, transaction, key, keyOnly: true).ConfigureAwait(false);
            return resBuf.GetReader().ReadBoolean();
        }

        private async Task<PooledBuffer> DoOutInOpAsync(
            ClientOp clientOp,
            Transaction? tx,
            PooledArrayBufferWriter? request = null,
            PreferredNode preferredNode = default) =>
            await _table.Socket.DoOutInOpAsync(clientOp, tx, request, preferredNode).ConfigureAwait(false);

        private async Task<PooledBuffer> DoRecordOutOpAsync(
            ClientOp op,
            ITransaction? transaction,
            T record,
            bool keyOnly = false)
        {
            var schema = await _table.GetLatestSchemaAsync().ConfigureAwait(false);
            var tx = transaction.ToInternal();

            using var writer = ProtoCommon.GetMessageWriter();
            var colocationHash = _ser.Write(writer, tx, schema, record, keyOnly);
            var preferredNode = await _table.GetPreferredNode(colocationHash, transaction).ConfigureAwait(false);

            return await DoOutInOpAsync(op, tx, writer, preferredNode).ConfigureAwait(false);
        }
    }
}
