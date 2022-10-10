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

namespace Apache.Ignite.Table;

using System.Threading.Tasks;
using Transactions;

/// <summary>
/// Key-value view provides access to table records in form of separate key and value parts.
/// </summary>
/// <typeparam name="TK">Key type.</typeparam>
/// <typeparam name="TV">Value type.</typeparam>
public interface IKeyValueView<TK, TV>
    where TK : class
    where TV : class // TODO: Remove class constraint (IGNITE-16355)
{
    /// <summary>
    /// Gets a value associated with the given key.
    /// </summary>
    /// <param name="transaction">The transaction or <c>null</c> to auto commit.</param>
    /// <param name="key">A record with key columns set.</param>
    /// <returns>
    /// A <see cref="Task{TResult}"/> representing the asynchronous operation.
    /// The task result contains a record with all columns.
    /// </returns>
    Task<TV?> GetAsync(ITransaction? transaction, TK key);
}