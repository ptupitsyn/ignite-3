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

namespace Apache.Ignite.Internal.Table.Serialization;

/// <summary>
/// Extensions for <see cref="Schema"/>.
/// </summary>
internal static class SchemaExtensions
{
    /// <summary>
    /// Gets the index range for the specified part.
    /// </summary>
    /// <param name="schema">Schema.</param>
    /// <param name="part">Part.</param>
    /// <returns>Index range.</returns>
    public static (int Start, int Count) GetRange(this Schema schema, TuplePart part) =>
        part switch
        {
            TuplePart.Key => (0, schema.KeyColumnCount),
            TuplePart.Val => (schema.KeyColumnCount, schema.ValueColumnCount),
            _ => (0, schema.Columns.Count)
        };

    /// <summary>
    /// Creates a schema slice.
    /// </summary>
    /// <param name="schema">Schema.</param>
    /// <param name="part">Part.</param>
    /// <returns>Slice.</returns>
    public static SchemaSlice Slice(this Schema schema, TuplePart part) => new(schema, part);

    /// <summary>
    /// Creates a schema slice.
    /// </summary>
    /// <param name="schema">Schema.</param>
    /// <param name="keyOnly">Whether to use only the key part.</param>
    /// <returns>Slice.</returns>
    public static SchemaSlice Slice(this Schema schema, bool keyOnly) => new(schema, keyOnly ? TuplePart.Key : TuplePart.KeyAndVal);

    /// <summary>
    /// Creates a schema slice.
    /// </summary>
    /// <param name="schema">Schema.</param>
    /// <returns>Slice.</returns>
    public static SchemaSlice SliceKey(this Schema schema) => new(schema, TuplePart.Key);

    /// <summary>
    /// Creates a schema slice.
    /// </summary>
    /// <param name="schema">Schema.</param>
    /// <returns>Slice.</returns>
    public static SchemaSlice SliceVal(this Schema schema) => new(schema, TuplePart.Val);
}