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
/// A slice of schema.
/// </summary>
/// <param name="Schema">Schema.</param>
/// <param name="Part">Part.</param>
internal readonly record struct SchemaSlice(Schema Schema, TuplePart Part = TuplePart.KeyAndVal)
{
    /// <summary>
    /// Gets the index range for the specified part.
    /// </summary>
    /// <returns>Index range.</returns>
    public (int Start, int Count) Range => Schema.GetRange(Part);

    /// <summary>
    /// Wraps a schema into a slice.
    /// </summary>
    /// <param name="schema">Schema.</param>
    /// <returns>Wrapped schema.</returns>
    public static implicit operator SchemaSlice(Schema schema) => new(schema);
}