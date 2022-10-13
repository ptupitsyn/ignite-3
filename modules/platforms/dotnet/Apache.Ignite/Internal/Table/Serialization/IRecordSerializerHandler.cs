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
    using MessagePack;

    /// <summary>
    /// Serializer handler.
    /// </summary>
    /// <typeparam name="T">Record type.</typeparam>
    internal interface IRecordSerializerHandler<T>
    {
        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="part">Record part to read.</param>
        /// <returns>Record.</returns>
        T Read(ref MessagePackReader reader, Schema schema, TuplePart part = TuplePart.KeyAndVal);

        /// <summary>
        /// Reads the value part and combines with the specified key part into a new object.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="key">Key part.</param>
        /// <returns>Resulting record with key and value parts.</returns>
        T ReadValuePart(ref MessagePackReader reader, Schema schema, T key);

        /// <summary>
        /// Writes a record.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="schema">Schema.</param>
        /// <param name="record">Record.</param>
        /// <param name="part">Record part to write.</param>
        void Write(ref MessagePackWriter writer, Schema schema, T record, TuplePart part = TuplePart.KeyAndVal);
    }
}
