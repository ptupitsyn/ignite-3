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

namespace Apache.Ignite.Sql
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Exception is thrown when query cursor data fetch attempt is performed on a closed cursor.
    /// </summary>
    public class CursorClosedException : IgniteException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CursorClosedException"/> class.
        /// </summary>
        /// <param name="message">Message.</param>
        public CursorClosedException(string message)
            : base(Guid.NewGuid(), ErrorGroup.Sql.CursorClosed, message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CursorClosedException"/> class.
        /// </summary>
        /// <param name="traceId">Trace id.</param>
        /// <param name="code">Code.</param>
        /// <param name="message">Message.</param>
        /// <param name="innerException">Inner exception.</param>
        public CursorClosedException(Guid traceId, int code, string message, Exception? innerException = null)
            : base(traceId, code, message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CursorClosedException"/> class.
        /// </summary>
        protected CursorClosedException(SerializationInfo serializationInfo, StreamingContext streamingContext)
            : base(serializationInfo, streamingContext)
        {
            // No-op.
        }
    }
}
