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

package org.apache.ignite.internal.tx.message;

import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.tx.TransactionMeta;
import org.apache.ignite.internal.tx.TxState;
import org.jetbrains.annotations.Nullable;

/** Message for transferring a {@link TransactionMeta}. */
public interface TransactionMetaMessage extends NetworkMessage {
    /** Ordinal of {@link TxState} value. */
    int txStateInt();

    /** Commit timestamp in primitive representation, {@link HybridTimestamp#NULL_HYBRID_TIMESTAMP} as {@code null}. */
    long commitTimestampLong();

    /** Transaction state. */
    default TxState txState() {
        TxState state = TxState.fromOrdinal(txStateInt());

        assert state != null : txStateInt();

        return state;
    }

    /** Commit timestamp. */
    default @Nullable HybridTimestamp commitTimestamp() {
        return nullableHybridTimestamp(commitTimestampLong());
    }

    /** Converts to {@link TransactionMeta}. */
    default TransactionMeta asTransactionMeta() {
        throw new AssertionError("Must be implemented by heirs.");
    }
}
