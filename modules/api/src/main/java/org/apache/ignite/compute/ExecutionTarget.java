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

package org.apache.ignite.compute;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

public interface ExecutionTarget {
    static ExecutionTarget node(ClusterNode node) {
        return new NodesExecutionTarget(List.of(node), false);
    }

    static ExecutionTarget anyNode(Collection<ClusterNode> nodes) {
        return new NodesExecutionTarget(nodes, false);
    }

    static ExecutionTarget anyNode(ClusterNode... nodes) {
        return new NodesExecutionTarget(List.of(nodes), false);
    }

    static ExecutionTarget allNodes(Collection<ClusterNode> nodes) {
        return new NodesExecutionTarget(nodes, true);
    }

    static ExecutionTarget allNodes(ClusterNode... nodes) {
        return new NodesExecutionTarget(List.of(nodes), true);
    }

    static ExecutionTarget colocated(String tableName, Tuple key) {
        return new ColocationKeyExecutionTarget(tableName, key, null);
    }

    static <K> ExecutionTarget colocated(String tableName, K key, Mapper<K> keyMapper) {
        return new ColocationKeyExecutionTarget(tableName, key, keyMapper);
    }
}
