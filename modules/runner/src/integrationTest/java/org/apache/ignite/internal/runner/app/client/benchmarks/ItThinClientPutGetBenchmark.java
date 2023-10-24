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

package org.apache.ignite.internal.runner.app.client.benchmarks;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParameters;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.testframework.TestIgnitionManager;

public class ItThinClientPutGetBenchmark {
    private Ignite server;

    private IgniteClient client;

    private void init() {
        String node0Name = "ItThinClientPutGetBenchmark";

        String cfg = "{\n"
                + "  network.port: 3344,\n"
                + "  network.nodeFinder.netClusterNodes: [ \"localhost:3344\" ]\n"
                + "  clientConnector.port: 10800,\n"
                + "  rest.port: 10300\n"
                + "}";

        CompletableFuture<Ignite> fut = TestIgnitionManager.start(node0Name, cfg, workDir.resolve(node0Name));

        String metaStorageNode = node0Name;

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(metaStorageNode)
                .metaStorageNodeNames(List.of(metaStorageNode))
                .clusterName("cluster")
                .build();
        TestIgnitionManager.init(initParameters);

        server = fut.join();
    }
}
