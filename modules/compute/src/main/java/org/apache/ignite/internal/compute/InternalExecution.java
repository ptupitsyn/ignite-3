package org.apache.ignite.internal.compute;

import java.util.concurrent.CompletableFuture;

public interface InternalExecution<R> {
    /**
     * Returns execution result.
     *
     * @return Execution result future.
     */
    CompletableFuture<R> resultAsync();

}
