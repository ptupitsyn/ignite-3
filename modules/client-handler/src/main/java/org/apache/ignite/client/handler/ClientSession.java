package org.apache.ignite.client.handler;

import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class ClientSession {
    /** Session ID. */
    private final UUID id = UUID.randomUUID();

    /** Session resources. */
    private final ClientResourceRegistry resources = new ClientResourceRegistry();

    /** Pending responses (TODO data structure choice - ?). */
    private final ConcurrentLinkedQueue<Object> responseQueue = new ConcurrentLinkedQueue<>();

    public UUID id() {
        return id;
    }
}
