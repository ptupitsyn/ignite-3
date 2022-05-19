package org.apache.ignite.client.handler;

import io.netty.buffer.ByteBuf;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class ClientSession {
    /** Session ID. */
    private final UUID id = UUID.randomUUID();

    /** Session resources. */
    private final ClientResourceRegistry resources = new ClientResourceRegistry();

    /** Pending outgoing messages (TODO data structure choice - ?). */
    private final ConcurrentLinkedQueue<ByteBuf> messageQueue = new ConcurrentLinkedQueue<>();

    public UUID id() {
        return id;
    }

    public ClientResourceRegistry resources() {
        return resources;
    }

    public void channelInactive() {
        // TODO: Start timer or save current time.
    }

    public void enqueueMessage(ByteBuf buf) {
        // TODO: RW lock - don't enqueue messages if this session has timed out.
        // TODO: When reconnected, send old messages to the new socket (this method may be called when we already have a new connection)
        messageQueue.add(buf);
    }
}
