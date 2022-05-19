package org.apache.ignite.client.handler;

import io.netty.buffer.ByteBuf;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

public final class ClientSession {
    /** Session ID. */
    private final UUID id = UUID.randomUUID();

    /** Session resources. */
    private final ClientResourceRegistry resources = new ClientResourceRegistry();

    /** Pending outgoing messages (TODO data structure choice - ?). */
    private final ConcurrentLinkedQueue<ByteBuf> messageQueue = new ConcurrentLinkedQueue<>();

    private volatile Consumer<ByteBuf> messageConsumer;

    private volatile boolean closed;

    public UUID id() {
        return id;
    }

    public ClientResourceRegistry resources() {
        return resources;
    }

    public void channelInactive() {
        // TODO: Start timer or save current time.
        // TOOD: Locking
        messageConsumer = null;
    }

    public void channelActive(Consumer<ByteBuf> messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    public void enqueueMessage(ByteBuf buf) {
        // TODO: Locking.
        if (closed) {
            return;
        }

        if (messageConsumer != null) {
            messageConsumer.accept(buf);
        } else {
            messageQueue.add(buf);
        }
    }
}
