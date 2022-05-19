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
        // TODO: Locking
        assert this.messageConsumer == null : "Invalid session state: channel is already active.";

        this.messageConsumer = messageConsumer;

        while (true) {
            ByteBuf buf = messageQueue.remove();

            if (buf == null) {
                return;
            }

            messageConsumer.accept(buf);
        }
    }

    public void send(ByteBuf buf) {
        // TODO: Locking.
        if (closed) {
            // Session has been closed - discard requests.
            buf.release();
            return;
        }

        if (messageConsumer != null) {
            messageConsumer.accept(buf);
        } else {
            messageQueue.add(buf);
        }
    }
}
