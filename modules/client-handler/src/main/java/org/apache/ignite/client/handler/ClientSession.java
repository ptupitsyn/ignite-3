package org.apache.ignite.client.handler;

import io.netty.buffer.ByteBuf;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public final class ClientSession {
    /** Session ID. */
    private final UUID id = UUID.randomUUID();

    /** Session resources. */
    private final ClientResourceRegistry resources = new ClientResourceRegistry();

    /** Pending outgoing messages. */
    private final ConcurrentLinkedQueue<ByteBuf> outbox = new ConcurrentLinkedQueue<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final ScheduledExecutorService scheduledExecutor;

    private final Consumer<ClientSession> onClosed;

    private final int connectionRestoreTimeout;

    private Consumer<ByteBuf> messageConsumer;

    private boolean closed;

    private long deactivationId;

    public ClientSession(
            ScheduledExecutorService scheduledExecutorService,
            int connectionRestoreTimeout,
            Consumer<ByteBuf> messageConsumer,
            Consumer<ClientSession> onClosed) {
        assert scheduledExecutorService != null;
        assert messageConsumer != null;
        assert onClosed != null;

        // TODO: Debug logging.
        // TODO: Do we need all this locking, or Netty does everything in one thread sequentially?
        scheduledExecutor = scheduledExecutorService;
        this.connectionRestoreTimeout = connectionRestoreTimeout;
        this.messageConsumer = messageConsumer;
        this.onClosed = onClosed;
    }

    public UUID id() {
        return id;
    }

    public ClientResourceRegistry resources() {
        return resources;
    }

    public boolean activate(Consumer<ByteBuf> messageConsumer) {
        rwLock.writeLock().lock();

        try {
            if (closed) {
                return false;
            }

            this.messageConsumer = messageConsumer;

            return true;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public boolean deactivate() {
        rwLock.writeLock().lock();

        try {
            if (closed) {
                return false;
            }

            messageConsumer = null;

            long deactivationId = ++this.deactivationId;

            scheduledExecutor.schedule(() -> close(deactivationId), connectionRestoreTimeout, TimeUnit.MILLISECONDS);

            return true;
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    public void sendQueuedBuffers() {
        rwLock.writeLock();

        try {
            if (closed || messageConsumer == null) {
                return;
            }

            while (true) {
                ByteBuf buf = outbox.poll();

                if (buf == null) {
                    break;
                }

                messageConsumer.accept(buf);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void send(ByteBuf buf) {
        rwLock.readLock().lock();

        try {
            if (closed) {
                // Session has been closed - discard requests.
                buf.release();
                return;
            }

            if (messageConsumer != null) {
                messageConsumer.accept(buf);
            } else {
                outbox.add(buf);
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void close(long deactivationId) {
        rwLock.writeLock().lock();

        try {
            if (deactivationId != this.deactivationId) {
                // Newer deactivation request has been scheduled.
                return;
            }

            messageConsumer = null;
            closed = true;

            resources.close();

            while (true) {
                ByteBuf buf = outbox.poll();

                if (buf == null) {
                    break;
                }

                buf.release();
            }

            onClosed.accept(this);
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
