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

    /** Pending outgoing messages (TODO data structure choice - ?). */
    private final ConcurrentLinkedQueue<ByteBuf> messageQueue = new ConcurrentLinkedQueue<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final ScheduledExecutorService scheduledExecutor;

    private final Consumer<ClientSession> onClosed;

    private volatile Consumer<ByteBuf> messageConsumer;

    private volatile boolean closed;

    private volatile long channelInactiveTime;

    public ClientSession(
            ScheduledExecutorService scheduledExecutorService,
            Consumer<ByteBuf> messageConsumer,
            Consumer<ClientSession> onClosed) {
        assert scheduledExecutorService != null;
        assert messageConsumer != null;
        assert onClosed != null;

        // TODO: Debug logging.
        scheduledExecutor = scheduledExecutorService;
        this.messageConsumer = messageConsumer;
        this.onClosed = onClosed;
    }

    public UUID id() {
        return id;
    }

    public ClientResourceRegistry resources() {
        return resources;
    }

    public boolean deactivate() {
        rwLock.writeLock().lock();

        try {
            if (closed) {
                return false;
            }

            messageConsumer = null;

            long time = System.currentTimeMillis();
            channelInactiveTime = time;

            // TODO: Configurable timeout
            scheduledExecutor.schedule(() -> {
                // TODO: clock is not monotonic, it is better to rely on some other value (AtomicLong or something).
                if (time != channelInactiveTime) {
                    // Channel was activated and deactivated again, this scheduled action is no longer valid.
                    return;
                }

                close();
            }, 1000, TimeUnit.MILLISECONDS);

            return true;
        }
        finally {
            rwLock.writeLock().unlock();
        }
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

    public void sendQueuedBuffers() {
        while (true) {
            ByteBuf buf = messageQueue.poll();

            if (buf == null) {
                return;
            }
            messageConsumer.accept(buf);
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
                messageQueue.add(buf);
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void close() {
        rwLock.writeLock().lock();

        try {
            messageConsumer = null;
            closed = true;
        } finally {
            rwLock.writeLock().unlock();
        }

        resources.close();
        onClosed.accept(this);
    }
}
