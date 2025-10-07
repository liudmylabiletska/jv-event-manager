package mate.academy;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EventManager {

    private static final Logger LOGGER = Logger.getLogger(EventManager.class.getName());
    private final Set<EventListener> listeners = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors());
    private final AtomicBoolean closed = new AtomicBoolean();

    public void registerListener(EventListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Listener cannot be null");
        }
        if (closed.get()) {
            throw new IllegalStateException("Cannot register listener: EventManager is shut down");
        }

        listeners.add(listener);
        if (closed.get()) {
            listeners.remove(listener);
            throw new IllegalStateException("Cannot register listener: EventManager is shut down");
        }
    }

    public void deregisterListener(EventListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Listener cannot be null");
        }
        listeners.remove(listener);
    }

    public void notifyEvent(Event event) {
        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }
        if (closed.get()) {
            throw new IllegalStateException("Cannot notify event: EventManager is shut down");
        }
        List<EventListener> snapshot = new ArrayList<>(listeners);

        for (EventListener l : snapshot) {
            try {
                executor.submit(() -> {
                    try {
                        l.onEvent(event);
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Exception caught in listener "
                                + l.getClass().getSimpleName() + " while processing event.", e);
                    }
                });
            } catch (RejectedExecutionException e) {
                LOGGER.log(Level.WARNING, "Task for listener " + l.getClass().getSimpleName()
                        + " rejected. Submission aborted due to shutdown.", e);
                break;
            }
        }
    }

    public void shutdown() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        executor.shutdown();
        try {
            boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);
            if (!terminated) {
                LOGGER.log(Level.WARNING, "Not all tasks finished, forcing immediate shutdown...");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
