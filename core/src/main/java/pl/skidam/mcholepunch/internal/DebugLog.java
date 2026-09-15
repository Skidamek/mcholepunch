package pl.skidam.mcholepunch.internal;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pl.skidam.mcholepunch.HolepunchFailure;

/**
 * Opt-in transport tracing used to diagnose holepunch connections. Tracing is enabled with the
 * {@code mcholepunch.debug} system property and logs at info under the "mcholepunch" log4j
 * logger, so it lands in the host's logs without any logging configuration; Minecraft and every
 * loader provide log4j themselves. Without tracing, only a failure the transport itself produced
 * stays visible, as one warn line; a plain peer close is quiet.
 */
public final class DebugLog {
	private static final Logger LOGGER = LogManager.getLogger("mcholepunch");
	private static final boolean ENABLED = Boolean.parseBoolean(System.getProperty("mcholepunch.debug", "false"));
	private static final int PREVIEW_BYTES = Math.max(0, Integer.getInteger("mcholepunch.debug.previewBytes", 16));
	private static final long START_NANOS = System.nanoTime();
	private static final AtomicLong NEXT_ID = new AtomicLong();
	private static final ScheduledExecutorService WATCHDOG = Executors.newSingleThreadScheduledExecutor(runnable -> {
		Thread thread = new Thread(runnable, "mcholepunch-debug-watchdog");
		thread.setDaemon(true);
		return thread;
	});

	private DebugLog() {}

	public static boolean enabled() {
		return ENABLED;
	}

	public static long nextConnectionId() {
		return NEXT_ID.incrementAndGet();
	}

	public static void log(String side, long connectionId, String message, Object... arguments) {
		if (!enabled()) {
			return;
		}
		LOGGER.info(prefix(side, connectionId) + " " + format(message, arguments));
	}

	public static void failure(String side, long connectionId, String message, Throwable throwable) {
		boolean cleanClose = throwable instanceof HolepunchFailure failure && failure.kind() == HolepunchFailure.Kind.CLOSED;
		if (!ENABLED && cleanClose) {
			return;
		}
		String line = prefix(side, connectionId) + " " + format(message + ": " + throwable);
		if (ENABLED) {
			LOGGER.info(line, throwable);
			return;
		}
		LOGGER.warn(line);
	}

	/** Shared line header keeping the elapsed stamp, thread, and connection identity grep-friendly across log sinks. */
	private static String prefix(String side, long connectionId) {
		long elapsedMillis = Duration.ofNanos(System.nanoTime() - START_NANOS).toMillis();
		return "[+" + elapsedMillis + "ms][" + Thread.currentThread().getName() + "][" + side + "-" + connectionId + "]";
	}

	private static String format(String message, Object... arguments) {
		try {
			return arguments.length == 0 ? message : String.format(Locale.ROOT, message, arguments);
		} catch (Throwable formattingFailure) {
			return message + " [formatting failed: " + formattingFailure + "]";
		}
	}

	public static ScheduledFuture<?> watchdog(String side, long connectionId, Supplier<String> state) {
		if (!enabled()) {
			return null;
		}
		long seconds = Math.max(1L, Long.getLong("mcholepunch.debug.watchdogSeconds", 5L));
		return WATCHDOG.scheduleAtFixedRate(() -> {
			try {
				log(side, connectionId, "WATCHDOG %s", state.get());
			} catch (Throwable throwable) {
				failure(side, connectionId, "watchdog failed", throwable);
			}
		}, seconds, seconds, TimeUnit.SECONDS);
	}

	public static void cancel(ScheduledFuture<?> future) {
		if (future != null) {
			future.cancel(false);
		}
	}

	public static String preview(ByteBuffer source) {
		if (!enabled()) {
			return "";
		}
		ByteBuffer copy = source.duplicate();
		int total = copy.remaining();
		int length = Math.min(total, PREVIEW_BYTES);
		byte[] bytes = new byte[length];
		copy.get(bytes);
		return preview(bytes, total);
	}

	public static String preview(byte[] bytes) {
		if (!enabled()) {
			return "";
		}
		return preview(bytes, bytes.length);
	}

	private static String preview(byte[] bytes, int totalLength) {
		if (PREVIEW_BYTES == 0 || bytes.length == 0) {
			return "len=" + totalLength;
		}
		int length = Math.min(bytes.length, PREVIEW_BYTES);
		StringBuilder result = new StringBuilder(16 + length * 3);
		result.append("len=").append(totalLength).append(" hex=");
		for (int index = 0; index < length; index++) {
			if (index != 0) {
				result.append(' ');
			}
			int value = bytes[index] & 0xff;
			if (value < 16) {
				result.append('0');
			}
			result.append(Integer.toHexString(value));
		}
		if (length < totalLength) {
			result.append(" …");
		}
		return result.toString();
	}
}
