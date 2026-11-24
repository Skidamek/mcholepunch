package pl.skidam.mcholepunch.server.netty;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import pl.skidam.mcholepunch.HolepunchOptions;

/** Holds the single channel-handover application registered for holepunch takeovers. */
public final class NettyChannelRegistry {
	private static final AtomicReference<Registration> REGISTRATION = new AtomicReference<>();

	private NettyChannelRegistry() {}

	/**
	 * Registers an application with the default send-side frame cap
	 * ({@link HolepunchOptions#DEFAULT_MAXIMUM_FRAME_BYTES}).
	 *
	 * @param maximumPendingWriteBytes
	 *            write-buffer water mark for taken-over channels
	 *            ({@code channel.isWritable()}), not a hard cap
	 */
	public static Registration register(long maximumPendingWriteBytes, HolepunchChannelApplication application) {
		return register(maximumPendingWriteBytes, HolepunchOptions.DEFAULT_MAXIMUM_FRAME_BYTES, application);
	}

	/**
	 * @param maximumPendingWriteBytes
	 *            write-buffer water mark for taken-over channels
	 *            ({@code channel.isWritable()}), not a hard cap
	 * @param maximumFrameBytes
	 *            send-side transport frame cap, from 1 up to
	 *            {@link HolepunchOptions#MAX_FRAME_BYTES}; writes above it are split and the
	 *            receiver accepts the whole range
	 */
	public static Registration register(long maximumPendingWriteBytes, int maximumFrameBytes, HolepunchChannelApplication application) {
		Objects.requireNonNull(application, "application");
		if (maximumPendingWriteBytes <= 0) {
			throw new IllegalArgumentException("maximumPendingWriteBytes must be positive");
		}
		if (maximumFrameBytes < 1 || maximumFrameBytes > HolepunchOptions.MAX_FRAME_BYTES) {
			throw new IllegalArgumentException("maximumFrameBytes must be between 1 and " + HolepunchOptions.MAX_FRAME_BYTES);
		}
		Registration registration = new Registration(maximumPendingWriteBytes, maximumFrameBytes, application);
		if (!REGISTRATION.compareAndSet(null, registration)) {
			throw new IllegalStateException("a holepunch channel application is already registered");
		}
		return registration;
	}

	public static Registration current() {
		return REGISTRATION.get();
	}

	public static final class Registration implements AutoCloseable {
		private final long maximumPendingWriteBytes;
		private final int maximumFrameBytes;
		private final HolepunchChannelApplication application;

		private Registration(long maximumPendingWriteBytes, int maximumFrameBytes, HolepunchChannelApplication application) {
			this.maximumPendingWriteBytes = maximumPendingWriteBytes;
			this.maximumFrameBytes = maximumFrameBytes;
			this.application = application;
		}

		public long maximumPendingWriteBytes() {
			return maximumPendingWriteBytes;
		}

		public int maximumFrameBytes() {
			return maximumFrameBytes;
		}

		public HolepunchChannelApplication application() {
			return application;
		}

		@Override
		public void close() {
			REGISTRATION.compareAndSet(this, null);
		}
	}
}
