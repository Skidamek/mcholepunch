package pl.skidam.mcholepunch.server.netty;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Holds the single channel-handover application registered for holepunch takeovers. */
public final class NettyChannelRegistry {
	private static final AtomicReference<Registration> REGISTRATION = new AtomicReference<>();

	private NettyChannelRegistry() {}

	public static Registration register(long maximumPendingWriteBytes, HolepunchChannelApplication application) {
		Objects.requireNonNull(application, "application");
		if (maximumPendingWriteBytes <= 0) {
			throw new IllegalArgumentException("maximumPendingWriteBytes must be positive");
		}
		Registration registration = new Registration(maximumPendingWriteBytes, application);
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
		private final HolepunchChannelApplication application;

		private Registration(long maximumPendingWriteBytes, HolepunchChannelApplication application) {
			this.maximumPendingWriteBytes = maximumPendingWriteBytes;
			this.application = application;
		}

		public long maximumPendingWriteBytes() {
			return maximumPendingWriteBytes;
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
