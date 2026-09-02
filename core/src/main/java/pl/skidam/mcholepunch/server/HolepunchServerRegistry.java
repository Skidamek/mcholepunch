package pl.skidam.mcholepunch.server;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.HolepunchOptions;
import pl.skidam.mcholepunch.internal.protocol.HolepunchMarker;

public final class HolepunchServerRegistry {
	private static final AtomicReference<Registration> REGISTRATION = new AtomicReference<>();

	private HolepunchServerRegistry() {}

	public static Registration register(Executor executor, ApplicationHandlerFactory factory) {
		return register(executor, HolepunchOptions.DEFAULT_MAX_PENDING_WRITE_BYTES, factory);
	}

	public static Registration register(Executor executor, long maximumPendingWriteBytes, ApplicationHandlerFactory factory) {
		return register(executor, maximumPendingWriteBytes, HolepunchOptions.DEFAULT_HANDLER_SETUP_TIMEOUT, factory);
	}

	public static Registration register(Executor executor, long maximumPendingWriteBytes, Duration handlerSetupTimeout, ApplicationHandlerFactory factory) {
		Objects.requireNonNull(executor, "executor");
		Objects.requireNonNull(handlerSetupTimeout, "handlerSetupTimeout");
		if (handlerSetupTimeout.isZero() || handlerSetupTimeout.isNegative()) {
			throw new IllegalArgumentException("handlerSetupTimeout must be positive");
		}
		Objects.requireNonNull(factory, "factory");
		if (maximumPendingWriteBytes <= 0) {
			throw new IllegalArgumentException("maximumPendingWriteBytes must be positive");
		}
		Registration registration = new Registration(executor, maximumPendingWriteBytes, handlerSetupTimeout, factory);
		if (!REGISTRATION.compareAndSet(null, registration)) {
			throw new IllegalStateException("a holepunch application handler is already registered");
		}
		return registration;
	}

	public static Registration current() {
		return REGISTRATION.get();
	}

	@FunctionalInterface
	public interface ApplicationHandlerFactory {
		HolepunchHandler create(String username, SocketAddress remoteAddress, HolepunchMarker marker) throws Exception;
	}

	public static final class Registration implements AutoCloseable {
		private final Executor executor;
		private final long maximumPendingWriteBytes;
		private final Duration handlerSetupTimeout;
		private final ApplicationHandlerFactory factory;

		private Registration(Executor executor, long maximumPendingWriteBytes, Duration handlerSetupTimeout, ApplicationHandlerFactory factory) {
			this.executor = executor;
			this.maximumPendingWriteBytes = maximumPendingWriteBytes;
			this.handlerSetupTimeout = handlerSetupTimeout;
			this.factory = factory;
		}

		public Executor executor() {
			return executor;
		}

		public long maximumPendingWriteBytes() {
			return maximumPendingWriteBytes;
		}

		public Duration handlerSetupTimeout() {
			return handlerSetupTimeout;
		}

		public ApplicationHandlerFactory factory() {
			return factory;
		}

		@Override
		public void close() {
			REGISTRATION.compareAndSet(this, null);
		}
	}
}
