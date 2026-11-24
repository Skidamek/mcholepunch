package pl.skidam.mcholepunch;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import pl.skidam.mcholepunch.internal.Usernames;
import pl.skidam.mcholepunch.internal.protocol.HolepunchMarker;

public final class HolepunchOptions {
	public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
	public static final Duration DEFAULT_NEGOTIATION_TIMEOUT = Duration.ofSeconds(10);
	/**
	 * Forge 1.17+ / NeoForge ClientIntention hostname extra. `ConnectionType.forVersionFlag` treats
	 * a host that contains this as MODDED with FML protocol 3. It lives here because appending it
	 * to the configured host is how a client shapes its own handshake; the library itself never
	 * inspects the value.
	 */
	public static final String FORGE_FML3_HANDSHAKE_HOST_SUFFIX = "\u0000FML3\u0000";

	private final String username;
	private final String handshakeHostSuffix;
	private final Duration connectTimeout;
	private final Duration negotiationTimeout;
	private final Executor callbackExecutor;
	private final byte[] markerConstant;

	private HolepunchOptions(Builder builder) {
		username = builder.username;
		handshakeHostSuffix = builder.handshakeHostSuffix;
		connectTimeout = builder.connectTimeout;
		negotiationTimeout = builder.negotiationTimeout;
		callbackExecutor = builder.callbackExecutor;
		markerConstant = builder.markerConstant;
	}

	public static Builder builder() {
		return new Builder();
	}

	public String username() {
		return username;
	}

	public String handshakeHostSuffix() {
		return handshakeHostSuffix;
	}

	public Duration connectTimeout() {
		return connectTimeout;
	}

	public Duration negotiationTimeout() {
		return negotiationTimeout;
	}

	public Executor callbackExecutor() {
		return callbackExecutor;
	}

	/**
	 * The 80-bit marker constant this client packs into its marker UUID, when the caller replaces
	 * the built-in one; empty when the built-in constant is used. Both ends of a deployment must
	 * supply the same value or the server replays the login to vanilla.
	 */
	public Optional<byte[]> markerConstant() {
		return markerConstant == null ? Optional.empty() : Optional.of(markerConstant.clone());
	}

	public static final class Builder {
		private String username;
		private String handshakeHostSuffix = "";
		private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
		private Duration negotiationTimeout = DEFAULT_NEGOTIATION_TIMEOUT;
		private Executor callbackExecutor = ForkJoinPool.commonPool();
		private byte[] markerConstant;

		public Builder username(String username) {
			this.username = Objects.requireNonNull(username, "username");
			return this;
		}

		public Builder handshakeHostSuffix(String suffix) {
			handshakeHostSuffix = Objects.requireNonNull(suffix, "handshakeHostSuffix");
			return this;
		}

		public Builder connectTimeout(Duration timeout) {
			connectTimeout = positive(timeout, "connectTimeout");
			return this;
		}

		public Builder negotiationTimeout(Duration timeout) {
			negotiationTimeout = positive(timeout, "negotiationTimeout");
			return this;
		}

		public Builder callbackExecutor(Executor executor) {
			callbackExecutor = Objects.requireNonNull(executor, "executor");
			return this;
		}

		/**
		 * Replaces the built-in 80-bit marker constant, so a deployment's logins are only claimed by
		 * servers installed with the same value. The value is not a secret: it decides routing, and
		 * anyone who reads it can still build a decodable marker.
		 */
		public Builder markerConstant(byte[] constant) {
			markerConstant = HolepunchMarker.requireMarkerConstant(constant);
			return this;
		}

		public HolepunchOptions build() {
			if (username != null && !Usernames.isValid(username)) {
				throw new IllegalArgumentException("username must be a valid Minecraft username");
			}
			return new HolepunchOptions(this);
		}

		private static Duration positive(Duration value, String name) {
			Objects.requireNonNull(value, name);
			if (value.isZero() || value.isNegative()) {
				throw new IllegalArgumentException(name + " must be positive");
			}
			return value;
		}
	}
}
