package pl.skidam.mcholepunch;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * The login identity the peer sees and the socket this client dials. {@code handshakeHost} and
 * {@code handshakePort} go into the Minecraft login handshake; {@code transportAddress} is the
 * resolved TCP address.
 */
public record HolepunchRoute(String handshakeHost, int handshakePort, InetSocketAddress transportAddress) {
	public HolepunchRoute {
		Objects.requireNonNull(handshakeHost, "handshakeHost");
		if (handshakeHost.isBlank()) {
			throw new IllegalArgumentException("handshakeHost must not be blank");
		}
		if (handshakePort < 1 || handshakePort > 65535) {
			throw new IllegalArgumentException("handshakePort out of range");
		}
		Objects.requireNonNull(transportAddress, "transportAddress");
		if (transportAddress.isUnresolved()) {
			throw new IllegalArgumentException("transportAddress must be resolved");
		}
	}

	/** A route for a caller that already resolved the transport, such as the address the game reached. */
	public static HolepunchRoute at(String handshakeHost, int handshakePort, InetSocketAddress transportAddress) {
		return new HolepunchRoute(handshakeHost, handshakePort, transportAddress);
	}
}
