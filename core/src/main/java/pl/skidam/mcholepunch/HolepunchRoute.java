package pl.skidam.mcholepunch;

import java.net.InetSocketAddress;
import java.util.Objects;

public record HolepunchRoute(String host, int port, InetSocketAddress socketAddress) {
	public HolepunchRoute {
		Objects.requireNonNull(host, "host");
		if (host.isBlank()) {
			throw new IllegalArgumentException("host must not be blank");
		}
		if (port < 1 || port > 65535) {
			throw new IllegalArgumentException("port out of range");
		}
		Objects.requireNonNull(socketAddress, "socketAddress");
		if (socketAddress.isUnresolved()) {
			throw new IllegalArgumentException("socketAddress must be resolved");
		}
	}
}
