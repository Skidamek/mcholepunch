package pl.skidam.mcholepunch.internal;

import java.util.Objects;

import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.HolepunchOptions;

public record HolepunchRequest(String host, int port, int protocolVersion, HolepunchHandler handler, HolepunchOptions options) {
	public HolepunchRequest {
		Objects.requireNonNull(host, "host");
		if (host.isBlank()) {
			throw new IllegalArgumentException("host must not be blank");
		}
		if (port < 1 || port > 65535) {
			throw new IllegalArgumentException("port out of range");
		}
		if (protocolVersion < 0) {
			throw new IllegalArgumentException("protocolVersion must be non-negative");
		}
		Objects.requireNonNull(handler, "handler");
		Objects.requireNonNull(options, "options");
	}
}
