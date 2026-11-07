package pl.skidam.mcholepunch.internal;

import java.util.Objects;

import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.HolepunchOptions;
import pl.skidam.mcholepunch.HolepunchRoute;

public record HolepunchRequest(HolepunchRoute route, int protocolVersion, HolepunchHandler handler, HolepunchOptions options) {
	public HolepunchRequest {
		Objects.requireNonNull(route, "route");
		if (protocolVersion < 0) {
			throw new IllegalArgumentException("protocolVersion must be non-negative");
		}
		Objects.requireNonNull(handler, "handler");
		Objects.requireNonNull(options, "options");
	}
}
