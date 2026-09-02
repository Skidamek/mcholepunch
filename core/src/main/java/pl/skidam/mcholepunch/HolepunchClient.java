package pl.skidam.mcholepunch;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import pl.skidam.mcholepunch.internal.HolepunchRequest;
import pl.skidam.mcholepunch.internal.client.NioHolepunchClient;

public final class HolepunchClient {
	private HolepunchClient() {}

	public static CompletionStage<HolepunchRoute> resolve(String host, int port) {
		return new NioHolepunchClient().resolve(host, port);
	}

	public static CompletionStage<HolepunchConnection> connect(String host, int port, int protocolVersion, HolepunchHandler handler, HolepunchOptions options) {
		Objects.requireNonNull(options, "options");
		return new NioHolepunchClient().connect(new HolepunchRequest(host, port, protocolVersion, handler, options));
	}

	public static CompletionStage<HolepunchConnection> connect(HolepunchRoute route, int protocolVersion, HolepunchHandler handler, HolepunchOptions options) {
		Objects.requireNonNull(route, "route");
		Objects.requireNonNull(options, "options");
		return new NioHolepunchClient().connect(new HolepunchRequest(route.host(), route.port(), protocolVersion, handler, options), route);
	}
}
