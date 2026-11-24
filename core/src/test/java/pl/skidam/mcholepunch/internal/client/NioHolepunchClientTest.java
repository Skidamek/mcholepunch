package pl.skidam.mcholepunch.internal.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.HolepunchOptions;
import pl.skidam.mcholepunch.HolepunchRoute;
import pl.skidam.mcholepunch.internal.HolepunchRequest;
import pl.skidam.mcholepunch.internal.net.VanillaAddressResolver;

class NioHolepunchClientTest {
	@Test
	void resolvedRouteBypassesResolver() throws Exception {
		AtomicInteger resolutions = new AtomicInteger();
		VanillaAddressResolver resolver = new VanillaAddressResolver() {
			@Override
			public HolepunchRoute resolve(String host, int port) {
				resolutions.incrementAndGet();
				throw new AssertionError("resolver should not be called");
			}
		};
		int closedPort;
		try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			closedPort = socket.getLocalPort();
		}

		HolepunchRoute route = HolepunchRoute.at("minecraft.example", 25565, new InetSocketAddress(InetAddress.getLoopbackAddress(), closedPort));
		HolepunchRequest request = new HolepunchRequest(route, 763, new HolepunchHandler() {
		},
				HolepunchOptions.builder().build());

		assertThrows(Exception.class, () -> new NioHolepunchClient(resolver).connect(request).toCompletableFuture().get(5, TimeUnit.SECONDS));
		assertEquals(0, resolutions.get());
	}

	@Test
	void resolveRunsOnItsOwnPoolAndReturnsTheRoute() throws Exception {
		int port;
		try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			port = socket.getLocalPort();
		}
		HolepunchRoute route = new NioHolepunchClient(new VanillaAddressResolver() {
			@Override
			public HolepunchRoute resolve(String host, int resolvedPort) {
				assertEquals("minecraft.example", host);
				return HolepunchRoute.at(host, resolvedPort, new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
			}
		}).resolve("minecraft.example", 25565).toCompletableFuture().get(5, TimeUnit.SECONDS);
		assertEquals(25565, route.handshakePort());
		assertEquals(port, route.transportAddress().getPort());
	}
}
