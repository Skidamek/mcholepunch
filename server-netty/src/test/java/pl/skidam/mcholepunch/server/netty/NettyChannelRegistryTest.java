package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import pl.skidam.mcholepunch.internal.protocol.TransportFrame;

class NettyChannelRegistryTest {
	@Test
	void frameCapIsBoundedByTheProtocolCeiling() {
		HolepunchChannelApplication application = (channel, connection) -> {};
		assertThrows(IllegalArgumentException.class, () -> NettyChannelRegistry.register(1024, 0, application));
		assertThrows(IllegalArgumentException.class, () -> NettyChannelRegistry.register(1024, TransportFrame.MAX_FRAME_BYTES + 1, application));

		try (NettyChannelRegistry.Registration registration = NettyChannelRegistry.register(1024, 1024, application)) {
			assertEquals(1024, registration.maximumPendingWriteBytes());
			assertEquals(1024, registration.maximumFrameBytes());
		}
	}
}
