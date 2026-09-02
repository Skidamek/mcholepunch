package pl.skidam.mcholepunch.internal.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import pl.skidam.mcholepunch.HolepunchRoute;

class VanillaAddressResolverTest {
	@Test
	void skipsSrvLookupForIpLiterals() throws Exception {
		AtomicBoolean srvLookup = new AtomicBoolean();
		VanillaAddressResolver resolver = new VanillaAddressResolver() {
			@Override
			protected SrvRecord lookupSrv(String host) {
				srvLookup.set(true);
				return null;
			}
		};

		HolepunchRoute ipv4 = resolver.resolve("127.0.0.1", 25565);
		HolepunchRoute ipv6 = resolver.resolve("[::1]", 25565);
		HolepunchRoute mappedIpv6 = resolver.resolve("::ffff:192.0.2.1", 25565);
		HolepunchRoute scopedIpv6 = resolver.resolve("fe80::1%1", 25565);

		assertFalse(srvLookup.get());
		assertEquals("192.0.2.1", mappedIpv6.socketAddress().getAddress().getHostAddress());
		assertEquals("127.0.0.1", ipv4.socketAddress().getHostString());
		assertEquals(ipv4.socketAddress().getAddress().getHostAddress(), ipv4.socketAddress().getAddress().getHostName());
		assertTrue(ipv6.socketAddress().getAddress().getHostAddress().endsWith(":1"));
		assertEquals(ipv6.socketAddress().getAddress().getHostAddress(), ipv6.socketAddress().getAddress().getHostName());
		assertEquals(1, ((Inet6Address) scopedIpv6.socketAddress().getAddress()).getScopeId());
		assertEquals(scopedIpv6.socketAddress().getAddress().getHostAddress(), scopedIpv6.socketAddress().getAddress().getHostName());
	}

	@Test
	void rejectsInvalidAddresses() {
		VanillaAddressResolver resolver = new VanillaAddressResolver();

		assertThrows(UnknownHostException.class, () -> resolver.resolve(null, 25565));
		assertThrows(UnknownHostException.class, () -> resolver.resolve(" ", 25565));
		assertThrows(UnknownHostException.class, () -> resolver.resolve("[127.0.0.1]", 25565));
		assertThrows(UnknownHostException.class, () -> resolver.resolve("[::1", 25565));
		assertThrows(IllegalArgumentException.class, () -> resolver.resolve("localhost", 0));
		assertThrows(IllegalArgumentException.class, () -> resolver.resolve("localhost", 65536));
	}

	@Test
	void rejectsExplicitlyUnavailableSrvServices() {
		VanillaAddressResolver resolver = new VanillaAddressResolver() {
			@Override
			protected SrvRecord lookupSrv(String host) {
				return new SrvRecord(".", 0);
			}
		};

		assertThrows(UnknownHostException.class, () -> resolver.resolve("minecraft.example", 25565));
	}

	@Test
	void appliesSrvLookupForNamedDefaultPortHosts() throws Exception {
		VanillaAddressResolver resolver = new VanillaAddressResolver() {
			@Override
			protected SrvRecord lookupSrv(String host) {
				assertEquals("minecraft.example", host);
				return new SrvRecord("127.0.0.1", 25566);
			}
		};

		HolepunchRoute resolved = resolver.resolve(" minecraft.example ", 25565);

		assertEquals("minecraft.example", resolved.host());
		assertEquals(25565, resolved.port());
		assertEquals(25566, resolved.socketAddress().getPort());
		assertEquals("127.0.0.1", resolved.socketAddress().getAddress().getHostAddress());
	}
}
