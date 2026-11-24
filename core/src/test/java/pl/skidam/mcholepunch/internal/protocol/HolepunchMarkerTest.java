package pl.skidam.mcholepunch.internal.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class HolepunchMarkerTest {
	@Test
	void roundTripsAndRetainsRfcBits() {
		UUID uuid = HolepunchMarker.create(0, 123456);
		Optional<HolepunchMarker> decoded = HolepunchMarker.decode(uuid);
		assertTrue(decoded.isPresent());
		assertEquals(1, decoded.get().protocolVersion());
		assertEquals(123456, decoded.get().nonce());
		assertEquals(4, uuid.version());
		assertEquals(2, uuid.variant());
	}

	@Test
	void masksFixedPrefixWithNonce() {
		UUID first = HolepunchMarker.create(0, 0);
		UUID second = HolepunchMarker.create(0, 1);

		assertNotEquals(first.getMostSignificantBits(), second.getMostSignificantBits());
	}

	@Test
	void rejectsWrongMarker() {
		assertTrue(HolepunchMarker.decode(UUID.randomUUID()).isEmpty());
	}

	@Test
	void rejectsFlagsItsOwnDecoderWouldReject() {
		assertThrows(IllegalArgumentException.class, () -> HolepunchMarker.create(1, 0));
		assertThrows(IllegalArgumentException.class, () -> new HolepunchMarker(1, 1, 0));
	}

	@Test
	void roundTripsACustomConstantAndSeparatesDomains() {
		byte[] custom = {(byte) 0xA5, 0x5A, (byte) 0xC3, 0x3C, (byte) 0x96, 0x69, (byte) 0x7E, (byte) 0x87, 0x11, (byte) 0xEE};
		UUID uuid = HolepunchMarker.create(custom, 0, 999);
		Optional<HolepunchMarker> decoded = HolepunchMarker.decode(custom, uuid);
		assertTrue(decoded.isPresent());
		assertEquals(999, decoded.get().nonce());
		assertEquals(123456, HolepunchMarker.decode(HolepunchMarker.create(0, 123456)).orElseThrow().nonce(), "the built-in constant must keep working");

		assertTrue(HolepunchMarker.decode(uuid).isEmpty(), "a custom-constant marker must not decode under the built-in constant");
		assertTrue(HolepunchMarker.decode(custom, HolepunchMarker.create(0, 999)).isEmpty(), "a built-in-constant marker must not decode under the custom constant");
	}

}
