package pl.skidam.mcholepunch.internal.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

}
