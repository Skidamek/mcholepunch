package pl.skidam.mcholepunch.internal.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import pl.skidam.mcholepunch.HolepunchOptions;

class LoginCodecTest {
	@Test
	void parsesSplitAndCoalescedFrames() throws IOException {
		ByteBuffer handshake = LoginCodec.handshake(776, "example.test", 25565);
		ByteBuffer login = LoginCodec.loginStart("Holepunch", UUID.randomUUID());
		ByteBuffer combined = ByteBuffer.allocate(handshake.remaining() + login.remaining());
		combined.put(handshake).put(login).flip();
		MinecraftFrameReader reader = new MinecraftFrameReader(4096);
		List<ByteBuffer> first = reader.feed(combined.slice(0, 1));
		assertEquals(0, first.size());
		List<ByteBuffer> frames = reader.feed(combined.slice(1, combined.remaining() - 1));
		assertEquals(2, frames.size());
		assertEquals(0, VarInts.read(frames.get(0)));
		assertEquals(0, VarInts.read(frames.get(1)));
	}

	@Test
	void handshakeClaimsTheExactVanillaProtocolVersion() throws IOException {
		String host = "play.example.com" + HolepunchOptions.FORGE_FML3_HANDSHAKE_HOST_SUFFIX;
		ByteBuffer packet = new MinecraftFrameReader(4096).feed(LoginCodec.handshake(763, host, 25565)).get(0);
		assertEquals(0, VarInts.read(packet));
		assertEquals(763, VarInts.read(packet));
		assertEquals(host, LoginCodec.readString(packet));
		assertEquals(25565, ((packet.get() & 0xff) << 8) | (packet.get() & 0xff));
		assertEquals(2, VarInts.read(packet));
		assertFalse(packet.hasRemaining());
	}

	@Test
	void loginStartIsAlwaysNameFollowedByProfileId() throws IOException {
		UUID marker = HolepunchMarker.create(0, 99);
		ByteBuffer packet = new MinecraftFrameReader(4096).feed(LoginCodec.loginStart("Player", marker)).get(0);
		assertEquals(0, VarInts.read(packet));
		assertEquals("Player", LoginCodec.readString(packet));
		UUID profileId = new UUID(packet.getLong(), packet.getLong());
		assertEquals(marker, profileId);
		assertTrue(HolepunchMarker.decode(profileId).isPresent());
		assertFalse(packet.hasRemaining());
	}

	@Test
	void rejectsVanillaLoginStartWithoutProfileId() {
		// Vanilla clients up to 1.18.2 send only the name; failing the read is what makes the claim
		// handler replay those logins to vanilla untouched.
		assertThrows(IOException.class, () -> LoginCodec.readLoginStart(unframedLoginStart("Vanilla", false, null)));
	}

	@Test
	void rejectsVanillaLoginStartWithAbsentProfileId() {
		// Vanilla 1.19.1 to 1.20.1 send a presence byte before an optional profile id; the dialect
		// has no presence byte, so those frames must fail the read and replay to vanilla.
		assertThrows(IOException.class, () -> LoginCodec.readLoginStart(unframedLoginStart("Vanilla", true, null)));
	}

	@Test
	void rejectsTruncatedLoginStart() throws IOException {
		ByteBuffer packet = unframedLoginStart("Holepunch", false, UUID.randomUUID());
		byte[] bytes = new byte[packet.remaining()];
		packet.duplicate().get(bytes);
		assertThrows(IOException.class, () -> LoginCodec.readLoginStart(ByteBuffer.wrap(bytes, 0, bytes.length - 1)));
	}

	@Test
	void recognizesOnlyLoginIntentions() throws IOException {
		assertTrue(LoginCodec.isLoginIntention(new MinecraftFrameReader(4096).feed(LoginCodec.handshake(758, "localhost", 25565)).get(0)));
		assertFalse(LoginCodec.isLoginIntention(statusHandshake()));
		assertFalse(LoginCodec.isLoginIntention(ByteBuffer.wrap(new byte[]{13, 37})));
	}

	@Test
	void parsesAuthenticationFlag() throws IOException {
		assertTrue(LoginCodec.readEncryptionRequest(encryptionRequestFrame(true)).shouldAuthenticate());
		assertFalse(LoginCodec.readEncryptionRequest(encryptionRequestFrame(false)).shouldAuthenticate());
	}

	@Test
	void rejectsEncryptionRequestWithoutAuthenticationFlag() {
		// The negotiation dialect always carries the authentication flag, like the newest vanilla;
		// a flag-less frame is not a holepunch encryption request.
		ByteArrayOutputStream packet = new ByteArrayOutputStream();
		VarInts.write(packet, 1);
		VarInts.write(packet, 0);
		writeByteArray(packet, new byte[]{1, 2, 3});
		writeByteArray(packet, new byte[]{4, 5, 6, 7});
		assertThrows(IOException.class, () -> LoginCodec.readEncryptionRequest(ByteBuffer.wrap(packet.toByteArray())));
	}

	@Test
	void encodesUnframedEncryptionRequestPacket() throws IOException {
		ByteBuffer packet = LoginCodec.encryptionRequestPacket(new byte[]{1, 2, 3}, new byte[]{4, 5, 6, 7}, false);

		LoginCodec.EncryptionRequest request = LoginCodec.readEncryptionRequest(packet);
		assertEquals(3, request.publicKey().length);
		assertEquals(4, request.verifyToken().length);
		assertFalse(request.shouldAuthenticate());
	}

	@Test
	void parsesEncryptionResponseAndDefensivelyCopiesBlocks() throws IOException {
		byte[] encryptedSecret = {1, 2, 3};
		byte[] encryptedChallenge = {4, 5, 6};
		ByteArrayOutputStream packet = new ByteArrayOutputStream();
		VarInts.write(packet, 1);
		writeByteArray(packet, encryptedSecret);
		writeByteArray(packet, encryptedChallenge);

		LoginCodec.EncryptionResponse response = LoginCodec.readEncryptionResponse(ByteBuffer.wrap(packet.toByteArray()));
		encryptedSecret[0] = 9;
		encryptedChallenge[0] = 9;
		assertArrayEquals(new byte[]{1, 2, 3}, response.encryptedSecret());
		assertArrayEquals(new byte[]{4, 5, 6}, response.encryptedChallenge());
		byte[] returned = response.encryptedSecret();
		returned[0] = 9;
		assertArrayEquals(new byte[]{1, 2, 3}, response.encryptedSecret());
	}

	@Test
	void rejectsMalformedEncryptionResponses() {
		ByteArrayOutputStream oversized = new ByteArrayOutputStream();
		VarInts.write(oversized, 1);
		VarInts.write(oversized, 513);
		assertThrows(IOException.class, () -> LoginCodec.readEncryptionResponse(ByteBuffer.wrap(oversized.toByteArray())));
		assertThrows(IOException.class, () -> LoginCodec.readEncryptionResponse(ByteBuffer.wrap(new byte[]{2})));
		assertThrows(IOException.class, () -> LoginCodec.readEncryptionResponse(ByteBuffer.wrap(new byte[]{1, 1})));
		assertThrows(IOException.class, () -> LoginCodec.readEncryptionResponse(ByteBuffer.wrap(new byte[]{1, 1, 1, 1, 2, 0})));
	}

	@Test
	void encodesUnframedTakeoverQueryPacket() throws IOException {
		ByteBuffer packet = LoginCodec.takeoverQueryPacket(42);

		TakeoverQuery query = LoginCodec.readTakeoverQuery(packet);
		assertEquals(42, query.transactionId());
		assertEquals(TakeoverQuery.IDENTIFIER, query.identifier());
	}

	private static ByteBuffer unframedLoginStart(String username, boolean presenceByte, UUID profileId) throws IOException {
		ByteArrayOutputStream packet = new ByteArrayOutputStream();
		VarInts.write(packet, 0);
		byte[] name = username.getBytes(StandardCharsets.UTF_8);
		VarInts.write(packet, name.length);
		packet.writeBytes(name);
		if (presenceByte) {
			packet.write(0);
		}
		if (profileId != null) {
			long most = profileId.getMostSignificantBits();
			long least = profileId.getLeastSignificantBits();
			for (int shift = 56; shift >= 0; shift -= 8) {
				packet.write((int) (most >>> shift) & 0xff);
			}
			for (int shift = 56; shift >= 0; shift -= 8) {
				packet.write((int) (least >>> shift) & 0xff);
			}
		}
		return ByteBuffer.wrap(packet.toByteArray());
	}

	private static ByteBuffer statusHandshake() {
		ByteArrayOutputStream packet = new ByteArrayOutputStream();
		VarInts.write(packet, 0);
		VarInts.write(packet, 758);
		byte[] host = "localhost".getBytes(StandardCharsets.UTF_8);
		VarInts.write(packet, host.length);
		packet.writeBytes(host);
		packet.write((25565 >>> 8) & 0xff);
		packet.write(25565 & 0xff);
		VarInts.write(packet, 1); // Next state: status.
		return ByteBuffer.wrap(packet.toByteArray());
	}

	private static ByteBuffer encryptionRequestFrame(boolean shouldAuthenticate) {
		ByteArrayOutputStream packet = new ByteArrayOutputStream();
		VarInts.write(packet, 1);
		VarInts.write(packet, 0); // Empty server id.
		writeByteArray(packet, new byte[]{1, 2, 3});
		writeByteArray(packet, new byte[]{4, 5, 6, 7});
		packet.write(shouldAuthenticate ? 1 : 0);
		return ByteBuffer.wrap(packet.toByteArray());
	}

	private static void writeByteArray(ByteArrayOutputStream output, byte[] value) {
		VarInts.write(output, value.length);
		output.writeBytes(value);
	}
}
