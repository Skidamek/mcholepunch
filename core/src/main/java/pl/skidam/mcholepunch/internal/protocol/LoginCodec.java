package pl.skidam.mcholepunch.internal.protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class LoginCodec {
	private static final int MAX_STRING_BYTES = 32767;
	private static final int MAX_FRAME_BYTES = 2 * 1024 * 1024;
	private static final int MAX_RSA_BLOCK_BYTES = 512;
	private static final int MAX_HOSTNAME_BYTES = 255;
	// Login-phase packet ids are shared by every Minecraft version we speak.
	private static final int HANDSHAKE_PACKET_ID = 0;
	private static final int LOGIN_START_PACKET_ID = 0;
	private static final int ENCRYPTION_REQUEST_PACKET_ID = 1;
	private static final int ENCRYPTION_RESPONSE_PACKET_ID = 1;
	private static final int CUSTOM_QUERY_PACKET_ID = 4;
	private static final int LOGIN_INTENTION = 2;

	private LoginCodec() {}

	public static ByteBuffer handshake(int protocolVersion, String host, int port) throws IOException {
		ByteArrayOutputStream packet = new ByteArrayOutputStream();
		VarInts.write(packet, HANDSHAKE_PACKET_ID);
		VarInts.write(packet, protocolVersion);
		writeString(packet, host);
		packet.write((port >>> 8) & 0xff);
		packet.write(port & 0xff);
		VarInts.write(packet, LOGIN_INTENTION);
		return MinecraftFrameWriter.frame(packet.toByteArray());
	}

	/**
	 * Login Start always uses the newest vanilla shape, name followed by the profile id, whatever
	 * Minecraft version the two ends run: the holepunch marker hides in the profile id, and vanilla
	 * never parses these frames because the holepunch server claims the connection before a login
	 * listener exists.
	 */
	public static ByteBuffer loginStart(String username, UUID profileId) throws IOException {
		ByteArrayOutputStream packet = new ByteArrayOutputStream();
		VarInts.write(packet, LOGIN_START_PACKET_ID);
		writeString(packet, username);
		writeUuid(packet, profileId);
		return MinecraftFrameWriter.frame(packet.toByteArray());
	}

	public static LoginStart readLoginStart(ByteBuffer frame) throws IOException {
		int packetId = VarInts.read(frame);
		if (packetId != LOGIN_START_PACKET_ID) {
			throw new IOException("expected Login Start, got packet " + packetId);
		}
		String username = readString(frame);
		UUID profileId = readUuid(frame);
		if (frame.hasRemaining()) {
			throw new IOException("trailing bytes in Login Start");
		}
		return new LoginStart(username, profileId);
	}

	/**
	 * Recognizes a Login Intention handshake by shape alone, without binding to any protocol
	 * version number, so the claim handler stays identical across Minecraft versions.
	 */
	public static boolean isLoginIntention(ByteBuffer frame) {
		try {
			if (VarInts.read(frame) != HANDSHAKE_PACKET_ID) return false;
			VarInts.read(frame);
			int hostnameLength = VarInts.read(frame);
			if (hostnameLength < 0 || hostnameLength > MAX_HOSTNAME_BYTES || hostnameLength > frame.remaining()) return false;
			frame.position(frame.position() + hostnameLength);
			if (frame.remaining() < 2) return false;
			frame.position(frame.position() + 2);
			return VarInts.read(frame) == LOGIN_INTENTION && !frame.hasRemaining();
		} catch (Exception exception) {
			return false;
		}
	}

	public static EncryptionRequest readEncryptionRequest(ByteBuffer frame) throws IOException {
		int packetId = VarInts.read(frame);
		if (packetId != ENCRYPTION_REQUEST_PACKET_ID) {
			throw new IOException("expected Encryption Request, got packet " + packetId);
		}
		String serverId = readString(frame);
		byte[] publicKey = readByteArray(frame, MAX_FRAME_BYTES);
		byte[] verifyToken = readByteArray(frame, MAX_RSA_BLOCK_BYTES);
		boolean shouldAuthenticate = readBoolean(frame);
		if (frame.hasRemaining()) {
			throw new IOException("trailing bytes in Encryption Request");
		}
		return new EncryptionRequest(serverId, publicKey, verifyToken, shouldAuthenticate);
	}

	public static ByteBuffer encryptionRequestPacket(byte[] publicKey, byte[] verifyToken, boolean shouldAuthenticate) throws IOException {
		ByteArrayOutputStream packet = new ByteArrayOutputStream();
		VarInts.write(packet, ENCRYPTION_REQUEST_PACKET_ID);
		writeString(packet, "");
		writeByteArray(packet, publicKey);
		writeByteArray(packet, verifyToken);
		packet.write(shouldAuthenticate ? 1 : 0);
		return ByteBuffer.wrap(packet.toByteArray());
	}

	public static EncryptionResponse readEncryptionResponse(ByteBuffer packet) throws IOException {
		int packetId = VarInts.read(packet);
		if (packetId != ENCRYPTION_RESPONSE_PACKET_ID) {
			throw new IOException("expected Encryption Response, got packet " + packetId);
		}
		byte[] encryptedSecret = readByteArray(packet, MAX_RSA_BLOCK_BYTES);
		byte[] encryptedChallenge = readByteArray(packet, MAX_RSA_BLOCK_BYTES);
		if (encryptedSecret.length == 0 || encryptedChallenge.length == 0) {
			throw new IOException("empty RSA block in Encryption Response");
		}
		if (packet.hasRemaining()) {
			throw new IOException("trailing bytes in Encryption Response");
		}
		return new EncryptionResponse(encryptedSecret, encryptedChallenge);
	}

	public static ByteBuffer encryptionResponse(byte[] encryptedSecret, byte[] encryptedChallenge) throws IOException {
		ByteArrayOutputStream packet = new ByteArrayOutputStream();
		VarInts.write(packet, ENCRYPTION_RESPONSE_PACKET_ID);
		writeByteArray(packet, encryptedSecret);
		writeByteArray(packet, encryptedChallenge);
		return MinecraftFrameWriter.frame(packet.toByteArray());
	}

	public static TakeoverQuery readTakeoverQuery(ByteBuffer frame) throws IOException {
		int packetId = VarInts.read(frame);
		if (packetId != CUSTOM_QUERY_PACKET_ID) {
			throw new IOException("expected Custom Query, got packet " + packetId);
		}
		int transactionId = VarInts.read(frame);
		String identifier = readString(frame);
		byte[] payload = new byte[frame.remaining()];
		frame.get(payload);
		if (!TakeoverQuery.IDENTIFIER.equals(identifier) || payload.length != 0) {
			throw new IOException("unexpected Custom Query");
		}
		return new TakeoverQuery(transactionId, identifier, payload);
	}

	public static ByteBuffer takeoverQueryPacket(int transactionId) throws IOException {
		ByteArrayOutputStream packet = new ByteArrayOutputStream();
		VarInts.write(packet, CUSTOM_QUERY_PACKET_ID);
		VarInts.write(packet, transactionId);
		writeString(packet, TakeoverQuery.IDENTIFIER);
		return ByteBuffer.wrap(packet.toByteArray());
	}

	public static ByteBuffer takeoverQuery(int transactionId) throws IOException {
		ByteBuffer packet = takeoverQueryPacket(transactionId);
		byte[] bytes = new byte[packet.remaining()];
		packet.get(bytes);
		return MinecraftFrameWriter.frame(bytes);
	}

	public static String readString(ByteBuffer input) throws IOException {
		int length = VarInts.read(input);
		if (length < 0 || length > MAX_STRING_BYTES || length > input.remaining()) {
			throw new IOException("invalid Minecraft string length");
		}
		byte[] value = new byte[length];
		input.get(value);
		String result = new String(value, StandardCharsets.UTF_8);
		if (result.getBytes(StandardCharsets.UTF_8).length != length) {
			throw new IOException("invalid UTF-8 string");
		}
		return result;
	}

	public static byte[] readByteArray(ByteBuffer input, int maximumLength) throws IOException {
		int length = VarInts.read(input);
		if (length < 0 || length > maximumLength || length > input.remaining()) {
			throw new IOException("invalid Minecraft byte array length");
		}
		byte[] result = new byte[length];
		input.get(result);
		return result;
	}

	private static void writeString(ByteArrayOutputStream output, String value) throws IOException {
		byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
		if (bytes.length > MAX_STRING_BYTES) {
			throw new IOException("Minecraft string is too long");
		}
		VarInts.write(output, bytes.length);
		output.write(bytes);
	}

	private static void writeByteArray(ByteArrayOutputStream output, byte[] value) {
		VarInts.write(output, value.length);
		output.writeBytes(value);
	}

	private static UUID readUuid(ByteBuffer input) throws IOException {
		if (input.remaining() < 16) {
			throw new IOException("missing UUID");
		}
		long most = 0;
		long least = 0;
		for (int shift = 56; shift >= 0; shift -= 8) {
			most = (most << 8) | (input.get() & 0xff);
		}
		for (int shift = 56; shift >= 0; shift -= 8) {
			least = (least << 8) | (input.get() & 0xff);
		}
		return new UUID(most, least);
	}

	private static void writeUuid(ByteArrayOutputStream output, UUID uuid) {
		long most = uuid.getMostSignificantBits();
		long least = uuid.getLeastSignificantBits();
		for (int shift = 56; shift >= 0; shift -= 8) {
			output.write((int) (most >>> shift) & 0xff);
		}
		for (int shift = 56; shift >= 0; shift -= 8) {
			output.write((int) (least >>> shift) & 0xff);
		}
	}

	private static boolean readBoolean(ByteBuffer input) throws IOException {
		if (!input.hasRemaining()) {
			throw new IOException("missing boolean");
		}
		int value = input.get() & 0xff;
		if (value > 1) {
			throw new IOException("invalid boolean");
		}
		return value != 0;
	}

	public record LoginStart(String username, UUID profileId) {}

	public record EncryptionRequest(String serverId, byte[] publicKey, byte[] verifyToken, boolean shouldAuthenticate) {
		public EncryptionRequest {
			publicKey = publicKey.clone();
			verifyToken = verifyToken.clone();
		}

		@Override
		public byte[] publicKey() {
			return publicKey.clone();
		}

		@Override
		public byte[] verifyToken() {
			return verifyToken.clone();
		}
	}

	public record EncryptionResponse(byte[] encryptedSecret, byte[] encryptedChallenge) {
		public EncryptionResponse {
			encryptedSecret = encryptedSecret.clone();
			encryptedChallenge = encryptedChallenge.clone();
		}

		@Override
		public byte[] encryptedSecret() {
			return encryptedSecret.clone();
		}

		@Override
		public byte[] encryptedChallenge() {
			return encryptedChallenge.clone();
		}
	}
}
