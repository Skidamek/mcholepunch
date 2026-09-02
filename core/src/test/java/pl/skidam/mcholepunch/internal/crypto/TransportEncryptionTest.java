package pl.skidam.mcholepunch.internal.crypto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class TransportEncryptionTest {
	@Test
	void clientAndServerShareInitialStreamThenRawBytes() throws Exception {
		byte[] secret = MinecraftCrypt.newSecret();
		TransportEncryption client = new TransportEncryption(MinecraftCrypt.decryptor(secret), MinecraftCrypt.encryptor(secret));
		TransportEncryption server = TransportEncryption.server(secret);
		byte[] clientToServer = "client-to-server initial".getBytes(StandardCharsets.UTF_8);
		byte[] serverToClient = "server-to-client initial".getBytes(StandardCharsets.UTF_8);

		assertArrayEquals(clientToServer, server.decrypt(client.encrypt(clientToServer)));
		assertArrayEquals(serverToClient, client.decrypt(server.encrypt(serverToClient)));

		client.activateRaw();
		server.activateRaw();
		assertTrue(client.isRaw());
		assertTrue(server.isRaw());
		byte[] clientRaw = "client-to-server raw".getBytes(StandardCharsets.UTF_8);
		byte[] serverRaw = "server-to-client raw".getBytes(StandardCharsets.UTF_8);
		assertArrayEquals(clientRaw, server.decrypt(client.encrypt(clientRaw)));
		assertArrayEquals(serverRaw, client.decrypt(server.encrypt(serverRaw)));
	}

	@Test
	void byteBufferPathsRoundTripInBothModes() throws Exception {
		byte[] secret = MinecraftCrypt.newSecret();
		TransportEncryption client = new TransportEncryption(MinecraftCrypt.decryptor(secret), MinecraftCrypt.encryptor(secret));
		TransportEncryption server = TransportEncryption.server(secret);
		byte[] message = "byte buffer transport".getBytes(StandardCharsets.UTF_8);

		ByteBuffer plaintext = ByteBuffer.wrap(message);
		ByteBuffer ciphertext = ByteBuffer.allocate(message.length);
		assertEquals(message.length, client.encrypt(plaintext, ciphertext));
		ciphertext.flip();
		ByteBuffer decoded = ByteBuffer.allocate(message.length);
		assertEquals(message.length, server.decrypt(ciphertext, decoded));
		decoded.flip();
		byte[] result = new byte[message.length];
		decoded.get(result);
		assertArrayEquals(message, result);

		client.activateRaw();
		ByteBuffer rawInput = ByteBuffer.wrap(message);
		ByteBuffer rawOutput = ByteBuffer.allocate(message.length);
		assertEquals(message.length, client.encrypt(rawInput, rawOutput));
		rawOutput.flip();
		byte[] rawResult = new byte[message.length];
		rawOutput.get(rawResult);
		assertArrayEquals(message, rawResult);
	}
}
