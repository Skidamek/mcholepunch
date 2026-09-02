package pl.skidam.mcholepunch.internal.crypto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class TransportEncryptionTest {
	@Test
	void clientAndServerShareInitialStreamThenRawBytes() throws Exception {
		byte[] secret = MinecraftCrypt.newSecret();
		TransportEncryption client = TransportEncryption.client(secret, MinecraftCrypt.decryptor(secret), MinecraftCrypt.encryptor(secret));
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
}
