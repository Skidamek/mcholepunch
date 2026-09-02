package pl.skidam.mcholepunch.internal.client;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.PublicKey;
import java.util.UUID;

import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.HolepunchOptions;
import pl.skidam.mcholepunch.HolepunchRequest;
import pl.skidam.mcholepunch.HolepunchRoute;
import pl.skidam.mcholepunch.internal.DebugLog;
import pl.skidam.mcholepunch.internal.Usernames;
import pl.skidam.mcholepunch.internal.crypto.CipherStreamState;
import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.protocol.HolepunchMarker;
import pl.skidam.mcholepunch.internal.protocol.LoginCodec;
import pl.skidam.mcholepunch.internal.protocol.MinecraftFrameReader;

final class ClientNegotiation {
	private ClientNegotiation() {}

	static Result negotiate(long debugId, SocketChannel channel, HolepunchRequest request, HolepunchRoute address) throws Exception {
		HolepunchOptions options = request.options();
		String username = Usernames.usernameOrDefault(options);
		UUID marker = HolepunchMarker.random(0);
		ByteBuffer handshake = LoginCodec.handshake(request.protocolVersion(), address.host() + options.handshakeHostSuffix(), address.port());
		DebugLog.log("CLIENT", debugId, "send handshake %s", DebugLog.preview(handshake));
		writeFully(channel, handshake);
		ByteBuffer loginStart = LoginCodec.loginStart(username, marker);
		DebugLog.log("CLIENT", debugId, "send login start username=%s marker=%s %s", username, marker, DebugLog.preview(loginStart));
		writeFully(channel, loginStart);

		MinecraftFrameReader reader = new MinecraftFrameReader(LoginCodec.MAX_FRAME_BYTES);
		CipherStreamState decryptor = null;
		ByteBuffer network = ByteBuffer.allocate(8192);
		long deadline = System.nanoTime() + options.negotiationTimeout().toNanos();
		channel.socket().setSoTimeout(Math.toIntExact(options.negotiationTimeout().toMillis()));
		while (System.nanoTime() < deadline) {
			network.clear();
			int count;
			try {
				count = channel.read(network);
			} catch (SocketTimeoutException exception) {
				throw failure(HolepunchFailure.Kind.TIMEOUT, "negotiation timed out", exception);
			}
			if (count < 0) {
				throw failure(HolepunchFailure.Kind.CLOSED, "server closed during negotiation", null);
			}
			network.flip();
			DebugLog.log("CLIENT", debugId, "negotiation TCP read bytes=%d encrypted=%s", count, decryptor != null);
			if (decryptor != null) {
				network = ByteBuffer.wrap(decryptor.update(readRemaining(network)));
			}
			for (ByteBuffer frame : reader.feed(network, 1)) {
				if (decryptor == null) {
					LoginCodec.EncryptionRequest encryption;
					try {
						encryption = LoginCodec.readEncryptionRequest(frame);
					} catch (IOException exception) {
						throw failure(HolepunchFailure.Kind.UNEXPECTED_PACKET, "unexpected packet during encryption negotiation", exception);
					}
					DebugLog.log("CLIENT", debugId, "received encryption request publicKey=%d verifyToken=%d shouldAuthenticate=%s", encryption.publicKey().length, encryption.verifyToken().length,
							encryption.shouldAuthenticate());
					if (encryption.shouldAuthenticate()) {
						throw failure(HolepunchFailure.Kind.AUTHENTICATION_REQUESTED, "server requested Mojang authentication", null);
					}
					PublicKey key = MinecraftCrypt.decodePublicKey(encryption.publicKey());
					byte[] secret = MinecraftCrypt.newSecret();
					byte[] encryptedSecret = MinecraftCrypt.rsaEncrypt(key, secret);
					byte[] encryptedToken = MinecraftCrypt.rsaEncrypt(key, encryption.verifyToken());
					ByteBuffer response = LoginCodec.encryptionResponse(encryptedSecret, encryptedToken);
					DebugLog.log("CLIENT", debugId, "send encryption response %s", DebugLog.preview(response));
					writeFully(channel, response);
					decryptor = MinecraftCrypt.decryptor(secret);
					DebugLog.log("CLIENT", debugId, "encryption negotiation complete");
					return new Result(secret, decryptor, reader, deadline);
				}
			}
		}
		throw failure(HolepunchFailure.Kind.TIMEOUT, "negotiation timed out", null);
	}

	static byte[] readRemaining(ByteBuffer buffer) {
		byte[] result = new byte[buffer.remaining()];
		buffer.get(result);
		return result;
	}

	static void writeFully(SocketChannel channel, ByteBuffer data) throws IOException {
		while (data.hasRemaining()) {
			channel.write(data);
		}
	}

	static HolepunchFailure failure(HolepunchFailure.Kind kind, String message, Throwable cause) {
		return new HolepunchFailure(kind, message, cause);
	}

	record Result(byte[] secret, CipherStreamState decryptor, MinecraftFrameReader frameReader, long deadline) {}
}
