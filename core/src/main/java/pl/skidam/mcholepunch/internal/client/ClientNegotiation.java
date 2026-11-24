package pl.skidam.mcholepunch.internal.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.PublicKey;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.HolepunchOptions;
import pl.skidam.mcholepunch.HolepunchRoute;
import pl.skidam.mcholepunch.internal.DebugLog;
import pl.skidam.mcholepunch.internal.HolepunchRequest;
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
		byte[] markerConstant = options.markerConstant().orElseGet(HolepunchMarker::defaultMarkerConstant);
		UUID marker = HolepunchMarker.create(markerConstant, 0, ThreadLocalRandom.current().nextInt(1 << 26));
		ByteBuffer handshake = LoginCodec.handshake(request.protocolVersion(), address.handshakeHost() + options.handshakeHostSuffix(), address.handshakePort());
		DebugLog.log("CLIENT", debugId, "send handshake %s", DebugLog.preview(handshake));
		NioHolepunchConnection.writeFully(channel, handshake);
		ByteBuffer loginStart = LoginCodec.loginStart(username, marker);
		DebugLog.log("CLIENT", debugId, "send login start username=%s marker=%s %s", username, marker, DebugLog.preview(loginStart));
		NioHolepunchConnection.writeFully(channel, loginStart);

		MinecraftFrameReader reader = new MinecraftFrameReader(LoginCodec.MAX_FRAME_BYTES);
		ByteBuffer network = ByteBuffer.allocate(8192);
		long deadline = System.nanoTime() + options.negotiationTimeout().toNanos();
		while (true) {
			network.clear();
			// A blocking SocketChannel read ignores SO_TIMEOUT, so the deadline is enforced by closing
			// the channel at the deadline rather than by a socket option.
			int count = NioHolepunchConnection.readWithDeadline(channel, network, deadline, "negotiation timed out");
			if (count < 0) {
				throw new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "server closed during negotiation", null);
			}
			network.flip();
			DebugLog.log("CLIENT", debugId, "negotiation TCP read bytes=%d", count);
			for (ByteBuffer frame : reader.feed(network, 1)) {
				LoginCodec.EncryptionRequest encryption;
				try {
					encryption = LoginCodec.readEncryptionRequest(frame);
				} catch (IOException exception) {
					throw new HolepunchFailure(HolepunchFailure.Kind.UNEXPECTED_PACKET, "unexpected packet during encryption negotiation", exception);
				}
				DebugLog.log("CLIENT", debugId, "received encryption request publicKey=%d verifyToken=%d shouldAuthenticate=%s", encryption.publicKey().length, encryption.verifyToken().length,
						encryption.shouldAuthenticate());
				if (encryption.shouldAuthenticate()) {
					throw new HolepunchFailure(HolepunchFailure.Kind.AUTHENTICATION_REQUESTED, "server requested Mojang authentication", null);
				}
				PublicKey key = MinecraftCrypt.decodePublicKey(encryption.publicKey());
				byte[] secret = MinecraftCrypt.newSecret();
				byte[] encryptedSecret = MinecraftCrypt.rsaEncrypt(key, secret);
				byte[] encryptedToken = MinecraftCrypt.rsaEncrypt(key, encryption.verifyToken());
				ByteBuffer response = LoginCodec.encryptionResponse(encryptedSecret, encryptedToken);
				DebugLog.log("CLIENT", debugId, "send encryption response %s", DebugLog.preview(response));
				NioHolepunchConnection.writeFully(channel, response);
				CipherStreamState decryptor = MinecraftCrypt.decryptor(secret);
				DebugLog.log("CLIENT", debugId, "encryption negotiation complete");
				return new Result(secret, decryptor, reader, deadline);
			}
		}
	}

	record Result(byte[] secret, CipherStreamState decryptor, MinecraftFrameReader frameReader, long deadline) {}
}
