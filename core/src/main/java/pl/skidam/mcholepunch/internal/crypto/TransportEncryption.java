package pl.skidam.mcholepunch.internal.crypto;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class TransportEncryption {
	private final AtomicReference<CipherStreamState> decryptor;
	private final AtomicReference<CipherStreamState> encryptor;
	private final AtomicBoolean raw = new AtomicBoolean();

	public TransportEncryption(byte[] secret, CipherStreamState initialDecryptor, CipherStreamState initialEncryptor) throws GeneralSecurityException {
		Objects.requireNonNull(secret, "secret");
		this.decryptor = new AtomicReference<>(Objects.requireNonNull(initialDecryptor, "initialDecryptor"));
		this.encryptor = new AtomicReference<>(Objects.requireNonNull(initialEncryptor, "initialEncryptor"));
	}

	public static TransportEncryption client(byte[] secret, CipherStreamState initialDecryptor, CipherStreamState initialEncryptor) throws GeneralSecurityException {
		return new TransportEncryption(secret, initialDecryptor, initialEncryptor);
	}

	public static TransportEncryption server(byte[] secret) throws GeneralSecurityException {
		return new TransportEncryption(secret, MinecraftCrypt.decryptor(secret), MinecraftCrypt.encryptor(secret));
	}

	public byte[] decrypt(byte[] input) {
		if (raw.get()) {
			return input.clone();
		}
		return decryptor.get().update(input);
	}

	public byte[] encrypt(byte[] input) {
		if (raw.get()) {
			return input.clone();
		}
		return encryptor.get().update(input);
	}

	public int decrypt(ByteBuffer input, ByteBuffer output) {
		if (raw.get()) {
			int bytes = input.remaining();
			output.put(input);
			return bytes;
		}
		return decryptor.get().update(input, output);
	}

	public int encrypt(ByteBuffer input, ByteBuffer output) {
		if (raw.get()) {
			int bytes = input.remaining();
			output.put(input);
			return bytes;
		}
		return encryptor.get().update(input, output);
	}

	public boolean isRaw() {
		return raw.get();
	}

	public void activateRaw() {
		if (raw.get()) {
			return;
		}
		raw.compareAndSet(false, true);
	}
}
