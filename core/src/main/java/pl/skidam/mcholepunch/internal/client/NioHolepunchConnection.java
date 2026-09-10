package pl.skidam.mcholepunch.internal.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.HolepunchRoute;
import pl.skidam.mcholepunch.internal.DebugLog;
import pl.skidam.mcholepunch.internal.HolepunchRequest;
import pl.skidam.mcholepunch.internal.concurrent.SerialExecutor;
import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;
import pl.skidam.mcholepunch.internal.crypto.TransportEncryption;
import pl.skidam.mcholepunch.internal.protocol.LoginCodec;
import pl.skidam.mcholepunch.internal.protocol.MinecraftFrameReader;
import pl.skidam.mcholepunch.internal.protocol.TransportFrame;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameCodec;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameReader;
import pl.skidam.mcholepunch.internal.transport.TransportFeed;
import pl.skidam.mcholepunch.internal.transport.TransportHandoff;

final class NioHolepunchConnection implements HolepunchConnection {
	private static final int TRANSFER_BUFFER_SIZE = 128 * 1024;
	private final long debugId;
	private final SocketChannel channel;
	private final HolepunchHandler handler;
	private final HolepunchRequest request;
	private final Executor callbackExecutor;
	private final byte[] transportSecret;
	private final TransportEncryption encryption;
	private final MinecraftFrameReader frameReader;
	private final TransportFeed transportFeed;
	private final ExecutorService readerExecutor;
	private final Object writeLock = new Object();
	private final AtomicBoolean closed = new AtomicBoolean();
	private final AtomicBoolean openSucceeded = new AtomicBoolean();
	private final AtomicLong receivedBytes = new AtomicLong();
	private final AtomicLong sentBytes = new AtomicLong();
	private final Object readFlow = new Object();
	private volatile boolean takeoverComplete;
	private boolean readsPaused;
	private final ScheduledFuture<?> watchdog;
	private final ByteArrayOutputStream initialData = new ByteArrayOutputStream();
	private byte[] initialDataSnapshot = new byte[0];
	private ByteBuffer encryptedOutput = ByteBuffer.allocate(8192);
	private final TransportHandoff handoff = new TransportHandoff();
	private CompletableFuture<Void> commitResult;

	private NioHolepunchConnection(long debugId, SocketChannel channel, HolepunchRequest request, ClientNegotiation.Result result) throws GeneralSecurityException {
		this.debugId = debugId;
		this.channel = channel;
		this.handler = request.handler();
		this.transportSecret = result.secret();
		this.request = request;
		this.callbackExecutor = new SerialExecutor(request.options().callbackExecutor());
		this.encryption = new TransportEncryption(result.decryptor(), MinecraftCrypt.encryptor(result.secret()));
		this.frameReader = result.frameReader();
		this.transportFeed = new TransportFeed(encryption, new TransportFrameReader(TransportFrame.MAX_FRAME_BYTES));
		this.readerExecutor = Executors.newSingleThreadExecutor(runnable -> daemon(runnable, "mcholepunch-reader"));
		this.watchdog = DebugLog.watchdog("CLIENT", debugId, this::debugState);
	}

	static NioHolepunchConnection negotiate(long debugId, SocketChannel channel, HolepunchRequest request, HolepunchRoute address) throws Exception {
		ClientNegotiation.Result result = ClientNegotiation.negotiate(debugId, channel, request, address);
		NioHolepunchConnection connection = new NioHolepunchConnection(debugId, channel, request, result);
		DebugLog.log("CLIENT", debugId, "waiting for takeover query");
		connection.awaitTakeover(result.deadline());
		channel.socket().setSoTimeout(0);
		DebugLog.log("CLIENT", debugId, "takeover complete; switching to coordinated transport initialBytes=%d", connection.initialDataSnapshot.length);
		connection.dispatchOpen();
		connection.dispatchInitialData();
		connection.readerExecutor.execute(connection::readLoop);
		return connection;
	}

	private void awaitTakeover(long deadline) throws Exception {
		ByteBuffer network = ByteBuffer.allocate(8192);
		channel.socket().setSoTimeout(remainingMillis(deadline));
		while (System.nanoTime() < deadline) {
			network.clear();
			int count;
			try {
				count = channel.read(network);
			} catch (SocketTimeoutException exception) {
				break;
			}
			if (count < 0) {
				throw new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "server closed during takeover", null);
			}
			network.flip();
			DebugLog.log("CLIENT", debugId, "takeover TCP read encryptedBytes=%d", count);
			ByteBuffer plaintext = ByteBuffer.wrap(encryption.decrypt(readRemaining(network)));
			for (ByteBuffer frame : frameReader.feed(plaintext, 1)) {
				try {
					LoginCodec.readTakeoverQuery(frame);
					DebugLog.log("CLIENT", debugId, "received takeover query frameBytes=%d", frame.position());
				} catch (IOException exception) {
					throw new HolepunchFailure(HolepunchFailure.Kind.UNEXPECTED_PACKET, "unexpected packet during takeover", exception);
				}
				ByteBuffer trailing = frameReader.drainPending();
				if (trailing.hasRemaining()) {
					processTransportPlaintext(trailing);
				}
				takeoverComplete = true;
				initialDataSnapshot = initialData.toByteArray();
				return;
			}
		}
		throw new HolepunchFailure(HolepunchFailure.Kind.TIMEOUT, "takeover timed out", null);
	}

	private static int remainingMillis(long deadline) {
		return Math.toIntExact(Math.min(Integer.MAX_VALUE, TimeUnit.NANOSECONDS.toMillis(Math.max(1, deadline - System.nanoTime()))));
	}

	private void dispatchOpen() {
		callbackExecutor.execute(() -> {
			try {
				DebugLog.log("CLIENT", debugId, "dispatch onOpen");
				handler.onOpen(this);
				openSucceeded.set(true);
				DebugLog.log("CLIENT", debugId, "onOpen completed");
			} catch (Throwable throwable) {
				fail(new HolepunchFailure(HolepunchFailure.Kind.IO, "onOpen failed", throwable));
			}
		});
	}

	private void dispatchInitialData() {
		if (initialDataSnapshot.length != 0) {
			dispatchRead(ByteBuffer.wrap(initialDataSnapshot));
			initialDataSnapshot = new byte[0];
		}
	}

	@Override
	public boolean isRaw() {
		return handoff.isRaw();
	}

	@Override
	public CompletionStage<Void> write(ByteBuffer data) {
		if (data == null) {
			return CompletableFuture.failedFuture(new NullPointerException("data"));
		}
		ByteBuffer copy = data.duplicate();
		int size = copy.remaining();
		if (size == 0) {
			return CompletableFuture.completedFuture(null);
		}
		// The blocked caller is the write backpressure: there is no queue to overflow, so the
		// old pending-write limit has no synchronous-era equivalent.
		synchronized (writeLock) {
			if (closed.get()) {
				return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection is closed", null));
			}
			if (handoff.isCommitting()) {
				return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.IO, "transport commit in flight", null));
			}
			try {
				sendFrame(handoff.isRaw() ? copy : TransportFrameCodec.data(copy), handoff.isRaw());
				DebugLog.log("CLIENT", debugId, "write complete plaintext=%d totalSent=%d", size, sentBytes.addAndGet(size));
				return CompletableFuture.completedFuture(null);
			} catch (Throwable throwable) {
				HolepunchFailure failure = new HolepunchFailure(HolepunchFailure.Kind.IO, "write failed", throwable);
				fail(failure);
				return CompletableFuture.failedFuture(failure);
			}
		}
	}

	/** Encrypts and writes one frame; the caller holds {@link #writeLock}, which confines the stateful cipher and the reuse buffer. */
	private void sendFrame(ByteBuffer frame, boolean rawEra) throws IOException {
		ensureEncryptedCapacity(frame.remaining());
		encryptedOutput.clear();
		int encryptedBytes = encryption.encrypt(frame, encryptedOutput);
		encryptedOutput.flip();
		DebugLog.log("CLIENT", debugId, "sending kind=DATA plaintext=%d wire=%d rawEra=%s", frame.limit(), encryptedBytes, rawEra);
		writeFully(channel, encryptedOutput);
	}

	/** Reads the buffer's remaining bytes into a fresh array. */
	static byte[] readRemaining(ByteBuffer buffer) {
		byte[] result = new byte[buffer.remaining()];
		buffer.get(result);
		return result;
	}

	/** Writes the buffer to the blocking channel in full. */
	static void writeFully(SocketChannel channel, ByteBuffer data) throws IOException {
		while (data.hasRemaining()) {
			channel.write(data);
		}
	}

	@Override
	public CompletionStage<Void> commitTransportUpgrade() {
		synchronized (writeLock) {
			if (closed.get()) {
				return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection is closed", null));
			}
			if (handoff.isRaw()) {
				return CompletableFuture.completedFuture(null);
			}
			if (commitResult != null) {
				return commitResult;
			}
			commitResult = new CompletableFuture<>();
			try {
				if (handoff.requestCommit()) {
					sendFrame(TransportFrameCodec.commit(), false);
					if (handoff.onCommitSent()) {
						activateRaw();
					}
				}
			} catch (Throwable throwable) {
				HolepunchFailure failure = new HolepunchFailure(HolepunchFailure.Kind.IO, "commit failed", throwable);
				fail(failure);
				return CompletableFuture.failedFuture(failure);
			}
			return commitResult;
		}
	}

	private void receiveControl() {
		synchronized (writeLock) {
			if (closed.get() || handoff.isRaw()) {
				return;
			}
			// The peer's COMMIT never schedules an outbound frame; when our own is already on
			// the wire this is the switch that activates the raw era for both directions.
			if (handoff.onPeerCommitReceived()) {
				activateRaw();
			}
		}
	}

	/** Flips the cipher and any committer to the raw era; called exactly once, when both COMMITs have crossed. */
	private void activateRaw() {
		encryption.activateRaw();
		DebugLog.log("CLIENT", debugId, "transport switched to raw pass-through");
		if (commitResult != null) commitResult.complete(null);
	}

	private void readLoop() {
		ByteBuffer network = ByteBuffer.allocate(TRANSFER_BUFFER_SIZE);
		try {
			while (!closed.get()) {
				awaitReadsEnabled();
				network.clear();
				int count = channel.read(network);
				if (count < 0) {
					fail(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "server closed connection", null));
					return;
				}
				network.flip();
				while (network.hasRemaining()) {
					if (transportFeed.isRaw()) {
						ByteBuffer raw = network.slice();
						network.position(network.limit());
						awaitRead(dispatchRead(raw, true));
						continue;
					}
					processTransportPlaintext(network);
				}
			}
		} catch (Throwable throwable) {
			fail(throwable instanceof HolepunchFailure failure ? failure : new HolepunchFailure(HolepunchFailure.Kind.IO, "read failed", throwable));
		}
	}

	private void processTransportPlaintext(ByteBuffer encryptedInput) throws IOException {
		List<CompletionStage<Void>> deliveries = new ArrayList<>(2);
		transportFeed.feed(encryptedInput, frame -> {
			switch (frame.type()) {
				case DATA -> deliveries.add(handleData(frame.payload()));
				case COMMIT -> receiveControl();
			}
		});
		for (CompletionStage<Void> delivered : deliveries) {
			awaitRead(delivered);
		}
	}

	private CompletionStage<Void> handleData(byte[] bytes) {
		long total = receivedBytes.addAndGet(bytes.length);
		if (!takeoverComplete) {
			initialData.writeBytes(bytes);
			DebugLog.log("CLIENT", debugId, "buffering initial application bytes=%d totalReceived=%d", bytes.length, total);
			return null;
		}
		DebugLog.log("CLIENT", debugId, "received application bytes=%d totalReceived=%d %s", bytes.length, total, DebugLog.preview(bytes));
		return dispatchRead(ByteBuffer.wrap(bytes));
	}

	private CompletionStage<Void> dispatchRead(ByteBuffer data) {
		return dispatchRead(data, false);
	}

	private CompletionStage<Void> dispatchRead(ByteBuffer data, boolean rawEra) {
		if (!data.hasRemaining()) {
			return CompletableFuture.completedFuture(null);
		}
		ByteBuffer copy = ByteBuffer.allocate(data.remaining());
		copy.put(data).flip();
		CompletableFuture<Void> completion = new CompletableFuture<>();
		try {
			callbackExecutor.execute(() -> {
				if (!openSucceeded.get()) {
					DebugLog.log("CLIENT", debugId, "dropping read because onOpen has not completed %s", DebugLog.preview(copy));
					completion.complete(null);
					return;
				}
				try {
					if (rawEra) {
						DebugLog.log("CLIENT", debugId, "dispatch onRawRead %s", DebugLog.preview(copy));
						handler.onRawRead(copy.asReadOnlyBuffer());
					} else {
						DebugLog.log("CLIENT", debugId, "dispatch onRead %s", DebugLog.preview(copy));
						handler.onRead(copy.asReadOnlyBuffer());
					}
					completion.complete(null);
				} catch (Throwable throwable) {
					HolepunchFailure failure = new HolepunchFailure(HolepunchFailure.Kind.IO, rawEra ? "onRawRead failed" : "onRead failed", throwable);
					completion.completeExceptionally(failure);
					fail(failure);
				}
			});
		} catch (Throwable throwable) {
			completion.completeExceptionally(throwable);
			fail(new HolepunchFailure(HolepunchFailure.Kind.IO, "read scheduling failed", throwable));
		}
		return completion;
	}

	private void awaitReadsEnabled() throws InterruptedException {
		synchronized (readFlow) {
			while (!closed.get() && readsPaused) {
				readFlow.wait();
			}
		}
	}

	private static void awaitRead(CompletionStage<Void> completion) throws IOException {
		try {
			completion.toCompletableFuture().get();
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new IOException("read callback interrupted", exception);
		} catch (Exception exception) {
			Throwable cause = exception.getCause() == null ? exception : exception.getCause();
			if (cause instanceof IOException ioException) {
				throw ioException;
			}
			throw new IOException("read callback failed", cause);
		}
	}

	@Override
	public byte[] transportSecret() {
		return transportSecret.clone();
	}

	@Override
	public void pauseReads() {
		synchronized (readFlow) {
			readsPaused = true;
		}
	}

	@Override
	public void resumeReads() {
		synchronized (readFlow) {
			if (readsPaused) {
				readsPaused = false;
				readFlow.notifyAll();
			}
		}
	}

	@Override
	public void close() {
		fail(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection closed", null));
	}

	private void fail(HolepunchFailure failure) {
		if (!closed.compareAndSet(false, true)) {
			DebugLog.log("CLIENT", debugId, "ignoring duplicate close: %s", failure);
			return;
		}
		DebugLog.failure("CLIENT", debugId, "closing connection kind=" + failure.kind(), failure);
		DebugLog.cancel(watchdog);
		synchronized (readFlow) {
			readFlow.notifyAll();
		}
		try {
			channel.close();
		} catch (IOException exception) {
			failure.addSuppressed(exception);
		}
		if (commitResult != null) commitResult.completeExceptionally(failure);
		readerExecutor.shutdownNow();
		callbackExecutor.execute(() -> handler.onClosed(failure));
	}

	private String debugState() {
		boolean paused;
		synchronized (readFlow) {
			paused = readsPaused;
		}
		return String.format(Locale.ROOT, "open=%s connected=%s closed=%s openCallback=%s phase=%s raw=%s readsPaused=%s sent=%d received=%d readerShutdown=%s", channel.isOpen(), channel.isConnected(),
				closed.get(), openSucceeded.get(), handoff.phase(), encryption.isRaw(), paused, sentBytes.get(), receivedBytes.get(), readerExecutor.isShutdown());
	}

	private static Thread daemon(Runnable runnable, String name) {
		Thread thread = new Thread(runnable, name);
		thread.setDaemon(true);
		return thread;
	}

	private void ensureEncryptedCapacity(int requiredBytes) {
		if (encryptedOutput.capacity() < requiredBytes) {
			encryptedOutput = ByteBuffer.allocate(requiredBytes);
		}
	}

}
