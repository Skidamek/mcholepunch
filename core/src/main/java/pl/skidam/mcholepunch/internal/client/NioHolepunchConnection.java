package pl.skidam.mcholepunch.internal.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
	/**
	 * Bounds blocking reads during negotiation: a blocking {@link SocketChannel#read} ignores
	 * {@code SO_TIMEOUT}, so the only way to break one is to close the channel under it. One
	 * daemon thread schedules every deadline close.
	 */
	private static final ScheduledExecutorService READ_DEADLINES = Executors.newSingleThreadScheduledExecutor(runnable -> daemon(runnable, "mcholepunch-read-deadline"));
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
	private final ExecutorService writerExecutor;
	private final Queue<CompletableFuture<Void>> pendingWrites = new ConcurrentLinkedQueue<>();
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
	private final AtomicReference<CompletableFuture<Void>> commitResult = new AtomicReference<>();
	/**
	 * Guards every decision that must be atomic against the writer shutting down: a write's era
	 * check, queue entry and enqueue; a commit's store, request and enqueue; and {@link #fail}'s
	 * queue drain, commit-future completion and executor shutdown. All of these are enqueue-only
	 * operations, so the critical sections never block, and writer FIFO then keeps each task in the
	 * era its submitter saw.
	 */
	private final Object writerLock = new Object();

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
		this.writerExecutor = Executors.newSingleThreadExecutor(runnable -> daemon(runnable, "mcholepunch-writer"));
		this.watchdog = DebugLog.watchdog("CLIENT", debugId, this::debugState);
	}

	static NioHolepunchConnection negotiate(long debugId, SocketChannel channel, HolepunchRequest request, HolepunchRoute address) throws Exception {
		ClientNegotiation.Result result = ClientNegotiation.negotiate(debugId, channel, request, address);
		NioHolepunchConnection connection = new NioHolepunchConnection(debugId, channel, request, result);
		DebugLog.log("CLIENT", debugId, "waiting for takeover query");
		connection.awaitTakeover(result.deadline());
		DebugLog.log("CLIENT", debugId, "takeover complete; switching to coordinated transport initialBytes=%d", connection.initialDataSnapshot.length);
		connection.dispatchOpen();
		connection.dispatchInitialData();
		connection.readerExecutor.execute(connection::readLoop);
		return connection;
	}

	private void awaitTakeover(long deadline) throws Exception {
		ByteBuffer network = ByteBuffer.allocate(8192);
		while (true) {
			network.clear();
			int count = readWithDeadline(channel, network, deadline, "takeover timed out");
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
					// Already-decrypted leftovers of the takeover read: frames here were coalesced with
					// the query by the server, so they must not go through the cipher again.
					List<CompletionStage<Void>> deliveries = new ArrayList<>(2);
					transportFeed.feedPlaintext(trailing, trailingFrame -> collectDelivery(trailingFrame, deliveries));
					awaitDeliveries(deliveries);
				}
				takeoverComplete = true;
				initialDataSnapshot = initialData.toByteArray();
				return;
			}
		}
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
		CompletableFuture<Void> result = new CompletableFuture<>();
		// Under the writer lock so the era check, the queue entry and the enqueue are one unit: a
		// write is either rejected up front or its task lands on the writer before the COMMIT task,
		// which keeps the framing and the cipher in the era this caller saw.
		synchronized (writerLock) {
			if (closed.get()) {
				return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection is closed", null));
			}
			if (handoff.isCommitting()) {
				return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.IO, "transport commit in flight", null));
			}
			pendingWrites.add(result);
			try {
				// The blocking socket write runs on the writer thread so the returned future completes only
				// once the bytes reached the wire; a caller bounding its wait (HolepunchSocket does) gets a
				// deadline that can actually fire instead of a synchronous write the bound never observes.
				writerExecutor.execute(() -> deliverWrite(copy, size, result));
			} catch (RejectedExecutionException rejected) {
				pendingWrites.remove(result);
				result.completeExceptionally(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection is closed", null));
			}
		}
		return result;
	}

	private void deliverWrite(ByteBuffer data, int size, CompletableFuture<Void> result) {
		try {
			if (closed.get()) {
				result.completeExceptionally(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection is closed", null));
				return;
			}
			if (handoff.isRaw()) {
				sendFrame(data);
			} else {
				// The peer rejects frames above the cap, so one write is split into the frames it will
				// accept instead of succeeding here and being dropped there.
				while (data.hasRemaining()) {
					int chunkBytes = Math.min(data.remaining(), TransportFrame.MAX_FRAME_BYTES);
					ByteBuffer chunk = data.slice();
					chunk.limit(chunkBytes);
					sendFrame(TransportFrameCodec.data(chunk));
					data.position(data.position() + chunkBytes);
				}
			}
			DebugLog.log("CLIENT", debugId, "write complete plaintext=%d totalSent=%d", size, sentBytes.addAndGet(size));
			result.complete(null);
		} catch (Throwable throwable) {
			HolepunchFailure failure = new HolepunchFailure(HolepunchFailure.Kind.IO, "write failed", throwable);
			result.completeExceptionally(failure);
			fail(failure);
		} finally {
			pendingWrites.remove(result);
		}
	}

	/** Encrypts and writes one frame; runs only on the writer thread, which confines the stateful cipher and the reuse buffer. */
	private void sendFrame(ByteBuffer frame) throws IOException {
		ensureEncryptedCapacity(frame.remaining());
		encryptedOutput.clear();
		int encryptedBytes = encryption.encrypt(frame, encryptedOutput);
		encryptedOutput.flip();
		DebugLog.log("CLIENT", debugId, "sending kind=DATA plaintext=%d wire=%d rawEra=%s", frame.limit(), encryptedBytes, handoff.isRaw());
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

	/**
	 * Blocking read bounded by the deadline: the deadline task closes the channel, which unblocks
	 * the read as an asynchronous close, and that close maps to TIMEOUT. Runs the read on the
	 * caller's thread; only the deadline close comes from the shared scheduler.
	 */
	static int readWithDeadline(SocketChannel channel, ByteBuffer destination, long deadlineNanos, String timeoutMessage) throws IOException {
		long remainingNanos = deadlineNanos - System.nanoTime();
		if (remainingNanos <= 0) {
			throw new HolepunchFailure(HolepunchFailure.Kind.TIMEOUT, timeoutMessage, null);
		}
		AtomicBoolean timedOut = new AtomicBoolean();
		ScheduledFuture<?> deadlineTask = READ_DEADLINES.schedule(() -> {
			// The deadline and the read race to claim the outcome: the task closes the channel only
			// when it wins the claim, so a read that returned first can never lose its connection to
			// a close that started after the data was already in hand.
			if (timedOut.compareAndSet(false, true)) {
				closeQuietly(channel);
			}
		}, remainingNanos, TimeUnit.NANOSECONDS);
		try {
			int count = channel.read(destination);
			// The read claims the outcome too: if the deadline task already claimed it, the channel
			// is closed and whatever arrived is past the budget. A success claimed here keeps the
			// channel alive even when a delayed scheduler has not fired the task yet; the wall-clock
			// check keeps that late success from slipping past the deadline.
			if (!timedOut.compareAndSet(false, true) || System.nanoTime() >= deadlineNanos) {
				throw new HolepunchFailure(HolepunchFailure.Kind.TIMEOUT, timeoutMessage, null);
			}
			return count;
		} catch (ClosedChannelException exception) {
			// Mid-read the close surfaces as AsynchronousCloseException (a subclass); between reads as
			// ClosedChannelException. Only the deadline task closes a negotiating channel, so past the
			// deadline both mean the read ended because the negotiation budget ran out.
			if (System.nanoTime() >= deadlineNanos) {
				throw new HolepunchFailure(HolepunchFailure.Kind.TIMEOUT, timeoutMessage, exception);
			}
			throw exception;
		} finally {
			deadlineTask.cancel(false);
		}
	}

	private static void closeQuietly(SocketChannel channel) {
		try {
			channel.close();
		} catch (IOException ignored) {
		}
	}

	@Override
	public CompletionStage<Void> commitTransportUpgrade() {
		synchronized (writerLock) {
			return beginCommit();
		}
	}

	private CompletionStage<Void> beginCommit() {
		if (closed.get()) {
			return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection is closed", null));
		}
		if (handoff.isRaw()) {
			return CompletableFuture.completedFuture(null);
		}
		CompletableFuture<Void> existing = commitResult.get();
		if (existing != null) {
			return existing;
		}
		CompletableFuture<Void> result = new CompletableFuture<>();
		if (!commitResult.compareAndSet(null, result)) {
			return commitResult.get();
		}
		if (handoff.requestCommit()) {
			// Writer-queue FIFO is what keeps the eras exact: every write enqueued before the COMMIT
			// task carries an encrypted frame, everything after the flip goes out raw.
			try {
				writerExecutor.execute(() -> {
					try {
						if (closed.get()) {
							// fail() may have run before the future landed in commitResult; every exit
							// from this task must complete it so a committer never waits forever.
							result.completeExceptionally(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection is closed", null));
							return;
						}
						sendFrame(TransportFrameCodec.commit());
						if (handoff.onCommitSent()) {
							activateRaw();
						}
					} catch (Throwable throwable) {
						fail(new HolepunchFailure(HolepunchFailure.Kind.IO, "commit failed", throwable));
					}
				});
			} catch (RejectedExecutionException rejected) {
				// fail() earlier in this race saw no commit future to complete (it was stored after its
				// check) and already closed the connection, so finish this future here.
				HolepunchFailure failure = new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection is closed", null);
				result.completeExceptionally(failure);
				fail(failure);
			}
		}
		return result;
	}

	private void receiveControl() {
		if (closed.get() || handoff.isRaw()) {
			return;
		}
		// The peer's COMMIT never schedules an outbound frame; when our own is already on
		// the wire this is the switch that activates the raw era for both directions.
		if (handoff.onPeerCommitReceived()) {
			activateRaw();
		}
	}

	/** Flips the cipher and any committer to the raw era; called exactly once, when both COMMITs have crossed. */
	private void activateRaw() {
		encryption.activateRaw();
		DebugLog.log("CLIENT", debugId, "transport switched to raw pass-through");
		CompletableFuture<Void> result = commitResult.get();
		if (result != null) result.complete(null);
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
		transportFeed.feed(encryptedInput, frame -> collectDelivery(frame, deliveries));
		awaitDeliveries(deliveries);
	}

	private void collectDelivery(TransportFrame frame, List<CompletionStage<Void>> deliveries) {
		switch (frame.type()) {
			// Data that arrives before the takeover completed is buffered for the post-open dispatch,
			// which handleData reports with a null delivery: nothing to await yet.
			case DATA -> {
				CompletionStage<Void> delivered = handleData(frame.payload());
				if (delivered != null) {
					deliveries.add(delivered);
				}
			}
			case COMMIT -> receiveControl();
		}
	}

	private void awaitDeliveries(List<CompletionStage<Void>> deliveries) throws IOException {
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
		// The writer lock owns the removal, not the completion: draining the queues and shutting the
		// writer down under it keeps a racing write either drained or rejected up front, while the
		// futures themselves are completed after the lock because completeExceptionally runs user
		// callbacks synchronously and those must never run under the lock.
		List<CompletableFuture<Void>> abandoned = new ArrayList<>();
		CompletableFuture<Void> commit;
		synchronized (writerLock) {
			CompletableFuture<Void> pending;
			while ((pending = pendingWrites.poll()) != null) {
				abandoned.add(pending);
			}
			commit = commitResult.get();
			writerExecutor.shutdownNow();
		}
		readerExecutor.shutdownNow();
		for (CompletableFuture<Void> abandonedWrite : abandoned) {
			abandonedWrite.completeExceptionally(failure);
		}
		if (commit != null) {
			commit.completeExceptionally(failure);
		}
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
