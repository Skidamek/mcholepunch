package pl.skidam.mcholepunch.server.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.internal.crypto.MinecraftCrypt;

class NettyHolepunchConnectionTest {
	@Test
	void deliversOpenBeforeImmediatelyReceivedApplicationData() {
		ReorderingExecutor callbacks = new ReorderingExecutor();
		List<String> events = new ArrayList<>();
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, callbacks, new HolepunchHandler() {
			@Override
			public void onOpen(HolepunchConnection connection) {
				events.add("open");
			}

			@Override
			public void onRead(ByteBuffer data) {
				events.add("read:" + data.get());
			}
		}, 1024, null);
		channel.pipeline().addLast(new NettyHolepunchConnection.RawInboundHandler(connection));

		connection.open();
		channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{42}));

		// A plain concurrent executor would now contain independent open and read tasks,
		// allowing the read to run first. The serial wrapper submits only the open task.
		assertEquals(1, callbacks.pendingTasks());
		callbacks.runNewest();
		callbacks.runNewest();

		assertEquals(List.of("open", "read:42"), events);
		channel.finishAndReleaseAll();
	}

	@Test
	void pausesNettyAutoReadUntilTheApplicationResumesIt() {
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyHolepunchConnection[] holder = new NettyHolepunchConnection[1];
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, Runnable::run, new HolepunchHandler() {
			@Override
			public void onRead(ByteBuffer data) {
				holder[0].pauseReads();
			}
		}, 1024, null);
		holder[0] = connection;
		channel.pipeline().addLast(new NettyHolepunchConnection.RawInboundHandler(connection));

		channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{42}));

		assertEquals(false, channel.config().isAutoRead());
		connection.resumeReads();
		assertEquals(true, channel.config().isAutoRead());
		channel.finishAndReleaseAll();
	}

	@Test
	void queuesInboundChunksAlreadyDecodedBeforeAutoReadStops() {
		ReorderingExecutor callbacks = new ReorderingExecutor();
		List<Integer> reads = new ArrayList<>();
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, callbacks, new HolepunchHandler() {
			@Override
			public void onRead(ByteBuffer data) {
				reads.add(data.get() & 0xff);
			}
		}, 1024, null);
		channel.pipeline().addLast(new NettyHolepunchConnection.RawInboundHandler(connection));

		channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{1}), Unpooled.wrappedBuffer(new byte[]{2}));

		callbacks.runNewest();
		callbacks.runNewest();

		assertEquals(List.of(1, 2), reads);
		channel.finishAndReleaseAll();
	}

	@Test
	void rejectsWritesWhileTransportUpgradeIsInProgress() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel();
		NettyEncryption encryption = new NettyEncryption(MinecraftCrypt.newSecret());
		NettyHolepunchConnection connection = new NettyHolepunchConnection(channel, Runnable::run, new HolepunchHandler() {
		}, 1024, encryption);

		connection.prepareTransportUpgrade();
		channel.runPendingTasks();
		CompletionStage<Void> write = connection.write(ByteBuffer.wrap(new byte[]{1}));
		channel.runPendingTasks();

		assertThrows(ExecutionException.class, () -> write.toCompletableFuture().get(1, TimeUnit.SECONDS));
		channel.finishAndReleaseAll();
	}

	private static final class ReorderingExecutor implements Executor {
		private final Deque<Runnable> tasks = new ArrayDeque<>();

		@Override
		public void execute(Runnable command) {
			tasks.addLast(command);
		}

		int pendingTasks() {
			return tasks.size();
		}

		void runNewest() {
			tasks.removeLast().run();
		}
	}
}
