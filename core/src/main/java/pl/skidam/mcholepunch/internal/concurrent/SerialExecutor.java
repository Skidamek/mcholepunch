package pl.skidam.mcholepunch.internal.concurrent;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Executor;

public final class SerialExecutor implements Executor {
	private final Executor delegate;
	private final Queue<Runnable> tasks = new ArrayDeque<>();
	private Runnable active;

	public SerialExecutor(Executor delegate) {
		this.delegate = Objects.requireNonNull(delegate, "delegate");
	}

	@Override
	public synchronized void execute(Runnable command) {
		Objects.requireNonNull(command, "command");
		tasks.add(() -> {
			try {
				command.run();
			} finally {
				scheduleNext();
			}
		});
		if (active == null) {
			scheduleNext();
		}
	}

	private synchronized void scheduleNext() {
		active = tasks.poll();
		if (active != null) {
			delegate.execute(active);
		}
	}
}
