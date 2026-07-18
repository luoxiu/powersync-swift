/// Dispatches events to a number of listeners as an ``AsyncStream``.
///
/// `package` so PowerSyncGRDB can multicast `tableUpdates` the same way the
/// non-GRDB pool does — a plain `AsyncStream` is single-consumer.
///
/// Bear Days / threetwo fork note: without multicast, AttachmentQueue `watch`
/// steals uni-cast AsyncStream events from the CRUD upload loop.
package final class BroadcastStream<T: Sendable>: Sendable {
    package init() {}

    private let listeners: Mutex<Set<BroadcastStreamListener<T>>> = Mutex([])

    private func register(continuation: AsyncStream<T>.Continuation) {
        let listener = BroadcastStreamListener(continuation: continuation)
        let _ = listeners.withLock { $0.insert(listener) }

        continuation.onTermination = { @Sendable [weak self] _ in
            let _ = self?.listeners.withLock {
                $0.remove(listener)
            }
        }
    }

    package func dispatch(event: T) {
        let listeners = self.listeners.withLock { Array($0) }
        for listener in listeners {
            listener.continuation.yield(event)
        }
    }

    package func subscribe(
        bufferingPolicy: AsyncStream<T>.Continuation.BufferingPolicy = .unbounded,
        addInitial: T? = nil
    ) -> AsyncStream<T> {
        return AsyncStream(bufferingPolicy: bufferingPolicy) { continuation in
            if let addInitial {
                continuation.yield(addInitial)
            }
            
            self.register(continuation: continuation)
        }
    }
}

final private class BroadcastStreamListener<T>: Sendable, Hashable {
    let continuation: AsyncStream<T>.Continuation
    init(continuation: AsyncStream<T>.Continuation) {
        self.continuation = continuation
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(ObjectIdentifier(self))
    }

    static func == (lhs: BroadcastStreamListener<T>, rhs: BroadcastStreamListener<T>) -> Bool {
        lhs === rhs
    }
}
