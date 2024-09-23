package dijkstra

import java.util.*
import java.util.concurrent.Phaser
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.Comparator
import kotlin.concurrent.thread

private val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 ->
    o1 ?: return@Comparator -1
    o2 ?: return@Comparator 1
    o1.distance.compareTo(o2.distance)
}

// Returns `Integer.MAX_VALUE` if a path has not been found.
fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    start.distance = 0
    // Create a priority (by distance) queue and add the start node into it
    val q = ConcurrentMultiPriorityQueue(workers * 2)
    q.add(start)
    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker
    val threadsProcessingNodes = AtomicInteger(0)
    repeat(workers) {
        thread {
            while (q.processedNodes.get() != 0 || threadsProcessingNodes.get() != 0) {
                val cur: Node = q.poll() ?: continue
                try {
                    threadsProcessingNodes.incrementAndGet()
                    for (e in cur.outgoingEdges) {
                        val curDist = synchronized(cur) {
                            cur.distance
                        }
                        synchronized(e.to) {
                            if (e.to.distance > curDist + e.weight) {
                                e.to.distance = curDist + e.weight
                                q.add(e.to)
                            }
                        }
                    }
                } finally {
                    threadsProcessingNodes.decrementAndGet()
                }
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
}

class ConcurrentMultiPriorityQueue(queuesCount: Int) {

    val processedNodes = AtomicInteger(0)

    private val queues: MutableList<PriorityQueue<Node>> = mutableListOf()

    private val queuesLocks: MutableList<Lock> = mutableListOf()

    init {
        repeat(queuesCount) {
            queues.add(PriorityQueue<Node>(NODE_DISTANCE_COMPARATOR))
            queuesLocks.add(ReentrantLock())
        }
    }

    fun poll(): Node? {
        val (firstQueue, firstIndex) = getRandomQueueWithOrderIndex()
        val (secondQueue, secondIndex) = getRandomQueueWithOrderIndex()

        val firstQueueLock = queuesLocks[firstIndex]
        val secondQueueLock = queuesLocks[secondIndex]

        var resultNode: Node? = null
        var acquiredFirst = false
        var acquiredSecond = false
        while (true) {
            try {
                acquiredFirst = firstQueueLock.tryLock()
                acquiredSecond = secondQueueLock.tryLock()

                if (acquiredFirst && acquiredSecond) {
                    val firstQueueElement = firstQueue.peek()
                    val secondQueueElement = secondQueue.peek()
                    resultNode =
                        if (firstQueueElement == null && secondQueueElement == null)
                            null
                        else
                            if (NODE_DISTANCE_COMPARATOR.compare(firstQueueElement, secondQueueElement) > 0)
                                firstQueue.poll()
                            else
                                secondQueue.poll()
                }
            } finally {
                if (acquiredFirst) {
                    firstQueueLock.unlock()
                }
                if (acquiredSecond) {
                    secondQueueLock.unlock()
                }
                break
            }
        }
        if (resultNode != null) processedNodes.decrementAndGet()
        return resultNode
    }

    fun add(node: Node) {
        val (queue, index) = getRandomQueueWithOrderIndex()
        val queueLock = queuesLocks[index]
        while (true) {
            if (queueLock.tryLock()) {
                try {
                    queue.add(node)
                    processedNodes.incrementAndGet()
                } finally {
                    queueLock.unlock()
                    break
                }
            }
        }
    }

    private fun getRandomQueueWithOrderIndex(): Pair<PriorityQueue<Node>, Int> {
        val randomIndex = ThreadLocalRandom.current().nextInt(queues.size)
        return Pair(queues[randomIndex], randomIndex)
    }
}
