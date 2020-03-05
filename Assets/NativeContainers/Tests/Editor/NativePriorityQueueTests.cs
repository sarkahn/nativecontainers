
using NativeCollections;
using NUnit.Framework;
using Unity.Collections;
using Unity.Jobs;


[TestFixture]
public class NativePriorityQueueTests
{

    struct PriorityQueueJob : IJob
    {
        public NativePriorityQueue<int> queue;

        public void Execute()
        {
            queue.Enqueue(15, 5);
            queue.Enqueue(10, 1);
            queue.Enqueue(3, 4);
            queue.Enqueue(7, 0);
        }
    }

    [Test]
    public void JobTest()
    {
        using (var queue = new NativePriorityQueue<int>(5, Allocator.TempJob))
        {
            new PriorityQueueJob
            {
                queue = queue
            }.Schedule().Complete();

            Assert.AreEqual(4, queue.Length);
            Assert.AreEqual(7, queue.Dequeue().value);
            Assert.AreEqual(10, queue.Dequeue().value);
            Assert.AreEqual(3, queue.Dequeue().value);
            Assert.AreEqual(15, queue.Dequeue().value);
        }
    }

    [Test]
    public void QueueModifiesLength()
    {
        var queue = new NativePriorityQueue<int>(10, Allocator.Temp);

        queue.Enqueue(0, 10);
        queue.Enqueue(1, 15);
        queue.Enqueue(2, 9);
        queue.Enqueue(3, 100);

        Assert.AreEqual(4, queue.Length);
    }

    [Test]
    public void NodesDequeueAccordingToPriority()
    {
        var queue = new NativePriorityQueue<int>(10, Allocator.Temp);

        queue.Enqueue(10, 5);
        queue.Enqueue(20, 4);
        queue.Enqueue(30, 8);
        queue.Enqueue(40, 0);

        var node = queue.Dequeue();

        Assert.AreEqual(40, node.value);
        Assert.AreEqual(0, node.priority);

        node = queue.Dequeue();

        Assert.AreEqual(20, node.value);
        Assert.AreEqual(4, node.priority);

        node = queue.Dequeue();

        Assert.AreEqual(10, node.value);
        Assert.AreEqual(5, node.priority);

        node = queue.Dequeue();

        Assert.AreEqual(30, node.value);
        Assert.AreEqual(8, node.priority);
    }

    [Test]
    public void RemoveByValue()
    {
        {
            var queue = new NativePriorityQueue<int>(10, Allocator.Temp);

            queue.Enqueue(10, 5);
            queue.Enqueue(12, 11);
            queue.Enqueue(10, 13);
            queue.Enqueue(15, 10);

            queue.RemoveByValue(10);

            Assert.AreEqual(2, queue.Length);

            Assert.AreEqual(15, queue.Dequeue().value);
            Assert.AreEqual(12, queue.Dequeue().value);
        }
    }

    [Test]
    public void EnqueueExpandsCapacity()
    {
        var queue = new NativePriorityQueue<int>(1, Allocator.Temp);

        for( int i = 0; i < 50; ++i )
            queue.Enqueue(i, i);

        Assert.Greater(queue.Capacity, 8);
    }
}
