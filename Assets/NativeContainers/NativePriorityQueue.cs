using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Unity.Burst;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Jobs;


namespace NativeCollections
{
    /// <summary>
    /// <para>An implementation of a generic job-safe priority queue as a NativeContainer. Uses an <see cref="UnsafeList"/> interally for storage
    /// so <see cref="Capacity"/> will automatically be expanded as elements are added.</para>
    /// <para>Built from a container of the same name by jeffvella, which is itself based on the C# "FastPriorityQueue" by BlueRaja.</para>
    /// </summary>
    // https://github.com/jeffvella/UnityAStarNavigation/blob/master/Assets/Plugins/Navigation/Helpers/Collections/NativePriorityQueue.cs
    // https://github.com/BlueRaja/High-Speed-Priority-Queue-for-C-Sharp/wiki/Getting-Started
    [NativeContainer]
    public unsafe struct NativePriorityQueue<T> : IDisposable
        where T : unmanaged
    {
        [NativeDisableUnsafePtrRestriction]
        internal UnsafeList* _nodes;

        Node LastUnsafe => ReadUnsafe(_nodes->Length - 1);

        #region SAFETY
#if ENABLE_UNITY_COLLECTIONS_CHECKS
        internal AtomicSafetyHandle m_Safety;

        [NativeSetClassTypeToNullOnSchedule]
        internal DisposeSentinel m_DisposeSentinel;
#endif

        [System.Diagnostics.Conditional("ENABLE_UNITY_COLLECTIONS_CHECKS")]
        private void RequireReadAccess()
        {
#if ENABLE_UNITY_COLLECTIONS_CHECKS
            AtomicSafetyHandle.CheckReadAndThrow(m_Safety);
#endif
        }

        [System.Diagnostics.Conditional("ENABLE_UNITY_COLLECTIONS_CHECKS")]
        private void RequireWriteAccess()
        {
#if ENABLE_UNITY_COLLECTIONS_CHECKS
            AtomicSafetyHandle.CheckWriteAndThrow(m_Safety);
#endif
        }
#endregion

        public NativePriorityQueue(int initialCapacity, Allocator allocator)
        {
            var totalSize = UnsafeUtility.SizeOf<Node>() * (long)initialCapacity;

#if ENABLE_UNITY_COLLECTIONS_CHECKS
            if (allocator <= Allocator.None)
                throw new ArgumentException("Allocator must be Temp, TempJob, or Persistent", "allocator");
            if (initialCapacity < 0)
                throw new ArgumentOutOfRangeException(nameof(initialCapacity), $"{nameof(initialCapacity)} must be >= 0");
            if (!UnsafeUtility.IsBlittable<T>())
                throw new ArgumentException($"{typeof(T)} used in {nameof(NativePriorityQueue<T>)} must be blittable");

            CollectionHelper.CheckIsUnmanaged<T>();

            if (totalSize > int.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(initialCapacity), $"Capacity * sizeof(T) cannot exceed {int.MaxValue} bytes");

            DisposeSentinel.Create(out m_Safety, out m_DisposeSentinel, 0, allocator);
            AtomicSafetyHandle.SetBumpSecondaryVersionOnScheduleWrite(m_Safety, true);
#endif

            int nodeSize = UnsafeUtility.SizeOf<Node>();
            int nodeAlign = UnsafeUtility.AlignOf<Node>();
            _nodes = UnsafeList.Create(nodeSize, nodeAlign, initialCapacity, allocator);
            _nodes->Add<Node>(default);
        }

        public void SetCapacity(int capacity)
        {
            RequireWriteAccess();

            _nodes->SetCapacity<T>(capacity);
        }

        public int Length
        {
            get
            {
                RequireReadAccess();
                return LengthUnsafe;
            }
        }

        int LengthUnsafe => _nodes->Length - 1;

        /// <summary>
        /// Returns the capacity of the queue. Capacity will automatically be expanded as elements are added to the
        /// queue.
        /// </summary>
        public int Capacity
        {
            get
            {
                RequireReadAccess();

                return CapacityUnsafe;
            }
        }

        int CapacityUnsafe => _nodes->Capacity;

        /// <summary>
        /// Read from a list element without any safety checks.
        /// </summary>
        Node ReadUnsafe(int index) => UnsafeUtility.ReadArrayElement<Node>(_nodes->Ptr, index);

        /// <summary>
        /// Write to a list element without any safety checks.
        /// </summary>
        void WriteUnsafe(int index, Node value) => UnsafeUtility.WriteArrayElement(_nodes->Ptr, index, value);

        public bool Contains(Node node)
        {
            RequireReadAccess();

            var listNode = ReadUnsafe(node.index);
            return node.Equals(listNode);
        }

        /// <summary>
        /// Add a node to the priority queue. Nodes are implicitly convertible to <typeparamref name="T"/>, so you can
        /// pass <typeparamref name="T"/> as the first argument instead of a node.
        /// </summary>
        public void Enqueue(Node node, int priority)
        {
            RequireWriteAccess();
            RequireReadAccess();

            node.priority = priority;
            node.index = LengthUnsafe + 1;

            // Add the node to the end of the list
            _nodes->Add<Node>(node);

            CascadeUpUnsafe(node);
        }

        /// <summary>
        /// Get the node with the lowest priority from the queue.
        /// </summary>
        public Node Dequeue()
        {
            RequireReadAccess();
            RequireWriteAccess();

            Node returnMe = ReadUnsafe(1);
             
            //If the node is already the last node, we can remove it immediately
            if (LengthUnsafe == 1)
            {
                _nodes->RemoveAtSwapBack<Node>(1);
                return returnMe;
            }

            var formerLast = RemoveAtSwapBackUnsafe(1);

            // Bubble down the swapped node, which was prevously the final node and is now in the first position
            CascadeDownUnsafe(formerLast);

            return returnMe;
        }

        private void CascadeUpUnsafe(Node node)
        {
            //aka Heapify-up
            int parent;
            if (node.index > 1)
            {
                parent = node.index >> 1;
                Node parentNode = ReadUnsafe(parent);

                if (HasHigherOrEqualPriority(parentNode, node))
                    return;

                //Node has lower priority value, so move parent down the heap to make room
                WriteUnsafe(node.index, parentNode);
                parentNode.index = node.index;
                node.index = parent;
            }
            else
            {
                return;
            }
            while (parent > 1)
            {
                parent >>= 1;
                Node parentNode = ReadUnsafe(parent);
                if (HasHigherOrEqualPriority(parentNode, node))
                    break;

                //Node has lower priority value, so move parent down the heap to make room
                WriteUnsafe(node.index, parentNode);
                parentNode.index = node.index;
                node.index = parent;
            }
            WriteUnsafe(node.index, node);
        }

        private void CascadeDownUnsafe(Node node)
        {
            //aka Heapify-down
            int finalQueueIndex = node.index;
            int childLeftIndex = 2 * finalQueueIndex;

            // If leaf node, we're done
            if (childLeftIndex > LengthUnsafe)
                return;

            // Check if the left-child is higher-priority than the current node
            int childRightIndex = childLeftIndex + 1;
            Node childLeft = ReadUnsafe(childLeftIndex);

            if (HasHigherPriority(childLeft, node))
            {
                // Check if there is a right child. If not, swap and finish.
                if (childRightIndex > LengthUnsafe)
                {
                    node.index = childLeftIndex;
                    childLeft.index = finalQueueIndex;
                    WriteUnsafe(finalQueueIndex, childLeft);
                    WriteUnsafe(childLeftIndex, node);
                    return;
                }
                // Check if the left-child is higher-priority than the right-child
                Node childRight = ReadUnsafe(childRightIndex);
                if (HasHigherPriority(childLeft, childRight))
                {
                    // left is highest, move it up and continue
                    childLeft.index = finalQueueIndex;
                    WriteUnsafe(finalQueueIndex, childLeft);
                    finalQueueIndex = childLeftIndex;
                }
                else
                {
                    // right is even higher, move it up and continue
                    childRight.index = finalQueueIndex;
                    WriteUnsafe(finalQueueIndex, childRight);
                    finalQueueIndex = childRightIndex;
                }
            }
            // Not swapping with left-child, does right-child exist?
            else if (childRightIndex > LengthUnsafe)
            {
                return;
            }
            else
            {
                // Check if the right-child is higher-priority than the current node
                Node childRight = ReadUnsafe(childRightIndex);
                if (HasHigherPriority(childRight, node))
                {
                    childRight.index = finalQueueIndex;
                    WriteUnsafe(finalQueueIndex, childRight);
                    finalQueueIndex = childRightIndex;
                }
                // Neither child is higher-priority than current, so finish and stop.
                else
                {
                    return;
                }
            }

            while (true)
            {
                childLeftIndex = 2 * finalQueueIndex;

                // If leaf node, we're done
                if (childLeftIndex > LengthUnsafe)
                {
                    node.index = finalQueueIndex;
                    WriteUnsafe(finalQueueIndex, node);
                    break;
                }

                // Check if the left-child is higher-priority than the current node
                childRightIndex = childLeftIndex + 1;
                childLeft = ReadUnsafe(childLeftIndex);
                if (HasHigherPriority(childLeft, node))
                {
                    // Check if there is a right child. If not, swap and finish.
                    if (childRightIndex > LengthUnsafe)
                    {
                        node.index = childLeftIndex;
                        childLeft.index = finalQueueIndex;
                        WriteUnsafe(finalQueueIndex, childLeft);
                        WriteUnsafe(childLeftIndex, node);
                        break;
                    }
                    // Check if the left-child is higher-priority than the right-child
                    Node childRight = ReadUnsafe(childRightIndex);
                    if (HasHigherPriority(childLeft, childRight))
                    {
                        // left is highest, move it up and continue
                        childLeft.index = finalQueueIndex;
                        WriteUnsafe(finalQueueIndex, childLeft);
                        finalQueueIndex = childLeftIndex;
                    }
                    else
                    {
                        // right is even higher, move it up and continue
                        childRight.index = finalQueueIndex;
                        WriteUnsafe(finalQueueIndex, childRight);
                        finalQueueIndex = childRightIndex;
                    }
                }
                // Not swapping with left-child, does right-child exist?
                else if (childRightIndex > LengthUnsafe)
                {
                    node.index = finalQueueIndex;
                    WriteUnsafe(finalQueueIndex, node);
                    break;
                }
                else
                {
                    // Check if the right-child is higher-priority than the current node
                    Node childRight = ReadUnsafe(childRightIndex);
                    if (HasHigherPriority(childRight, node))
                    {
                        childRight.index = finalQueueIndex;
                        WriteUnsafe(finalQueueIndex, childRight);
                        finalQueueIndex = childRightIndex;
                    }
                    // Neither child is higher-priority than current, so finish and stop.
                    else
                    {
                        node.index = finalQueueIndex;
                        WriteUnsafe(finalQueueIndex, node);
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// Returns true if 'higher' has higher priority than 'lower', false otherwise.
        /// Ties return false;
        /// </summary>
        private bool HasHigherPriority(Node higher, Node lower) => higher.priority < lower.priority;

        /// <summary>
        /// Returns true if 'higher' has higher priority than 'lower', false otherwise.
        /// Ties return true;
        /// </summary>
        private bool HasHigherOrEqualPriority(Node higher, Node lower) => higher.priority <= lower.priority;

        void UpdatePriorityUnsafe(ref Node node, int priority)
        {
            node.priority = priority;
            OnNodeUpdatedUnsafe(node);
        }

        /// <summary>
        /// Sets the priority for all nodes with the given value.
        /// </summary>
        public void UpdatePriorityByValue(T value, int priority)
        {
            RequireReadAccess();

            for (int i = 1; i <= LengthUnsafe; ++i)
            {
                var node = ReadUnsafe(i);
                if (EqualityComparer<T>.Default.Equals(node.value, value))
                {
                    UpdatePriorityUnsafe(ref node, priority);
                }
            }
        }

        private void OnNodeUpdatedUnsafe(Node node)
        {
            //Bubble the updated node up or down as appropriate
            int parentIndex = node.index >> 1;

            if (parentIndex > 0 && HasHigherPriority(node, ReadUnsafe(parentIndex)))
            {
                CascadeUpUnsafe(node);
            }
            else
            {
                //Note that CascadeDown will be called if parentNode == node (that is, node is the root)
                CascadeDownUnsafe(node);
            }
        }

        /// <summary>
        /// Remove all nodes with the given value.
        /// </summary>
        public void RemoveByValue(T val)
        {
            RequireReadAccess();
            RequireWriteAccess();

            for( int i = 1; i <= LengthUnsafe; ++i )
            {
                var node = ReadUnsafe(i);
                if( EqualityComparer<T>.Default.Equals(node.value, val) )
                {
                    var formerLast = RemoveAtSwapBackUnsafe(i);
                    CascadeDownUnsafe(formerLast);
                }
            }
        }

        /// <summary>
        /// Remove all nodes with the given priority.
        /// </summary>
        public void RemoveByPriority(int priority)
        {
            RequireReadAccess();
            RequireWriteAccess();

            for (int i = 1; i <= LengthUnsafe; ++i)
            {
                var node = ReadUnsafe(i);
                if (node.priority == priority)
                {
                    var formerLast = RemoveAtSwapBackUnsafe(i);
                    CascadeDownUnsafe(formerLast);
                }
            }
        }

        /// <summary>
        /// Remove a node by index.
        /// </summary>
        void RemoveByIndexUnsafe(int index)
        {
            var formerLast = RemoveAtSwapBackUnsafe(index);

            //Now bubble formerLastNode (which is no longer the last node) up or down as appropriate
            OnNodeUpdatedUnsafe(formerLast);
        }


        /// <summary>
        /// Remove the node at the given index and swap the last node into it's place. Note you should call 
        /// a cascade function immediately after this to ensure correct state. 
        /// This modifies the length of the queue.
        /// </summary>
        /// <returns>The former last node swapped into place.</returns>
        Node RemoveAtSwapBackUnsafe(int index)
        {
            // Swap the last node with the node at the given index
            var formerLast = LastUnsafe;
            formerLast.index = index;
            WriteUnsafe(index, formerLast);
            _nodes->RemoveAtSwapBack<Node>(LengthUnsafe);
            return formerLast;
        }

        /// <summary>
        /// A priority queue node. Implicitly convertible to <typeparamref name="T"/>.
        /// </summary>
        [StructLayout(LayoutKind.Sequential)]
        public struct Node : IEquatable<Node>
        {
            public T value;
            public int priority;
            public int index;

            public bool Equals(Node other)
            {
                return index == other.index && priority == other.priority &&
                    EqualityComparer<T>.Default.Equals(value, other.value);
            }

            public static implicit operator T(Node n) => n.value;
            public static implicit operator Node(T v) => new Node { value = v };

            public override string ToString() => $"Node [{value}, {priority}, {index}]";
        }

        public void Dispose()
        {
#if ENABLE_UNITY_COLLECTIONS_CHECKS
            DisposeSentinel.Dispose(ref m_Safety, ref m_DisposeSentinel);
#endif
            UnsafeList.Destroy(_nodes);
            _nodes = null;
        }

        public JobHandle Dispose(JobHandle inputDeps)
        {
#if ENABLE_UNITY_COLLECTIONS_CHECKS
            DisposeSentinel.Clear(ref m_DisposeSentinel);

            var jobHandle = new NativePriorityQueueDisposeJob { Data = new NativePriorityQueueDispose 
            { 
                m_ListData = _nodes, 
                m_Safety = m_Safety } 
            }.Schedule(inputDeps);

            AtomicSafetyHandle.Release(m_Safety);
#else
            var jobHandle = new NativePriorityQueueDisposeJob 
            { 
                Data = new NativePriorityQueueDispose { m_ListData = _nodes } 
            }.Schedule(inputDeps);
#endif
            _nodes = null;

            return jobHandle;
        }

        [NativeContainer]
        internal unsafe struct NativePriorityQueueDispose
        {
            [NativeDisableUnsafePtrRestriction]
            public UnsafeList* m_ListData;

#if ENABLE_UNITY_COLLECTIONS_CHECKS
            internal AtomicSafetyHandle m_Safety;
#endif

            public void Dispose()
            {
                UnsafeList.Destroy(m_ListData);
            }
        }

        [BurstCompile]
        internal unsafe struct NativePriorityQueueDisposeJob : IJob
        {
            internal NativePriorityQueueDispose Data;

            public void Execute()
            {
                Data.Dispose();
            }
        }
    }
}