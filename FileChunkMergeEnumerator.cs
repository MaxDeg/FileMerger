using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileMerger
{
    internal class FileChunkMergeEnumerator<TKey> : IEnumerable<IFileRecord<TKey>>, IEnumerator<IFileRecord<TKey>>
        where TKey : IComparable<TKey>
    {
        private readonly IEnumerator<IFileRecord<TKey>> left;
        private readonly IEnumerator<IFileRecord<TKey>> right;

        private IFileRecord<TKey> current;
        private Func<bool> moveLeft;
        private Func<bool> moveRight;

        public FileChunkMergeEnumerator(params FileChunk<TKey>[] data)
            : this(data.FirstOrDefault(), data.Skip(1))
        { }

        public FileChunkMergeEnumerator(FileChunk<TKey> head, IEnumerable<FileChunk<TKey>> tail)
        {
            if (head == null)
                throw new ArgumentNullException("head");

            this.current = null;
            this.left = head.GetEnumerator();

            this.moveLeft = () => this.left.MoveNext();
            this.moveRight = () => false;

            var nextHead = tail.FirstOrDefault();
            if (nextHead != null)
            {
                this.right = new FileChunkMergeEnumerator<TKey>(nextHead, tail.Skip(1));
                this.moveRight = () => this.right.MoveNext();
            }
        }

        public IFileRecord<TKey> Current
        {
            get { return this.current; }
        }

        object IEnumerator.Current { get { return this.Current; } }

        public bool MoveNext()
        {
            var hasMoreLeft = this.moveLeft();
            var hasMoreRight = this.moveRight();

            if (hasMoreLeft && hasMoreRight)
            {
                // reset the delegates
                this.moveLeft = () => this.left.MoveNext();
                this.moveRight = () => this.right.MoveNext();

                var compareResult = this.right.Current.Key.CompareTo(this.left.Current.Key);
                if (compareResult < 0)
                {
                    this.moveLeft = () => true; // left is not the good one so we don't move the the next
                    this.current = this.right.Current;
                }
                else if (compareResult > 0)
                {
                    this.moveRight = () => true; // right is not the good one so we don't move the the next
                    this.current = this.left.Current;
                }
                else
                    this.current = this.left.Current.ResolveConflict(this.right.Current);
            }
            else if (hasMoreLeft)
            {
                this.moveLeft = () => this.left.MoveNext();
                this.current = this.left.Current;
            }
            else if (hasMoreRight)
            {
                this.moveRight = () => this.right.MoveNext();
                this.current = this.right.Current;
            }

            return hasMoreLeft || hasMoreRight;
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            if (this.right != null) this.right.Dispose();
            if (this.left != null) this.left.Dispose();
        }

        public IEnumerator<IFileRecord<TKey>> GetEnumerator()
        {
            return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this;
        }
    }
}