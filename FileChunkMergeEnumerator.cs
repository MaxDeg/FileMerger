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
        private IFileRecord<TKey> current;
        private IEnumerator<IFileRecord<TKey>> left;
        private IEnumerator<IFileRecord<TKey>> right;

		public FileChunkMergeEnumerator(params FileChunk<TKey>[] data)
            : this(data.FirstOrDefault(), data.Skip(1))
        { }

		public FileChunkMergeEnumerator(FileChunk<TKey> head, IEnumerable<FileChunk<TKey>> tail)
        {
            if (head == null)
                throw new ArgumentNullException("head");

            this.current = null;
            this.left = head.GetEnumerator();

            var nextHead = tail.FirstOrDefault();
            if (nextHead != null)
            {
                this.right = new FileChunkMergeEnumerator<TKey>(nextHead, tail.Skip(1));

                this.left.MoveNext();
                this.right.MoveNext();
            }
        }

        public IFileRecord<TKey> Current
        {
            get { return this.current; }
        }

        object IEnumerator.Current { get { return this.Current; } }

        public bool MoveNext()
        {
            if (this.right == null || this.left == null)
            {
                var enumerator = this.right ?? this.left;
                if (enumerator == null) return false;

                if (!enumerator.MoveNext())
                {
                    enumerator.Dispose();

                    this.right = this.left = null;
					return false;
				}
                this.current = enumerator.Current;

                return this.current != null;
            }
            else
            {
                var compareResult = this.right.Current.Key.CompareTo(this.left.Current.Key);
                if (compareResult < 0)
                {
                    this.current = GetNext(ref this.right);
                }
                else if (compareResult > 0)
                {
                    this.current = GetNext(ref this.left);
                }
                else
                {
                    this.current = GetNext(ref this.left).ResolveConflict(GetNext(ref this.right));
                }

                return this.current != null;
            }
        }

        private static IFileRecord<TKey> GetNext(ref IEnumerator<IFileRecord<TKey>> enumerator)
        {
            var current = enumerator.Current;
            if (!enumerator.MoveNext())
            {
                enumerator.Dispose();
                enumerator = null;
            }

            while (enumerator != null && enumerator.Current.Key.CompareTo(current.Key) == 0)
            {
                current = current.ResolveConflict(enumerator.Current);
                if (!enumerator.MoveNext())
                {
                    enumerator.Dispose();
                    enumerator = null;
                }
            }

            return current;
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
