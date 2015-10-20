using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileMerger
{
    public class FileChunkMergeEnumerator<TKey> : IEnumerable<IFileRecord<TKey>>, IEnumerator<IFileRecord<TKey>>
        where TKey : IComparable<TKey>
    {
        private IFileRecord<TKey> _current;
        private IEnumerator<IFileRecord<TKey>> _left;
        private IEnumerator<IFileRecord<TKey>> _right;

        public FileChunkMergeEnumerator(params IEnumerable<IFileRecord<TKey>>[] data)
            : this(data.FirstOrDefault(), data.Skip(1))
        { }

        public FileChunkMergeEnumerator(IEnumerable<IFileRecord<TKey>> head, IEnumerable<IEnumerable<IFileRecord<TKey>>> tail)
        {
            if (head == null)
                throw new ArgumentNullException("head");

            this._current = null;
            this._left = head.GetEnumerator();

            var nextHead = tail.FirstOrDefault();
            if (nextHead != null)
            {
                this._right = new FileChunkMergeEnumerator<TKey>(nextHead, tail.Skip(1));

                this._left.MoveNext();
                this._right.MoveNext();
            }
        }

        public IFileRecord<TKey> Current
        {
            get { return this._right == null ? this._left.Current : this._current; }
        }

        object IEnumerator.Current { get { return this.Current; } }

        public bool MoveNext()
        {
            if (this._right == null || this._left == null)
            {
                var enumerator = this._right ?? this._left;
                if (enumerator == null) return false;

                var result = enumerator.MoveNext();
                this._current = enumerator.Current;

                while (enumerator.Current.Key.CompareTo(this._current.Key) == 0)
                {
                    this._current = this._current.ResolveConflict(enumerator.Current);
                    if (!enumerator.MoveNext())
                    {
                        enumerator = null;
                        break;
                    }
                }

                return this._current != null;
            }
            else
            {
                var compareResult = this._right.Current.Key.CompareTo(this._left.Current.Key);
                if (compareResult < 0)
                {
                    this._current = GetNext(ref this._right);
                }
                else if (compareResult > 0)
                {
                    this._current = GetNext(ref this._left);
                }
                else
                {
                    this._current = GetNext(ref this._left).ResolveConflict(GetNext(ref this._right));
                }

                return this._current != null;
            }
        }

        private static IFileRecord<TKey> GetNext(ref IEnumerator<IFileRecord<TKey>> enumerator)
        {
            var current = enumerator.Current;
            if (!enumerator.MoveNext()) enumerator = null;

            while (enumerator.Current.Key.CompareTo(current.Key) == 0)
            {
                current = current.ResolveConflict(enumerator.Current);
                if (!enumerator.MoveNext())
                {
                    enumerator = null;
                    break;
                }
            }

            return current;
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public void Dispose() { }

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
