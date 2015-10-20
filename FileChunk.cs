using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;

namespace FileMerger
{
    internal class FileChunk<TKey> : IDisposable, IEnumerable<IFileRecord<TKey>>
    {
        private readonly int _maxSize;
        private FileStream _chunkStream;
        private IFileRecordFormatter<TKey> _formatter;

        public FileChunk(int maxSize, IFileRecordFormatter<TKey> formatter)
        {
            this._chunkStream = new FileStream(Path.GetTempFileName(), FileMode.Open, FileAccess.ReadWrite);
            this._maxSize = maxSize;
            this._formatter = formatter;
        }

        public void Build(Stream stream)
        {
            var buffer = new SortedList<TKey, IFileRecord<TKey>>();

            using (var ms = new MemoryStream())
            {
                var formatter = new BinaryFormatter();

                foreach (var record in this._formatter.Deserialize(stream))
                {
                    formatter.Serialize(ms, record);

                    if (ms.Length >= this._maxSize) break;
                    buffer.Add(record.Key, record);
                }
            }

            this._formatter.Serialize(this._chunkStream, buffer.Select(p => p.Value));
        }

        public void Merge(IEnumerable<FileChunk<TKey>> otherChunk)
        {
            var resultStream = new FileStream(Path.GetTempFileName(), FileMode.Open, FileAccess.ReadWrite);
            var buffer = new List<IFileRecord<TKey>>(this._maxSize);
            var queue = new SortedList<TKey, FileRecordEnumerator>(otherChunk.Count());

            // initial data
            var enumerator = (FileRecordEnumerator)this.GetEnumerator();
            if (enumerator.MoveNext())
                queue.Add(enumerator.Current.Key, enumerator);

            foreach (var item in otherChunk)
            {
                var itemEnumerator = (FileRecordEnumerator)item.GetEnumerator();
                if (itemEnumerator.MoveNext())
                    queue.Add(itemEnumerator.Current.Key, itemEnumerator);
            }

            while (queue.Any())
            {
                var topItem = queue.First();
                queue.Remove(topItem.Key);

                // insert
                buffer.Add(topItem.Value.Current);
                if (buffer.Count == buffer.Capacity)
                {
                    this._formatter.Serialize(resultStream, buffer);
                    buffer.Clear();
                }

                if (topItem.Value.MoveNext())
                    queue.Add(topItem.Value.Current.Key, topItem.Value);
            }

            this._chunkStream.Close();
            File.Delete(this._chunkStream.Name);
            this._chunkStream = resultStream;
        }

        public void Save(string path)
        {
            this._chunkStream.Flush();
            this._chunkStream.Close();

            File.Copy(this._chunkStream.Name, path, true);
        }

        public void Dispose()
        {
            if (this._chunkStream != null)
            {
                var path = this._chunkStream.Name;
                this._chunkStream.Dispose();
                File.Delete(path);
            }
        }

        public IEnumerator<IFileRecord<TKey>> GetEnumerator()
        {
            this._chunkStream.Seek(0L, SeekOrigin.Begin);
            return new FileRecordEnumerator(this._formatter.Deserialize(this._chunkStream));
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            this._chunkStream.Seek(0L, SeekOrigin.Begin);
            return new FileRecordEnumerator(this._formatter.Deserialize(this._chunkStream));
        }

        internal class FileRecordEnumerator : IEnumerator<IFileRecord<TKey>>
        {
            private int _index;
            private IFileRecord<TKey> _current;
            private IEnumerable<IFileRecord<TKey>> _data;

            public FileRecordEnumerator(IEnumerable<IFileRecord<TKey>> data)
            {
                this._index = -1;
                this._current = null;
                this._data = data;
            }

            public IFileRecord<TKey> Current { get { return this._current; } }
            object IEnumerator.Current { get { return this._current; } }

            public bool MoveNext()
            {
                this._current = this._data.Skip(++this._index).FirstOrDefault();

                return this._current != null;
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            public void Dispose() { }
        }
    }
}
