using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;

namespace FileMerger
{
    internal class FileChunk<TKey> : IDisposable
    {
        private readonly FileStream _chunkStream;
        private readonly int _maxSize;
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
            var buffer = new List<IFileRecord<TKey>>(this._maxSize);
            var queue = new Queue<IFileRecord<TKey>>(otherChunk.Count());

            //foreach (var item in otherChunk.Select(c => c.))
            {
                //queue.Enqueue()
            }
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
    }
}
