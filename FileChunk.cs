using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;

namespace FileMerger
{
	internal class FileChunk<TKey> : IDisposable, IEnumerable<IFileRecord<TKey>>
		where TKey : IComparable<TKey>
	{
		private readonly string chunkPath;
		private readonly IRecordSerializer<TKey> serializer;

		public FileChunk(IEnumerable<IFileRecord<TKey>> records, IRecordSerializer<TKey> serializer)
		{
			this.chunkPath = Path.GetTempFileName();
			this.serializer = serializer;

			using (var stream = new FileStream(this.chunkPath, FileMode.Open, FileAccess.Write))
				this.serializer.Serialize(stream, records);
		}

		public void Dispose()
		{
			File.Delete(this.chunkPath);
		}

        public static IEnumerable<FileChunk<TKey>> CreateChunks<TSerializer>(TSerializer serializer, Stream stream, int maxSize)
            where TSerializer : IRecordSerializer<TKey>
        {
            var buffer = new SortedList<TKey, IFileRecord<TKey>>();
            var chunks = new List<FileChunk<TKey>>();

            using (var ms = new MemoryStream())
            {
                var formatter = new BinaryFormatter();

                foreach (var record in serializer.Deserialize(stream))
                {
                    formatter.Serialize(ms, record);

                    if (ms.Length >= maxSize)
                    {
                        chunks.Add(new FileChunk<TKey>(buffer.Select(p => p.Value), serializer));
                        buffer.Clear();
                        ms.SetLength(0L);
                    }

                    buffer.Add(record.Key, record);
                }

                if (buffer.Count > 0)
                    chunks.Add(new FileChunk<TKey>(buffer.Select(p => p.Value), serializer));
            }

            return chunks;
        }

        public static IEnumerable<FileChunk<TKey>> CreateChunks<TSerializer>(Stream stream, int maxSize)
			where TSerializer : IRecordSerializer<TKey>, new()
		{
            return CreateChunks<TSerializer>(new TSerializer(), stream, maxSize);
        }


		public IEnumerator<IFileRecord<TKey>> GetEnumerator()
        {
            return new FileRecordEnumerator(this.serializer, this.chunkPath);
        }

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.GetEnumerator();
		}

		internal class FileRecordEnumerator : IEnumerator<IFileRecord<TKey>>
        {
            private Stream stream;
            private IEnumerator<IFileRecord<TKey>> data;

            public FileRecordEnumerator(IRecordSerializer<TKey> serializer, string path)
            {
                this.stream = new FileStream(path, FileMode.Open, FileAccess.Read);
                this.data = serializer.Deserialize(stream).GetEnumerator();
            }

            public IFileRecord<TKey> Current { get { return this.data.Current; } }
            object IEnumerator.Current { get { return this.data.Current; } }

            public bool MoveNext()
            {
                return this.data.MoveNext();
            }

            public void Reset()
            {
                this.data.Reset();
            }

            public void Dispose()
            {
                this.stream.Dispose();
                this.data.Dispose();
            }
        }
	}
}
