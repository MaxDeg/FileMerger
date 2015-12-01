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
			using (var stream = new FileStream(this.chunkPath, FileMode.Open, FileAccess.Read))
				return new FileRecordEnumerator(this.serializer.Deserialize(stream));
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.GetEnumerator();
		}

		internal class FileRecordEnumerator : IEnumerator<IFileRecord<TKey>>
		{
			private IFileRecord<TKey> current;
			private IEnumerable<IFileRecord<TKey>> data;

			public FileRecordEnumerator(IEnumerable<IFileRecord<TKey>> data)
			{
				this.current = null;
				this.data = data.ToList();
			}

			public IFileRecord<TKey> Current { get { return this.current; } }
			object IEnumerator.Current { get { return this.current; } }

			public bool MoveNext()
			{
				this.current = this.data.FirstOrDefault();
				this.data = this.data.Skip(1);

				return this.current != null;
			}

			public void Reset()
			{
				throw new NotImplementedException();
			}

			public void Dispose() { }
		}
	}
}
