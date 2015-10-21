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
		private readonly string _chunkPath;
		private readonly IRecordSerializer<TKey> _serializer;

		public FileChunk(IEnumerable<IFileRecord<TKey>> records, IRecordSerializer<TKey> serializer)
		{
			this._chunkPath = Path.GetTempFileName();
			this._serializer = serializer;

			using (var stream = new FileStream(this._chunkPath, FileMode.Open, FileAccess.Write))
				this._serializer.Serialize(stream, records);
		}

		public void Dispose()
		{
			//File.Delete(this._chunkPath);
		}
		
		public static IEnumerable<FileChunk<TKey>> CreateChunks<TSerializer>(Stream stream, int maxSize)
			where TSerializer : IRecordSerializer<TKey>, new()
		{
			var serializer = new TSerializer();
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

						break;
					}

					buffer.Add(record.Key, record);
				}

				if (buffer.Count > 0)
					chunks.Add(new FileChunk<TKey>(buffer.Select(p => p.Value), serializer));
			}

			return chunks;
		}


		public IEnumerator<IFileRecord<TKey>> GetEnumerator()
		{
			using (var stream = new FileStream(this._chunkPath, FileMode.Open, FileAccess.Read))
				return new FileRecordEnumerator(this._serializer.Deserialize(stream));
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.GetEnumerator();
		}

		internal class FileRecordEnumerator : IEnumerator<IFileRecord<TKey>>
		{
			private IFileRecord<TKey> _current;
			private IEnumerable<IFileRecord<TKey>> _data;

			public FileRecordEnumerator(IEnumerable<IFileRecord<TKey>> data)
			{
				this._current = null;
				this._data = data.ToList();
			}

			public IFileRecord<TKey> Current { get { return this._current; } }
			object IEnumerator.Current { get { return this._current; } }

			public bool MoveNext()
			{
				this._current = this._data.FirstOrDefault();
				this._data = this._data.Skip(1);

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
