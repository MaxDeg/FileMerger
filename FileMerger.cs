using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileMerger
{
	public sealed class FileMerger<TKey, TSerializer>
		where TSerializer : IRecordSerializer<TKey>, new()
		where TKey : IComparable<TKey>
	{
		private readonly List<FileChunk<TKey>> _chunks = new List<FileChunk<TKey>>();

		public FileMerger(params string[] filePaths)
		{
			foreach (var path in filePaths)
			{
				using (var stream = File.OpenRead(path))
					this._chunks.AddRange(FileChunk<TKey>.CreateChunks<TSerializer>(stream, 50000));
			}
		}

		public void Add(string filePath)
		{
			this.Add<TSerializer>(filePath);
		}

		public void Add<TOtherSerializer>(string filePath)
			where TOtherSerializer : IRecordSerializer<TKey>, new()
		{
			using (var stream = File.OpenRead(filePath))
				this._chunks.AddRange(FileChunk<TKey>.CreateChunks<TOtherSerializer>(stream, 50000));
		}

		/// <summary>
		/// Start Merging file
		/// </summary>
		/// <param name="path">the resulting file</param>
		public void Merge(string path)
		{
			var serializer = new TSerializer();

			FileChunk<TKey> head = this._chunks.First();
			var tail = this._chunks.Skip(1).Take(10);

			do
			{
				head = new FileChunk<TKey>(new FileChunkMergeEnumerator<TKey>(head, tail), serializer);
				tail = this._chunks.Skip(10);
			}
			while (tail.Count() > 0);

			using (var stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write))
			{
				serializer.Serialize(stream, head);
			}
		}
	}
}
