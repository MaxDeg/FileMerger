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
        private readonly int chunkSize;
        private readonly List<FileChunk<TKey>> chunks = new List<FileChunk<TKey>>();

        public FileMerger(params string[] filePaths)
            : this(50000, filePaths)
        { }

        public FileMerger(int chunkSize, params string[] filePaths)
        {
            this.chunkSize = chunkSize;

            foreach (var path in filePaths)
            {
                using (var stream = File.OpenRead(path))
                    this.chunks.AddRange(FileChunk<TKey>.CreateChunks<TSerializer>(stream, 50000));
            }
        }

        public void Add(string filePath)
        {
            this.Add<TSerializer>(filePath);
        }

        public void Add<TOtherSerializer>(string filePath, int? chunkSize = null)
            where TOtherSerializer : IRecordSerializer<TKey>, new()
        {
            this.Add(new TOtherSerializer(), filePath);
        }

        public void Add<TOtherSerializer>(TOtherSerializer serializer, string filePath, int? chunkSize = null)
            where TOtherSerializer : IRecordSerializer<TKey>
        {
            using (var stream = File.OpenRead(filePath))
                this.chunks.AddRange(FileChunk<TKey>.CreateChunks(serializer, stream, chunkSize ?? this.chunkSize));
        }

        /// <summary>
        /// Start Merging file
        /// </summary>
        /// <param name="path">the resulting file</param>
        public void Merge(string path)
        {
            var serializer = new TSerializer();

            FileChunk<TKey> head = this.chunks.First();
            var tail = this.chunks.Skip(1).Take(10);

            do
            {
                head = new FileChunk<TKey>(new FileChunkMergeEnumerator<TKey>(head, tail), serializer);
                tail = this.chunks.Skip(10);
            }
            while (tail.Count() > 0);

            using (var stream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.Write))
            {
                serializer.Serialize(stream, head);
            }
        }
    }
}
