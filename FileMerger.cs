using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileMerger
{
    public sealed class FileMerger<TKey, TFormatter> 
        where TFormatter : IFileRecordFormatter<TKey>, new()
        where TKey : IComparable<TKey>
    {
        private readonly List<string> _filePaths;

        public FileMerger(params string[] filePaths)
        {
            this._filePaths = filePaths.ToList();

            this.MaxTempFile = 10;
            this.ChunkSize = 50000;
        }

        public int MaxTempFile { get; set; }
        public int ChunkSize { get; set; }

        /// <summary>
        /// Start Merging file
        /// </summary>
        /// <param name="path">the resulting file</param>
        public void Merge(string path)
        {
            var formatter = new TFormatter();
            var chunks = new List<FileChunk<TKey>>();

            foreach (var file in this._filePaths)
            {
                var stream = new FileStream(file, FileMode.Open, FileAccess.Read);

                while (stream.Position < stream.Length)
                {
                    var chunk = new FileChunk<TKey>(this.ChunkSize, formatter);
                    chunk.Build(stream);

                    chunks.Add(chunk);

                    if(chunks.Count == this.MaxTempFile)
                    {
                        this.MergeChunks(chunks);
                    }
                }
            }

            if (chunks.Count > 1)
            {
                this.MergeChunks(chunks);
            }

            chunks.Single().Save(path);
        }

        private void MergeChunks(List<FileChunk<TKey>> chunks)
        {
            var firstChunk = chunks.First();
            firstChunk.Merge(chunks.Skip(1));

            foreach (var mergedChunk in chunks.Skip(1))
                mergedChunk.Dispose();

            chunks.Clear();
            chunks.Add(firstChunk);
        }
    }
}
