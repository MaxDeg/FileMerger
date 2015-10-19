using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileMerger
{
    public sealed class FileMerger<TKey>
    {
        private readonly List<IFileChunker<TKey>> _files;

        public FileMerger(params IFileChunker<TKey>[] files)
        {
            this._files = files.ToList();

            this.MaxTempFile = 10;
        }

        public int MaxTempFile { get; set; }

        /// <summary>
        /// Start Merging file
        /// </summary>
        /// <param name="path">the resulting file</param>
        public Task Merge(string path)
        {
            throw new NotImplementedException();
        }
        
        private Task SortChunk()
        {

        }
    }
}
