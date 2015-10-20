using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileMerger
{
    public interface IFileRecordFormatter<TKey> where TKey : IComparable<TKey>
    {
        void Serialize(Stream stream, IEnumerable<IFileRecord<TKey>> records);
        IEnumerable<IFileRecord<TKey>> Deserialize(Stream stream);
    }
}
