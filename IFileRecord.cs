using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileMerger
{
    public interface IFileRecord<TKey> where TKey : IComparable<TKey>
    {
        TKey Key { get; }
        IFileRecord<TKey> ResolveConflict(IFileRecord<TKey> other);
    }
}
