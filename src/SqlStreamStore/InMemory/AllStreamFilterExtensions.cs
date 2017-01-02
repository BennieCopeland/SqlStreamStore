namespace SqlStreamStore.InMemory
{
    using System.Linq;

    internal static class AllStreamFilterExtensions
    {
        internal static bool Satisfies(this AllStreamFilter filter, InMemoryStreamMessage message)
        {
            if(filter == null)
            {
                return true;
            }
            if(filter.StreamIdStartsWith.Any(s => message.StreamId.StartsWith(s)))
            {
                return true;
            }
            return false;
        }
    }
}