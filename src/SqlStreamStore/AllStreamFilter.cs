namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;

    public class AllStreamFilter
    {
        private AllStreamFilter(IEnumerable<string> startsWith)
        {
            StreamIdStartsWith = new HashSet<string>(startsWith, StringComparer.Ordinal);
        }

        public static AllStreamFilter StreamIdsStartWith(params string[] startsWith) 
            => new AllStreamFilter(startsWith);

        public IReadOnlyCollection<string> StreamIdStartsWith { get; }

        public MessageTypeFilter MessageTypeFilter { get; set; }
    }
}