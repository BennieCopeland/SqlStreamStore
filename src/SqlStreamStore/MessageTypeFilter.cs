namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;

    public class MessageTypeFilter
    {
        public enum MessageTypeFilterType
        {
            Matching,
            Excluding
        }

        private MessageTypeFilter(MessageTypeFilterType filterType, IEnumerable<string> values)
        {
            FilterType = filterType;
            Values = new HashSet<string>(values, StringComparer.Ordinal);
        }

        public IReadOnlyCollection<string> Values { get; }

        public MessageTypeFilterType FilterType { get; }

        public static MessageTypeFilter Matching(params string[] values) 
            => new MessageTypeFilter(MessageTypeFilterType.Matching, values);

        public static MessageTypeFilter Excluding(params string[] values) 
            => new MessageTypeFilter(MessageTypeFilterType.Excluding, values);
    }
}