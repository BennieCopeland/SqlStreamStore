namespace SqlStreamStore.Subscriptions
{
    using System.Collections.Generic;

    public class StreamStoreNotificationInfo
    {
        public StreamStoreNotificationInfo(HashSet<string> streamIds, HashSet<string> messageTypes)
        {
            StreamIds = streamIds;
            MessageTypes = messageTypes;
        }

        public IReadOnlyCollection<string> MessageTypes { get; }

        public IReadOnlyCollection<string> StreamIds { get; }
    }
}