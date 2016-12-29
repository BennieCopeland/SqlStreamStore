namespace SqlStreamStore.Subscriptions
{
    using System;

    public interface IStreamStoreNotifier : IObservable<StreamStoreNotificationInfo>, IDisposable
    {}
}