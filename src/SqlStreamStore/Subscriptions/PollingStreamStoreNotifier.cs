namespace SqlStreamStore.Subscriptions
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using EnsureThat;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Logging;
    using SqlStreamStore.Streams;

    public sealed class PollingStreamStoreNotifier : IStreamStoreNotifier
    {
        private static readonly ILog s_logger = LogProvider.GetCurrentClassLogger();
        private readonly CancellationTokenSource _disposed = new CancellationTokenSource();
        private readonly IReadonlyStreamStore _streamStore;
        private readonly int _interval;
        private readonly Subject<StreamStoreNotificationInfo> _storeAppended = new Subject<StreamStoreNotificationInfo>();

        public PollingStreamStoreNotifier(IReadonlyStreamStore streamStore, int interval = 1000)
        {
            Ensure.That(streamStore, nameof(streamStore)).IsNotNull();
            Ensure.That(interval, nameof(interval)).IsGte(10);

            _streamStore = streamStore;
            _interval = interval;
            Task.Run(Poll, _disposed.Token);
        }

        public void Dispose()
        {
            _disposed.Cancel();
        }

        public IDisposable Subscribe(IObserver<StreamStoreNotificationInfo> observer) => _storeAppended.Subscribe(observer);

        private async Task Poll()
        {
            long headPosition = Position.End;
            while(headPosition == Position.End)
            {
                try
                {
                    headPosition = await _streamStore.ReadHeadPosition(_disposed.Token);
                }
                catch(Exception ex)
                {
                    s_logger.ErrorException("Exception occurred initializing polling.", ex);
                    await Task.Delay(_interval);
                }
            }

            Func<Task<ReadAllPage>> readAll =
                    () => _streamStore.ReadAllForwards(headPosition, 50, false, _disposed.Token);

            while (!_disposed.IsCancellationRequested)
            {
                try
                {
                    var page = await readAll();

                    if(page.Messages.Length > 0)
                    {
                        var streamIds = new HashSet<string>(page.Messages.Select(m => m.StreamId).Distinct());
                        var messageTypes = new HashSet<string>(page.Messages.Select(m => m.Type).Distinct());
                        _storeAppended.OnNext(new StreamStoreNotificationInfo(streamIds, messageTypes));
                        readAll = () => page.ReadNext(_disposed.Token);
                    }
                    else
                    {
                        await Task.Delay(_interval, _disposed.Token);
                    }
                }
                catch(Exception ex)
                {
                    s_logger.ErrorException("Exception occurred polling stream store for messages. " +
                                            $"HeadPosition: {headPosition}", ex);
                }
            }
        }
    }
}