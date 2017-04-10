﻿namespace LoadTests
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using EasyConsole;
    using Serilog;

    internal class Program
    {
        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo
                .File("LoadTests.txt")
                .CreateLogger();

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, __) => cts.Cancel();

            Output.WriteLine(ConsoleColor.Yellow, "Choose a test:");
            new Menu()
                .Add(
                    "Append with ExpectedVersion.Any",
                    () => new AppendExpectedVersionAnyParallel().Run(cts.Token))
                .Add(
                    "Read All",
                    () => new ReadAll().Run(cts.Token))
                .Add(
                    "Append Max Count",
                    () => new AppendMaxCount().Run(cts.Token))
                .Display();

            if(Debugger.IsAttached)
            {
                Console.ReadLine();
            }
        }
    }
}