﻿namespace PetProjects.LogAggregator.Presentation.ConsoleApplication
{
    using System;
    using System.IO;
    using System.Threading;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using PetProjects.Framework.Consul;
    using PetProjects.Framework.Consul.Client;
    using PetProjects.Framework.Consul.Store;
    using PetProjects.Framework.Consul.Watcher;
    using PetProjects.Framework.Logging.Consumer.ElasticSearch;
    using PetProjects.Framework.Logging.Producer;
    using Serilog.Events;

    using KafkaConfiguration = PetProjects.Framework.Logging.Consumer.KafkaConfiguration;

    public class Program
    {
        private static readonly ManualResetEvent QuitEvent = new ManualResetEvent(false);

        private static IConfigurationRoot Configuration { get; set; }

        public static void Main(string[] args)
        {
            // Setup Configuration with appsettings
            Program.SetupConfiguration();

            // Setup DI container
            var serviceCollection = new ServiceCollection();
            Program.SetupServices(serviceCollection);

            // Do the actual work here
            using (var parentServiceProvider = serviceCollection.BuildServiceProvider())
            {
                Program.Run(parentServiceProvider);
            }
        }

        private static void SetupConfiguration()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            Configuration = builder.Build();
        }

        private static void SetupServices(IServiceCollection serviceCollection)
        {
            SetupLogging(serviceCollection);

            serviceCollection.AddSingleton<KafkaConfiguration>(sp =>
            {
                var store = sp.GetRequiredService<IStringKeyValueStore>();
                return new KafkaConfiguration
                {
                    Brokers = store.GetAndConvertValue<string>("KafkaConfiguration/Brokers").Split(','),
                    Topic = store.GetAndConvertValue<string>("KafkaConfiguration/Topic")
                };
            });

            serviceCollection.AddSingleton<ElasticClientConfiguration>(sp =>
            {
                var store = sp.GetRequiredService<IStringKeyValueStore>();
                return new ElasticClientConfiguration
                {
                    Address = store.GetAndConvertValue<string>("ElasticConfiguration/Address"),
                    AppLogsIndex = store.GetAndConvertValue<string>("ElasticConfiguration/AppLogsIndex")
                };
            });

            serviceCollection.AddSingleton<ElasticStoreConfiguration>(sp => sp.GetRequiredService<ElasticClientConfiguration>());

            // this call must happen after previous two (addsingleton of kafkaconfig + elasticconfig)
            serviceCollection.AddPetProjectElasticLogConsumer(new KafkaConfiguration(), new ElasticClientConfiguration());
        }

        private static void SetupLogging(IServiceCollection serviceCollection)
        {
            SetupConsul(serviceCollection);

            serviceCollection.AddSingleton<ILogger>(NullLogger.Instance);

            using (var tempProvider = serviceCollection.BuildServiceProvider())
            {
                var store = tempProvider.GetRequiredService<IStringKeyValueStore>();
                serviceCollection.Remove(new ServiceDescriptor(typeof(ILogger), NullLogger.Instance));

                var kafkaConfig = new Framework.Logging.Producer.KafkaConfiguration
                {
                    Brokers = store.GetAndConvertValue<string>("KafkaConfiguration/Brokers").Split(','),
                    Topic = store.GetAndConvertValue<string>("KafkaConfiguration/Topic")
                };

                var sinkConfig = new PeriodicSinkConfiguration
                {
                    BatchSizeLimit = store.GetAndConvertValue<int>("Logging/BatchSizeLimit"),
                    Period = TimeSpan.FromMilliseconds(store.GetAndConvertValue<int>("Logging/PeriodMs"))
                };

                var logLevel = store.GetAndConvertValue<LogEventLevel>("Logging/LogLevel");
                var logType = store.GetAndConvertValue<string>("Logging/LogType");

                serviceCollection.AddLogging(builder => builder.AddPetProjectLogging(logLevel, sinkConfig, kafkaConfig, logType, true));
            }
        }

        private static void SetupConsul(IServiceCollection serviceCollection)
        {
            var consulClientConfig = Configuration.GetSection("ConsulClientConfiguration");
            var consulWatcherConfig = Configuration.GetSection("ConsulWatcherConfiguration");

            serviceCollection.AddSingleton<IInitialKeyValuesProvider<string>>(Configuration.ToInitialKeyValuesProvider());
            serviceCollection.AddSingleton<IWatcherConfiguration>(new WatcherConfiguration
            {
                BlockingQueryTimeout = TimeSpan.FromMilliseconds(consulWatcherConfig.GetValue<long>("BlockingQueryTimeoutMs")),
                DelayBetweenFailedRequests = TimeSpan.FromMilliseconds(consulWatcherConfig.GetValue<long>("DelayBetweenFailedRequestsMs"))
            });
            serviceCollection.AddSingleton<IConsulClientConfiguration>(new ConsulClientConfiguration
            {
                Address = consulClientConfig.GetValue<string>("Address"),
                ClientTimeout = TimeSpan.FromMilliseconds(consulClientConfig.GetValue<long>("ClientTimeoutMs"))
            });

            serviceCollection.AddSingleton<IStoreConfiguration>(Configuration.GetSection("ConsulStoreConfiguration").Get<StoreConfiguration>());
            serviceCollection.AddTransient<IConsulClientFactory, ConsulClientFactory>();
            serviceCollection.AddSingleton<Consul.IKVEndpoint>(sp => sp.GetRequiredService<IConsulClientFactory>().Create(sp.GetRequiredService<IConsulClientConfiguration>()).KV);
            serviceCollection.AddTransient<IKeyValueWatcher, ConsulKeyValueWatcher>();
            serviceCollection.AddSingleton<IStringKeyValueStore, ConsulStringKeyValueStore>();
        }

        private static void Run(IServiceProvider scopedProvider)
        {
            var logger = scopedProvider.GetService<ILoggerFactory>().CreateLogger<Program>();

            logger.LogCritical("Starting LogAggregator...");

            scopedProvider.StartPetProjectElasticLogConsumer();

            Console.CancelKeyPress += (sender, eArgs) =>
            {
                Program.QuitEvent.Set();
                eArgs.Cancel = true;
            };

            Program.QuitEvent.WaitOne();

            logger.LogCritical("LogAggregator Ended...");
        }
    }
}