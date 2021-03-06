﻿namespace PetProjects.LogAggregator.Presentation.ConsoleApplication
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;
    using PetProjects.Framework.Consul;
    using PetProjects.Framework.Consul.Store;
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
            Program.SetupServices(serviceCollection, GetConfigurationKeyValueStore());

            // Do the actual work here
            using (var parentServiceProvider = serviceCollection.BuildServiceProvider())
            {
                Program.Run(parentServiceProvider);
            }
        }
        
        private static IStringKeyValueStore GetConfigurationKeyValueStore()
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddPetProjectConsulServices(Program.Configuration, true);
            serviceCollection.AddSingleton<ILogger>(NullLogger.Instance);

            using (var tempProvider = serviceCollection.BuildServiceProvider())
            {
                return tempProvider.GetRequiredService<IStringKeyValueStore>();
            }
        }

        private static void SetupConfiguration()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
                .AddEnvironmentVariables("MTS_APP_SETTINGS_");

            Configuration = builder.Build();
        }

        private static void SetupServices(IServiceCollection serviceCollection, IStringKeyValueStore store)
        {
            SetupLogging(serviceCollection, store);

            var kafkaConfig = new KafkaConfiguration
            {
                Brokers = store.GetAndConvertValue<string>("KafkaConfiguration/Brokers").Split(','),
                Topic = store.GetAndConvertValue<string>("KafkaConfiguration/Topic"),
                ConsumerGroupId = store.GetAndConvertValue<string>("KafkaConfiguration/ConsumerGroupId")
            };

            var elasticConfig = new ElasticClientConfiguration
            {
                Address = store.GetAndConvertValue<string>("ElasticConfiguration/Address")
            };

            // this call must happen after previous two (addsingleton of kafkaconfig + elasticconfig)
            serviceCollection.AddPetProjectElasticLogConsumer(kafkaConfig, elasticConfig);
        }

        private static void SetupLogging(IServiceCollection serviceCollection, IStringKeyValueStore store)
        {
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

            serviceCollection.AddLogging(builder => builder.AddPetProjectLogging(logLevel, sinkConfig, kafkaConfig, logType, true).AddConsole());
            serviceCollection.TryAddSingleton<ILogger>(sp => sp.GetRequiredService<ILoggerFactory>().CreateLogger("No category"));
        }

        private static void Run(IServiceProvider scopedProvider)
        {
            var logger = scopedProvider.GetRequiredService<ILogger<Program>>();

            logger.LogCritical("Starting LogAggregator...");

            try
            {
                IEnumerable<Task> tasks;
                using (var newScope = scopedProvider.CreateScope())
                {
                    tasks = newScope.ServiceProvider.StartPetProjectElasticLogConsumerAsync();

                    Console.CancelKeyPress += (sender, eArgs) =>
                    {
                        Program.QuitEvent.Set();
                        eArgs.Cancel = true;
                    };

                    Program.QuitEvent.WaitOne();
                }

                Task.WhenAll(tasks).Wait();
            }
            catch (Exception ex)
            {
                logger.LogCritical(ex, "Fatal Exception occured.");
            }
            
            logger.LogCritical("LogAggregator Ended...");

            // wait 2 seconds for previous log to reach the sink
            Task.Delay(TimeSpan.FromMilliseconds(2000)).Wait();
        }
    }
}