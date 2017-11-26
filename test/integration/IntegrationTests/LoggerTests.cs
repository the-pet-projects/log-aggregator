namespace IntegrationTests
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;
    using PetProjects.Framework.Logging.Producer;
    using Serilog.Events;

    [TestClass]
    public class LoggerTests
    {
        private const int NumberOfAssertRetries = 40;
        private static readonly TimeSpan DelayBetweenAssertRetries = TimeSpan.FromMilliseconds(250);

        [TestMethod]
        public void ProduceLog_ValidLog_ElasticReturnsLog()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddLogging(builder => builder.AddPetProjectLogging(
                LogEventLevel.Verbose, 
                new PeriodicSinkConfiguration {BatchSizeLimit = 1,Period = TimeSpan.FromMilliseconds(5)}, 
                new KafkaConfiguration {Brokers = AppSettings.Current.Brokers.Split(','), Topic = AppSettings.Current.Topic},
                AssemblyInitialize.TypeName,
                true));

            using (var provider = services.BuildServiceProvider())
            {
                var logger = provider.GetService<ILoggerFactory>().CreateLogger<LoggerTests>();

                // Act
                logger.LogCritical("Teste 1231232131");

                // Assert
                for (var i = 0; i < LoggerTests.NumberOfAssertRetries; i++)
                {
                    var response = AssemblyInitialize.Client.Search<dynamic>(req => req.Index(AppSettings.Current.IndexName).Type(AssemblyInitialize.TypeName).Query(q => q.Type(t => t.Value(AssemblyInitialize.TypeName))));
                    var docs = response.Documents.Select(d => JsonConvert.SerializeObject(d));
                    try
                    {
                        Assert.IsTrue(docs.Any(d => d.Contains("Teste 1231232131")));
                        return;
                    }
                    catch (AssertFailedException)
                    {
                        Task.Delay(LoggerTests.DelayBetweenAssertRetries).Wait();
                    }
                }

                Assert.Fail("Reached maximum number of assert retries. Type = " + AssemblyInitialize.TypeName + ", Topic = " + AppSettings.Current.Topic + ", Brokers = " + AppSettings.Current.Brokers);
            }
        }
    }
}