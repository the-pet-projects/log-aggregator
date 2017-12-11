namespace IntegrationTests
{
    using System;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Nest;

    [TestClass]
    public class AssemblyInitialize
    {
        public static string TypeName = Guid.NewGuid().ToString();
        public static ElasticClient Client;
        public static string IndexName;

        [AssemblyInitialize]
        public static void AssemblyInit(TestContext context)
        {
            Client = new ElasticClient(new Uri(AppSettings.Current.ElasticEndpoint));

            IndexName = $"logs-{ AppSettings.Current.Topic.ToLowerInvariant() }-{ DateTime.UtcNow.ToString("dd-MM-yyyy") }";

            if (Client.TypeExists(IndexName, AssemblyInitialize.TypeName).Exists)
            {
                Client.DeleteByQuery<dynamic>(q => q.Index(IndexName).Type(AssemblyInitialize.TypeName).Query(rq => rq.Type(f => f.Value(AssemblyInitialize.TypeName))));
            }
        }

        [AssemblyCleanup]
        public static void AssemblyCleanup()
        {
            if (Client.TypeExists(IndexName, AssemblyInitialize.TypeName).Exists)
            {
                Client.DeleteByQuery<dynamic>(q => q.Index(IndexName).Type(AssemblyInitialize.TypeName).Query(rq => rq.Type(f => f.Value(AssemblyInitialize.TypeName))));
            }
        }
    }
}