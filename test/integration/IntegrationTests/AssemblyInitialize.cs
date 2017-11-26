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

        [AssemblyInitialize]
        public static void AssemblyInit(TestContext context)
        {
            Client = new ElasticClient(new Uri(AppSettings.Current.ElasticEndpoint));

            if (Client.TypeExists(AppSettings.Current.IndexName, AssemblyInitialize.TypeName).Exists)
            {
                Client.DeleteByQuery<dynamic>(q => q.Index(AppSettings.Current.IndexName).Type(AssemblyInitialize.TypeName).Query(rq => rq.Type(f => f.Value(AssemblyInitialize.TypeName))));
            }
        }

        [AssemblyCleanup]
        public static void AssemblyCleanup()
        {
            if (Client.TypeExists(AppSettings.Current.IndexName, AssemblyInitialize.TypeName).Exists)
            {
                Client.DeleteByQuery<dynamic>(q => q.Index(AppSettings.Current.IndexName).Type(AssemblyInitialize.TypeName).Query(rq => rq.Type(f => f.Value(AssemblyInitialize.TypeName))));
            }
        }
    }
}