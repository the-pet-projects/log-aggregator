FROM microsoft/dotnet:2.0-runtime-jessie
ARG source
RUN echo "source: $source"

WORKDIR /app

COPY ${source:-/build} .
EXPOSE 80

ENV MTS_APP_SETTINGS_ConsulStoreConfiguration:Environment=${CONSUL_ENVIRONMENT}
ENV MTS_APP_SETTINGS_ConsulClientConfiguration:Address=${CONSUL_ADDRESS}

ENTRYPOINT ["dotnet", "PetProjects.LogAggregator.Presentation.ConsoleApplication.dll"]
