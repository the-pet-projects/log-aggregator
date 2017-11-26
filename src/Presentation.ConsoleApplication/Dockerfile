FROM microsoft/aspnetcore:1.1
ARG source
RUN echo "source: $source"

WORKDIR /app

COPY ${source:-/build} .
EXPOSE 80

ENV MTS_APP_SETTINGS_ConsulStoreConfiguration:Environment=${CONSUL_ENVIRONMENT}
ENV MTS_APP_SETTINGS_ConsulClientConfiguration:Address=${CONSUL_ADDRESS}

ENTRYPOINT ["dotnet", "Presentation.API.dll"]
