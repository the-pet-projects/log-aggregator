FROM microsoft/dotnet:2.0-runtime
ARG source
RUN echo "source: $source"

WORKDIR /app

COPY ${source:-/build} .
EXPOSE 80

ENTRYPOINT ["dotnet", "PetProjects.LogAggregator.Presentation.ConsoleApplication.dll"]
