FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Cspi.NewsCollector.csproj ./
RUN dotnet restore Cspi.NewsCollector.csproj
COPY . ./
RUN dotnet publish Cspi.NewsCollector.csproj -c Release -o /out --no-restore
FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /out .
ENTRYPOINT ["dotnet", "Cspi.NewsCollector.dll"]
