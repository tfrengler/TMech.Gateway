using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TMech.Gateway;

internal static class Program
{
    internal static int Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        builder.Services.AddHostedService<GatewayService>(s =>
        {
            var logger = s.GetRequiredService<ILogger<GatewayService>>();
            var config = s.GetRequiredService<IOptions<Config>>();
            return new GatewayService(logger, $"{config.Value.Host}:{config.Value.Port}/");
        })
        .AddLogging(configuration => configuration.AddSimpleConsole(config => config.TimestampFormat = "[yyyy-MM-dd HH:mm:ss] - "))
        .AddOptions<Config>()
        .Bind(builder.Configuration.GetSection("System"))
        .ValidateDataAnnotations()
        .ValidateOnStart();

        builder
            .Build()
            .Run();

        return 0;
    }
}
