
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace TMech.Gateway;

public sealed class GatewayService : BackgroundService
{
    const int MaxConcurrentRequests = 8;
    const string AppTargetCustomHeader = "X-Gateway-Target-AppId";

    private readonly HttpListener _listener;
    private readonly string _listenAddress;
    private readonly SemaphoreSlim _semaphore;
    private readonly ILogger<GatewayService> _logger;
    private bool _shuttingDown = false;
    private readonly SocketsHttpHandler _httpHandler;

    public GatewayService(ILogger<GatewayService> logger, string listenAddress)
    {
        _semaphore = new SemaphoreSlim(MaxConcurrentRequests);
        _listenAddress = listenAddress;
        _logger = logger;
        _listener = new HttpListener();
        _httpHandler = new SocketsHttpHandler();
    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        _shuttingDown = true;
        TimeSpan timeout = TimeSpan.FromSeconds(5);
        var timeoutTimer = Stopwatch.StartNew();

        while (timeoutTimer.Elapsed < timeout && _semaphore.CurrentCount != MaxConcurrentRequests)
        {
            await Task.Delay(100);
        }

        _listener.Close();
        await base.StopAsync(stoppingToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _listener.Prefixes.Add(_listenAddress);
        _listener.Start();
        _ = Receive();

        _logger.LogInformation("Gateway listening on: {Adress}", _listenAddress);
        
        while (!stoppingToken.IsCancellationRequested) { }
    }

    private async Task Receive()
    {
        while (!_shuttingDown)
        {
            try
            {
                var context = await _listener.GetContextAsync();
                _ = Task.Run(() => ProcessRequest(context));
            }
            catch(HttpListenerException)
            {
                break;
            }
        }
    }

    private async Task ProcessRequest(HttpListenerContext context)
    {
        if (_shuttingDown) return;
        bool didNotTimeOut = await _semaphore.WaitAsync(TimeSpan.FromSeconds(30));

        if (!didNotTimeOut)
        {
            _logger.LogWarning("Request timed out waiting to be processed");
            return;
        }

        var request = context.Request;
        var response = context.Response;

        try 
        {
            if (request.HttpMethod != "OPTIONS")
            {
                string[]? targetAppId = request.Headers.GetValues(AppTargetCustomHeader);

                if (targetAppId is null || targetAppId?.Length == 0)
                {
                    _logger.LogWarning($"REJECTED: ({request.RemoteEndPoint}) {request.HttpMethod} {request.Url}");
                    response.StatusCode = 418;
                    response.Close();

                    return;
                }
            }

            _logger.LogInformation($"ACCEPTED: ({request.RemoteEndPoint}) {request.HttpMethod} {request.Url}");
            await ProxyRequest(request, response);
        }
        catch(Exception error)
        {
            _logger.LogError("PreprocessRequest failure: {Error} {StackTrace}", error.Message, Environment.NewLine + error.StackTrace);
            response.StatusCode = 502;
        }
        finally
        {
            response.Close();
            _semaphore.Release();
        }
    }

    private async Task ProxyRequest(HttpListenerRequest request, HttpListenerResponse response)
    {
        HttpRequestMessage proxyRequest = CopyRequest(request);
        HttpResponseMessage proxyResponse = null!;
        var httpClient = new HttpClient(_httpHandler, false);

        try
        {
            proxyResponse = await httpClient
                .SendAsync(proxyRequest, HttpCompletionOption.ResponseHeadersRead)
                .ConfigureAwait(false);

            await CopyResponse(response, proxyResponse);

            response.AddHeader("Access-Control-Allow-Origin", "*");
            response.AddHeader("Access-Control-Allow-Headers", "x-gateway-target-appId");
        }
        finally
        {
            httpClient?.Dispose();
            proxyResponse?.Dispose();
        }
    }

    private async Task CopyResponse(HttpListenerResponse response, HttpResponseMessage proxyResponse)
    {
        response.StatusCode = (int)proxyResponse.StatusCode;

        foreach (var header in proxyResponse.Headers)
        {
            response.Headers[header.Key] = string.Join(",", header.Value);
        }

        response.ContentType = proxyResponse.Content.Headers.ContentType?.ToString();

        using (Stream proxyResponseStream = await proxyResponse.Content.ReadAsStreamAsync())
        {
            await proxyResponseStream.CopyToAsync(response.OutputStream);
        }
    }

    private HttpRequestMessage CopyRequest(HttpListenerRequest request)
    {
        HttpMethod method = request.HttpMethod switch
        {
            "GET" => HttpMethod.Get,
            "POST" => HttpMethod.Post,
            "PUT" => HttpMethod.Put,
            "DELETE" => HttpMethod.Delete,
            "OPTIONS" => HttpMethod.Options,
            "PATCH" => HttpMethod.Patch,
            "HEAD" => HttpMethod.Head,
            _ => throw new NotImplementedException("Wrong or not caught by Thomas http method?")
        };

        var returnData = new HttpRequestMessage()
        {
            Method = method,
            RequestUri = new Uri("http://localhost:5000" + request.RawUrl)
        };

        foreach(string headerName in request.Headers)
        {
            string? headerValues = request.Headers[headerName];
            bool headerAdded = returnData.Headers.TryAddWithoutValidation(headerName, headerValues);

            if (!headerAdded) {
                _logger.LogWarning("Header could not be added: {Name} | {Values}", headerName, headerValues);
            }
        }

        if (request.HasEntityBody)
        {
            returnData.Content = new StreamContent(request.InputStream);
        }

        return returnData;
    }
}
