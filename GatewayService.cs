
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

/*
 * Listen for HTTP requests
 * If OPTION then pass through
 * Otherwise check for custom headers
 * Reject if headers are not present
 * Reject if headers are present but do not match any known service/endpoint
 * Proxy request via HttpClient HttpRequestMessage
 * Copy headers and status code
 * Copy body (conditionally)
 * 
 * 
 * HandleRequest
 * HandleRequestHEAD
 * HandleRequestORIGIN
 * 
 * TryGetRoutingInfo => service (endpoint?)
 * ProxyRequest(serviceUrl) => client, response
 * CopyProxyResponseHeadersAndStatus
 * CopyProxyResponseBody
 */

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

        _logger.LogInformation("Stopping... ({RequestsStillInFlight} requests still in flight)", MaxConcurrentRequests - _semaphore.CurrentCount);

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

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation("Gateway listening on: {Adress}", _listenAddress);
        }

        do
        {
            try
            {
                var context = await _listener.GetContextAsync().ConfigureAwait(false);
                _ = Task.Run(() => ProcessRequest(context), CancellationToken.None);
            }
            catch (HttpListenerException)
            {
                // Noop, exception only thrown by GetContextAsync when the listener is stopped 
                break;
            }
        }
        while (!stoppingToken.IsCancellationRequested);
    }

    private async Task ProcessRequest(HttpListenerContext context)
    {
        if (_shuttingDown) return;

        var timeout = TimeSpan.FromSeconds(30);
        bool didNotTimeOut = await _semaphore.WaitAsync(timeout);

        if (!didNotTimeOut)
        {
            _logger.LogWarning("Request timed out waiting to be processed ({Timeout})", timeout);
            return;
        }

        HttpListenerRequest request = context.Request;
        HttpListenerResponse response = context.Response;

        try 
        {
            Func<HttpListenerRequest, HttpListenerResponse, Task> requestHandler = request.HttpMethod switch
            {
                "OPTIONS" => HandleRequestOPTIONS,
                "HEAD" => HandleRequestHEAD,
                _ => HandleRequest
            };

            await requestHandler(request, response);
        }
        catch(Exception error)
        {
            _logger.LogError("HandleRequest failure: {Error}{StackTrace}", error.Message, Environment.NewLine + error.StackTrace);
            response.StatusCode = 502;
            response.Close();
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private async Task HandleRequestOPTIONS(HttpListenerRequest request, HttpListenerResponse response)
    {
        _logger.LogInformation($"PRE-FLIGHT: ({request.RemoteEndPoint}) {request.Url}");

        //response.AddHeader("Access-Control-Allow-Origin", "http://192.168.178.27");
        response.AddHeader("Access-Control-Allow-Origin", "*");
        response.AddHeader("Access-Control-Allow-Headers", AppTargetCustomHeader);
        response.AddHeader("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,PATCH");

        var proxyResponse = await ProxyRequest(request);
        CopyProxyResponseHeadersAndStatus(response, proxyResponse);

        await FinishRequest(request, response);
    }

    private async Task HandleRequestHEAD(HttpListenerRequest request, HttpListenerResponse response)
    {
        if (!TryGetRoutingInfo((WebHeaderCollection)request.Headers, out string serviceName))
        {
            await RejectRequest(request, response);
            return;
        }

        _logger.LogInformation($"ACCEPTED: ({serviceName}) ({request.RemoteEndPoint}) {request.HttpMethod} {request.Url}");

        var proxyResponse = await ProxyRequest(request);
        CopyProxyResponseHeadersAndStatus(response, proxyResponse);

        foreach (var header in proxyResponse.Content.Headers)
        {
            response.Headers[header.Key] = string.Join(",", header.Value);
        }

        await FinishRequest(request, response);
    }

    private async Task HandleRequest(HttpListenerRequest request, HttpListenerResponse response)
    {
        if (!TryGetRoutingInfo((WebHeaderCollection)request.Headers, out string serviceName))
        {
            await RejectRequest(request, response);
            return;
        }

        _logger.LogInformation($"ACCEPTED: ({serviceName}) ({request.RemoteEndPoint}) {request.HttpMethod} {request.Url}");

        var proxyResponse = await ProxyRequest(request);
        CopyProxyResponseHeadersAndStatus(response, proxyResponse);
        await CopyProxyResponseBody(response, proxyResponse);

        //Thread.Sleep(3000);
        await FinishRequest(request, response);
    }

    private async Task<HttpResponseMessage> ProxyRequest(HttpListenerRequest request)
    {
        HttpRequestMessage proxyRequest = CopyRequest(request);
        var httpClient = new HttpClient(_httpHandler, false);

        try
        {
            HttpResponseMessage proxyResponse = await httpClient
                .SendAsync(proxyRequest, HttpCompletionOption.ResponseHeadersRead)
                .ConfigureAwait(false);

            return proxyResponse;
        }
        finally
        {
            httpClient?.Dispose();
        }
    }

    private static void CopyProxyResponseHeadersAndStatus(HttpListenerResponse response, HttpResponseMessage proxyResponse)
    {
        response.StatusCode = (int)proxyResponse.StatusCode;
        response.ContentType = proxyResponse.Content.Headers.ContentType?.ToString();

        foreach (var header in proxyResponse.Headers)
        {
            response.Headers[header.Key] = string.Join(",", header.Value);
        }
    }

    private static async Task CopyProxyResponseBody(HttpListenerResponse response, HttpResponseMessage proxyResponse)
    {
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

    private Task RejectRequest(HttpListenerRequest request, HttpListenerResponse response)
    {
        _logger.LogWarning($"REJECTED: ({request.RemoteEndPoint}) {request.HttpMethod} {request.Url}");
        response.StatusCode = 418;
        response.Close();

        return Task.CompletedTask;
    }

    private Task FinishRequest(HttpListenerRequest request, HttpListenerResponse response)
    {
        _logger.LogInformation($"PROCESSED: ({request.RemoteEndPoint}) {request.HttpMethod} {request.Url}");
        response.Close();

        return Task.CompletedTask;
    }

    private static bool TryGetRoutingInfo(WebHeaderCollection headers, out string serviceName)
    {
        string[]? targetAppId = headers.GetValues(AppTargetCustomHeader.ToLowerInvariant());

        if ((targetAppId?.Length ?? 0) == 0)
        {
            serviceName = string.Empty;
            return false;
        }

        Debug.Assert(targetAppId is not null);
        serviceName = targetAppId[0];
        return true;
    }
}
