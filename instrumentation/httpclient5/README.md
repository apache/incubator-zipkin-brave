# brave-instrumentation-httpclient5
This module contains a tracing decorator for [Apache HttpClient](http://hc.apache.org/httpcomponents-client-5.0.x/index.html) 5.0+.
`HttpClient5Tracing` adds trace headers to outgoing requests. It
then reports to Zipkin how long each request takes, along with relevant
tags like the http url.

To enable tracing, create your client using `HttpClient5Tracing`.

```java
HttpClientBuilder httpClientBuilder = HttpClients.custom();
httpclient = HttpClient5Tracing.newBuilder(httpTracing).create(httpClientBuilder);
```

`HttpClient5Tracing` also supports `CachingHttpClientBuilder`, `HttpAsyncClientBuilder`,
 `CachingHttpAsyncClientBuilder`, `H2AsyncClientBuilder` and  `CachingH2AsyncClientBuilder`.
