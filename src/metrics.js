const client = require("prom-client");
client.collectDefaultMetrics({ prefix: "hotelsync_reservas_" });
const httpRequestsTotal = new client.Counter({
  name: "hotelsync_reservas_http_requests_total",
  help: "Total de peticiones HTTP",
  labelNames: ["method", "route", "status_code"]
});
const httpRequestDuration = new client.Histogram({
  name: "hotelsync_reservas_http_request_duration_seconds",
  help: "Duracion de peticiones HTTP",
  labelNames: ["method", "route", "status_code"],
  buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
});
function metricsMiddleware(req, res, next) {
  const start = process.hrtime.bigint();
  res.on("finish", () => {
    const duration = Number(process.hrtime.bigint() - start) / 1e9;
    const route = req.route ? req.route.path : req.path;
    httpRequestsTotal.inc({ method: req.method, route, status_code: res.statusCode });
    httpRequestDuration.observe({ method: req.method, route, status_code: res.statusCode }, duration);
  });
  next();
}
module.exports = { client, metricsMiddleware };
