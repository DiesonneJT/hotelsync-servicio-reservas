require("dotenv").config();
const express = require("express");
const cors = require("cors");

const reservaRoutes = require("./routes/reservaRoutes");
const { connect: connectRabbit } = require("./eventos/rabbitmq");
const { metricsMiddleware, client } = require("./metrics");

const app = express();
app.use(cors());
app.use(express.json());
app.use(metricsMiddleware);

app.get("/health", (req, res) => res.json({ estado: "ok", servicio: "reservas" }));
app.get("/metrics", async (req, res) => {
  res.set("Content-Type", client.register.contentType);
  res.end(await client.register.metrics());
});
app.use("/api", reservaRoutes);

const PUERTO = process.env.PORT || 3002;

connectRabbit()
  .then(() => {
    app.listen(PUERTO, () => {
      console.log(`[reservas] Escuchando en http://localhost:${PUERTO}`);
    });
  })
  .catch((err) => {
    console.error("[reservas] Error al iniciar:", err.message);
    app.listen(PUERTO, () => {
      console.log(`[servicio-reservas] Escuchando en http://localhost:${PUERTO} (sin RabbitMQ)`);
    });
  });
