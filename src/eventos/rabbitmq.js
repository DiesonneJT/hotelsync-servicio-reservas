const amqp = require("amqplib");

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://localhost";
const EXCHANGE = "hotelsync.events";

let connection = null;
let channel = null;
const suscripciones = [];

async function connect() {
  if (channel) return { connection, channel };
  connection = await amqp.connect(RABBITMQ_URL);
  connection.on("error", (err) => {
    console.error("[rabbitmq] Error de conexion:", err.message);
  });
  connection.on("close", () => {
    console.warn("[rabbitmq] Conexion cerrada. Reintentando en 5s...");
    channel = null;
    connection = null;
    setTimeout(reconectarSuscripciones, 5000);
  });

  channel = await connection.createChannel();
  await channel.assertExchange(EXCHANGE, "topic", { durable: true });
  console.log(`[rabbitmq] Conectado a ${RABBITMQ_URL}`);
  return { connection, channel };
}

async function reconectarSuscripciones() {
  for (const sub of suscripciones) {
    await registrarConsumidor(sub.routingKey, sub.handler);
  }
}

async function publish(routingKey, payload) {
  try {
    const { channel: ch } = await connect();
    const buffer = Buffer.from(JSON.stringify(payload));
    ch.publish(EXCHANGE, routingKey, buffer, { persistent: true });
    console.log(`[rabbitmq] Publicado ${routingKey}`, payload);
  } catch (error) {
    channel = null;
    connection = null;
    console.error(`[rabbitmq] Error publicando ${routingKey}:`, error.message);
  }
}

async function registrarConsumidor(routingKey, handler) {
  try {
    const { channel: ch } = await connect();
    const { queue } = await ch.assertQueue("", { exclusive: true });
    await ch.bindQueue(queue, EXCHANGE, routingKey);
    await ch.consume(queue, async (msg) => {
      if (!msg) return;
      try {
        const payload = JSON.parse(msg.content.toString());
        await handler(payload);
        ch.ack(msg);
      } catch (error) {
        console.error(`[rabbitmq] Error procesando ${routingKey}:`, error.message);
        ch.nack(msg, false, false);
      }
    });
    console.log(`[rabbitmq] Consumiendo ${routingKey} en cola ${queue}`);
  } catch (error) {
    channel = null;
    connection = null;
    console.error(`[rabbitmq] Error suscribiendose a ${routingKey} (reintentando en 5s):`, error.message);
    setTimeout(() => registrarConsumidor(routingKey, handler), 5000);
  }
}

async function consume(routingKey, handler) {
  if (!suscripciones.some((s) => s.routingKey === routingKey && s.handler === handler)) {
    suscripciones.push({ routingKey, handler });
  }
  await registrarConsumidor(routingKey, handler);
}

module.exports = { connect, publish, consume };
