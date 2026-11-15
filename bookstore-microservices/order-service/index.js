import express from "express";
import axios from "axios";
import db from "./db.js";
import { connectToBroker, publishMessage } from "./broker.js";

const app = express();
app.use(express.json());

// RabbitMQ
connectToBroker().catch((err) => console.error("Broker init error", err));

// Create order
app.post("/", async (req, res) => {
  // TODO: Implement order creation with the following steps:
  // 1. Validate request body:
  //    - Check productId exists
  //    - Check quantity is positive
  const { productId, quantity } = req.body;
  if (!productId || !quantity || Number(quantity) <= 0) {
    return res.status(400).json({
      error: "productId and a positive quantity are required.",
    });
  }

  let product;
  // 2. Call product service to verify product exists:
  //    - Use axios to GET product details
  //    - Handle timeouts and errors
  try {
    const productServiceUrl =
      process.env.PRODUCT_SERVICE_URL || "http://product-service:8002";
    // Check whether product-service uses /products/:id or /api/products/:id
    const response = await axios.get(`${productServiceUrl}/${productId}`, {
      timeout: 5000,
    });
    product = response.data;
  } catch (err) {
    if (err.response && err.response.status === 404) {
      return res.status(404).json({ error: `Product ${productId} not found.` });
    }
    console.error("Product verification failed:", err.message);
    return res
      .status(502)
      .json({ error: "Unable to verify product with product-service." });
  }

  try {
    // 3. Save new order to PostgreSQL database with PENDING status
    const result = await db.query(
      `INSERT INTO orders (product_id, quantity, status)
   VALUES ($1, $2, 'PENDING')
   RETURNING *`,
      [productId, Number(quantity)],
    );
    const newOrder = result.rows[0];

    // 4. Publish ORDER_CREATED event to RabbitMQ

    const message = {
      event: "ORDER_CREATED",
      orderId: newOrder.id,
      productId: newOrder.product_id,
      quantity: newOrder.quantity,
      product: product,
    };

    await publishMessage("order_events", message);
    console.log("Published ORDER_CREATED event to RabbitMQ:", message);

    // 5. Return created order
    return res.status(201).json(newOrder);
  } catch (err) {
    console.error("Failed to create order:", err.message);
    return res
      .status(500)
      .json({ error: "Internal server error while saving order." });
  }
});

// List orders
app.get("/", async (_req, res) => {
  const r = await db.query("SELECT * FROM orders ORDER BY id DESC");
  res.json(r.rows);
});

// Get order by id
app.get("/:id", async (req, res) => {
  const id = Number(req.params.id);
  const r = await db.query("SELECT * FROM orders WHERE id = $1", [id]);
  if (r.rows.length === 0)
    return res.status(404).json({ error: "Order not found" });
  res.json(r.rows[0]);
});

const PORT = 8003;
app.listen(PORT, () => console.log(`Order Service running on ${PORT}`));
