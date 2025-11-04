const express = require("express");
const { MongoClient, ObjectId } = require("mongodb");
const app = express();
const port = process.env.PORT || 3000;

// Enhanced logging
console.log("Starting transactions service...");
console.log("MongoDB URI:", process.env.MONGO_URI || "mongodb://mongo:27017");

app.use(express.json());

// Logging middleware
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.url}`);
  next();
});

const mongoUri = process.env.MONGO_URI || "mongodb://mongo:27017";
const client = new MongoClient(mongoUri);

// Connection with retry logic
async function connectToMongo() {
  try {
    console.log("Attempting MongoDB connection...");
    await client.connect();
    await client.db().command({ ping: 1 });
    console.log("Successfully connected to MongoDB!");
  } catch (err) {
    console.error("MongoDB connection failed:", err);
    process.exit(1);
  }
}

connectToMongo();

// NEW ROUTE: Get all transactions for all users
app.get("/api/transactions", async (req, res) => {
  console.log("Handling request for all transactions");
  try {
    const db = client.db("bank_app");

    // Get all users with their transactions
    const users = await db.collection("users").find({}).toArray();

    console.log(`Found ${users.length} users`);

    // Format the response
    const allTransactions = users.map((user) => ({
      userId: user._id,
      firstName: user.first_name,
      lastName: user.last_name,
      email: user.email,
      balance: user.balance,
      transactions: user.transactions || [],
    }));

    console.log(`Returning transactions for ${allTransactions.length} users`);
    res.json(allTransactions);
  } catch (err) {
    console.error("Error processing request:", err);
    res.status(500).json({ error: "Server Error", details: err.message });
  }
});

// EXISTING ROUTE: Get transactions for specific user
app.get("/api/transactions/:userId", async (req, res) => {
  console.log(`Handling request for user: ${req.params.userId}`);
  try {
    const db = client.db("bank_app");

    // Validate user ID
    let userId;
    try {
      userId = new ObjectId(req.params.userId);
    } catch (err) {
      console.error("Invalid user ID format:", err);
      return res.status(400).json({ error: "Invalid user ID format" });
    }

    // Check user exists
    const user = await db.collection("users").findOne({ _id: userId });
    if (!user) {
      console.log("User not found in database");
      return res.status(404).json({ error: "User not found" });
    }

    console.log(
      `Found user with ${user.transactions?.length || 0} transactions`
    );

    // Process transactions by month
    const transactions = await db
      .collection("users")
      .aggregate([
        { $match: { _id: userId } },
        { $unwind: "$transactions" },
        {
          $addFields: {
            "transactions.date": {
              $cond: {
                if: { $eq: [{ $type: "$transactions.date" }, "string"] },
                then: { $toDate: "$transactions.date" },
                else: "$transactions.date",
              },
            },
          },
        },
        {
          $group: {
            _id: {
              year: { $year: "$transactions.date" },
              month: { $month: "$transactions.date" },
            },
            transactions: {
              $push: {
                type: "$transactions.type",
                amount: "$transactions.amount",
                date: "$transactions.date",
              },
            },
          },
        },
        { $sort: { "_id.year": -1, "_id.month": -1 } },
      ])
      .toArray();

    console.log(`Returning ${transactions.length} transaction groups`);
    res.json(transactions);
  } catch (err) {
    console.error("Error processing request:", err);
    res.status(500).json({ error: "Server Error", details: err.message });
  }
});

// Health check endpoint
app.get("/health", (req, res) => {
  res.json({ status: "ok", service: "transactions" });
});

app.listen(port, () => {
  console.log(`Transactions service running on http://localhost:${port}`);
});

// Handle graceful shutdown
process.on("SIGTERM", async () => {
  console.log("SIGTERM received, closing MongoDB connection");
  await client.close();
  process.exit(0);
});
