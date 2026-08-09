require("dotenv").config();

// Fixes "Do not know how to serialize a BigInt" errors anywhere res.json() is used.
BigInt.prototype.toJSON = function () {
  return this.toString();
};

const express = require("express");
const cors = require("cors");
const cron = require("node-cron");
const { createClient } = require("@supabase/supabase-js");
const {
  initiateDeveloperControlledWalletsClient,
  generateEntitySecretCiphertext,
} = require("@circle-fin/developer-controlled-wallets");
const { createGatewayMiddleware } = require("@circle-fin/x402-batching/server");

const app = express();
app.use(cors({ origin: "*" }));
app.use(express.json());

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

function getCircleClient() {
  return initiateDeveloperControlledWalletsClient({
    apiKey: process.env.CIRCLE_API_KEY,
    entitySecret: process.env.CIRCLE_ENTITY_SECRET,
  });
}

const ARC_TESTNET_USDC = "0x3600000000000000000000000000000000000000";
const PORT = process.env.PORT || 3001;

// --- x402 demo (seller side) ---
const X402_SELLER_WALLET = process.env.X402_SELLER_WALLET || "0x48f40e29eb0aef155c3dac794d7a34d95bddc918";
const x402Gateway = createGatewayMiddleware({
  sellerAddress: X402_SELLER_WALLET,
  facilitatorUrl: "https://gateway-api-testnet.circle.com",
  networks: ["eip155:5042002"], // Arc Testnet
});

function normalizeAddress(addr) {
  return addr ? String(addr).toLowerCase() : null;
}

async function executeRule(rule) {
  console.log(`\n[SCHEDULER] Executing rule: ${rule.id}`);
  try {
    const { data: agent } = await supabase.from("agents").select("*").eq("id", rule.agent_id).single();
    if (!agent) { console.log(`[SKIP] Agent not found`); return; }
    if (agent.status !== "active") { console.log(`[SKIP] Agent not active`); return; }
    if (!rule.circle_wallet_id) { console.log(`[SKIP] No Circle wallet linked`); return; }

    const userWallet = normalizeAddress(agent.owner_address || agent.wallet_address);
    if (userWallet) {
      const { data: bal } = await supabase
        .from("user_balances")
        .select("*")
        .eq("wallet_address", userWallet)
        .single();

      if (!bal || parseFloat(bal.balance) < parseFloat(rule.amount)) {
        console.log(`[BALANCE] Insufficient balance for ${userWallet} - auto pausing agent`);
        await supabase.from("agents").update({ status: "paused" }).eq("id", agent.id);
        await supabase.from("payment_rules").update({ is_active: false, status: "paused" }).eq("agent_id", agent.id);
        console.log(`[AUTO-PAUSE] Agent "${agent.name}" paused - balance insufficient`);
        return;
      }
    }

    const client = getCircleClient();
    console.log(`[TX] Sending ${rule.amount} USDC to ${rule.recipient_address}`);

    const txResponse = await client.createTransaction({
      blockchain: "ARC-TESTNET",
      walletId: rule.circle_wallet_id,
      destinationAddress: rule.recipient_address,
      amount: [rule.amount.toString()],
      tokenAddress: ARC_TESTNET_USDC,
      fee: { type: "level", config: { feeLevel: "MEDIUM" } },
    });

    const txId = txResponse.data?.id;
    if (!txId) throw new Error("No transaction ID returned");

    const terminalStates = new Set(["COMPLETE", "FAILED", "CANCELLED", "DENIED"]);
    let state = txResponse.data?.state;
    let txHash = null;

    while (!state || !terminalStates.has(state)) {
      await new Promise((r) => setTimeout(r, 3000));
      const poll = await client.getTransaction({ id: txId });
      const tx = poll.data?.transaction;
      state = tx?.state;
      txHash = tx?.txHash || null;
      console.log(`[TX] State: ${state}`);
    }

    if (state === "COMPLETE") {
      await logTransaction(rule, agent, "success", null, txHash);

      try {
        if (userWallet) {
          const { data: bal } = await supabase.from("user_balances").select("*").eq("wallet_address", userWallet).single();
          if (bal) {
            const newBalance = Math.max(0, parseFloat(bal.balance) - parseFloat(rule.amount));
            await supabase.from("user_balances").update({
              balance: newBalance,
              total_spent: parseFloat(bal.total_spent) + parseFloat(rule.amount),
              updated_at: new Date().toISOString(),
            }).eq("wallet_address", userWallet);
            console.log(`[BALANCE] Deducted ${rule.amount} USDC. New balance: ${newBalance}`);
          }
        }
      } catch (e) {
        console.error("[BALANCE ERROR]", e.message);
      }

      await supabase.from("payment_rules").update({
        last_executed_at: new Date().toISOString(),
        execution_count: (rule.execution_count || 0) + 1,
      }).eq("id", rule.id);
      console.log(`[SUCCESS] TX: ${txHash}`);
    } else {
      await logTransaction(rule, agent, "failed", `Transaction state: ${state}`);
    }
  } catch (err) {
    console.error(`[ERROR]`, err.message);
    await logTransaction(rule, null, "failed", err.message);
  }
}

async function logTransaction(rule, agent, status, errorMsg = null, txHash = null) {
  await supabase.from("transactions").insert({
    agent_id: rule.agent_id, rule_id: rule.id,
    from_address: agent?.wallet_address || "circle-wallet",
    to_address: rule.recipient_address, amount: rule.amount,
    status, tx_hash: txHash, error_message: errorMsg,
    type: "scheduled", created_at: new Date().toISOString(),
  });
}

function isRuleDue(rule, now) {
  const lastRun = rule.last_executed_at ? new Date(rule.last_executed_at) : null;
  if (!lastRun) return true;
  const diffHours = (now - lastRun) / (1000 * 60 * 60);
  switch (rule.interval) {
    case "hourly": return diffHours >= 1;
    case "every6h": return diffHours >= 6;
    case "every12h": return diffHours >= 12;
    case "daily": return diffHours >= 24;
    case "weekly": return diffHours >= 168;
    case "monthly": return diffHours >= 720;
    default: return false;
  }
}

async function checkAndRunDueRules() {
  console.log(`\n[CRON] Checking at ${new Date().toISOString()}`);
  try {
    const { data: rules } = await supabase.from("payment_rules").select("*").eq("is_active", true).eq("status", "active");
    if (!rules || rules.length === 0) { console.log("[CRON] No active rules."); return; }
    const now = new Date();
    for (const rule of rules) {
      if (isRuleDue(rule, now)) await executeRule(rule);
    }
  } catch (err) {
    console.error("[CRON ERROR]", err.message);
  }
}

cron.schedule("*/5 * * * *", checkAndRunDueRules);

app.get("/health", (req, res) => res.json({ status: "ok", timestamp: new Date().toISOString(), network: "Arc Testnet" }));

// x402 demo: paid endpoint. Returns a live-looking gold price for $0.001 USDC.
// Unpaid requests automatically get a 402 Payment Required response from the gateway middleware.
async function fetchRealGoldPrice() {
  try {
    const res = await fetch("https://api.goldprice.dev/v1/prices?symbol=XAU-USD-SPOT", {
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) throw new Error(`Gold price API returned ${res.status}`);
    const data = await res.json();
    const entry = data.symbols?.[0];
    const price = parseFloat(entry?.price);
    if (!price || isNaN(price)) throw new Error("Invalid price in response");
    return { price: price.toFixed(2), source: "live", computed_at: entry.computed_at };
  } catch (err) {
    console.error("[GOLD PRICE] Falling back to estimate:", err.message);
    // Fallback so the demo never breaks if the external API is briefly down.
    const price = (2600 + Math.random() * 40).toFixed(2);
    return { price, source: "estimate", computed_at: new Date().toISOString() };
  }
}

app.post("/api/x402-demo/gold-price", x402Gateway.require("$0.001"), async (req, res) => {
  const payment = req.payment;
  const gold = await fetchRealGoldPrice();
  res.json({
    metal: "gold",
    price_usd_per_oz: gold.price,
    price_source: gold.source, // "live" (real market data) or "estimate" (fallback)
    price_computed_at: gold.computed_at,
    timestamp: new Date().toISOString(),
    paid: {
      amount: payment?.amount ? `${payment.amount} (raw units)` : "$0.001",
      payer: payment?.payer,
      network: payment?.network,
      transaction: payment?.transaction,
    },
  });
});

function getX402BuyerClient() {
  const { GatewayClient } = require("@circle-fin/x402-batching/client");
  return new GatewayClient({
    chain: "arcTestnet",
    privateKey: process.env.X402_BUYER_PRIVATE_KEY,
  });
}

// One-time setup: moves USDC from the buyer wallet's regular balance into its
// Gateway balance. Must be called once (with funds already in the wallet)
// before /fetch-gold-price will work. Safe to call again to top up later.
app.post("/api/x402-demo/setup-buyer", async (req, res) => {
  try {
    const amount = req.body?.amount || "1";
    const gateway = getX402BuyerClient();
    const result = await gateway.deposit(amount);
    res.json({
      success: true,
      buyerAddress: gateway.address,
      ...result,
      amount: result.amount?.toString(),
    });
  } catch (err) {
    console.error("[X402 SETUP ERROR]", err.message);
    res.status(500).json({ error: err.message });
  }
});

// The actual demo: the buyer wallet automatically pays the paywalled
// gold-price endpoint and returns the real data. This is the full
// "agent hits a 402, pays, gets the resource" loop, fully automatic.
app.post("/api/x402-demo/fetch-gold-price", async (req, res) => {
  try {
    const gateway = getX402BuyerClient();
    const url = `${req.protocol}://${req.get("host")}/api/x402-demo/gold-price`;
    const result = await gateway.pay(url, { method: "POST" });
    res.json({
      success: true,
      buyerAddress: gateway.address,
      data: result.data,
      amountPaid: result.amount?.toString(),
      formattedAmount: result.formattedAmount,
      transaction: result.transaction,
    });
  } catch (err) {
    console.error("[X402 PAY ERROR]", err);
    res.status(500).json({
      error: err.message,
      cause: err.cause ? String(err.cause) : undefined,
      causeMessage: err.cause?.message,
      responseData: err.response?.data,
      details: err.details,
      stack: err.stack?.split("\n").slice(0, 5),
    });
  }
});

app.get("/api/generate-ciphertext", async (req, res) => {
  try {
    const ciphertext = await generateEntitySecretCiphertext(process.env.CIRCLE_API_KEY, process.env.CIRCLE_ENTITY_SECRET);
    res.json({ ciphertext });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ───────── AGENTS (owner-filtered) ─────────

app.get("/api/agents", async (req, res) => {
  const owner = normalizeAddress(req.query.owner);
  if (!owner) return res.json([]); // no owner = no data, never return everyone's agents
  const { data, error } = await supabase
    .from("agents")
    .select("*")
    .eq("owner_address", owner)
    .order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post("/api/agents", async (req, res) => {
  const { name, description, wallet_address, alert_threshold, status, owner_address } = req.body;
  const owner = normalizeAddress(owner_address);
  if (!owner) return res.status(400).json({ error: "owner_address is required" });
  const { data, error } = await supabase.from("agents").insert({
    name, description, wallet_address,
    alert_threshold: alert_threshold || 10,
    status: status || "active",
    owner_address: owner,
    created_at: new Date().toISOString(),
  }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.put("/api/agents/:id", async (req, res) => {
  const owner = normalizeAddress(req.query.owner || req.body.owner_address);
  if (!owner) return res.status(400).json({ error: "owner is required" });
  const { data: existing } = await supabase.from("agents").select("owner_address").eq("id", req.params.id).single();
  if (!existing || existing.owner_address !== owner) return res.status(403).json({ error: "Not authorized to edit this agent" });
  const { owner_address, ...updateBody } = req.body;
  const { data, error } = await supabase.from("agents").update(updateBody).eq("id", req.params.id).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.delete("/api/agents/:id", async (req, res) => {
  const owner = normalizeAddress(req.query.owner);
  if (!owner) return res.status(400).json({ error: "owner is required" });
  const { data: existing } = await supabase.from("agents").select("owner_address").eq("id", req.params.id).single();
  if (!existing || existing.owner_address !== owner) return res.status(403).json({ error: "Not authorized to delete this agent" });
  const { error } = await supabase.from("agents").delete().eq("id", req.params.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ───────── RULES (owner-filtered) ─────────

app.get("/api/rules", async (req, res) => {
  const owner = normalizeAddress(req.query.owner);
  const { agent_id } = req.query;
  if (!owner) return res.json([]);
  let query = supabase.from("payment_rules").select("*, agents(name, wallet_address)").eq("owner_address", owner).order("created_at", { ascending: false });
  if (agent_id) query = query.eq("agent_id", agent_id);
  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post("/api/rules", async (req, res) => {
  const { agent_id, name, amount, interval, recipient_address, circle_wallet_id, owner_address } = req.body;
  const owner = normalizeAddress(owner_address);
  if (!owner) return res.status(400).json({ error: "owner_address is required" });

  // Verify the agent actually belongs to this owner
  const { data: agent } = await supabase.from("agents").select("owner_address").eq("id", agent_id).single();
  if (!agent || agent.owner_address !== owner) return res.status(403).json({ error: "Not authorized to create rules for this agent" });

  const { data, error } = await supabase.from("payment_rules").insert({
    agent_id, name, amount, interval, recipient_address,
    circle_wallet_id: circle_wallet_id || null,
    owner_address: owner,
    is_active: true, status: "active", execution_count: 0,
    created_at: new Date().toISOString(),
  }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.patch("/api/rules/:id/toggle", async (req, res) => {
  const owner = normalizeAddress(req.query.owner);
  if (!owner) return res.status(400).json({ error: "owner is required" });
  const { data: rule } = await supabase.from("payment_rules").select("is_active, owner_address").eq("id", req.params.id).single();
  if (!rule || rule.owner_address !== owner) return res.status(403).json({ error: "Not authorized" });
  const { data, error } = await supabase.from("payment_rules")
    .update({ is_active: !rule.is_active, status: !rule.is_active ? "active" : "paused" })
    .eq("id", req.params.id).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.delete("/api/rules/:id", async (req, res) => {
  const owner = normalizeAddress(req.query.owner);
  if (!owner) return res.status(400).json({ error: "owner is required" });
  const { data: rule } = await supabase.from("payment_rules").select("owner_address").eq("id", req.params.id).single();
  if (!rule || rule.owner_address !== owner) return res.status(403).json({ error: "Not authorized" });
  const { error } = await supabase.from("payment_rules").delete().eq("id", req.params.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.post("/api/rules/:id/execute", async (req, res) => {
  const owner = normalizeAddress(req.query.owner);
  const { data: rule, error } = await supabase.from("payment_rules").select("*").eq("id", req.params.id).single();
  if (error || !rule) return res.status(404).json({ error: "Rule not found" });
  if (owner && rule.owner_address !== owner) return res.status(403).json({ error: "Not authorized" });
  executeRule(rule);
  res.json({ success: true, message: "Payment execution triggered!" });
});

// ───────── TRANSACTIONS (owner-filtered via agent ownership) ─────────

app.get("/api/transactions", async (req, res) => {
  const owner = normalizeAddress(req.query.owner);
  const { agent_id, limit = 50 } = req.query;
  if (!owner) return res.json([]);

  const { data: ownedAgents } = await supabase.from("agents").select("id").eq("owner_address", owner);
  const ownedIds = (ownedAgents || []).map(a => a.id);
  if (ownedIds.length === 0) return res.json([]);

  let query = supabase.from("transactions").select("*, agents(name)").in("agent_id", ownedIds).order("created_at", { ascending: false }).limit(limit);
  if (agent_id) query = query.eq("agent_id", agent_id);
  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.get("/api/circle-wallets", async (req, res) => {
  try {
    const client = getCircleClient();
    const response = await client.listWallets({ blockchain: "ARC-TESTNET" });
    res.json(response.data?.wallets || []);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/setup-circle-wallet", async (req, res) => {
  try {
    const client = getCircleClient();
    const walletSetResponse = await client.createWalletSet({ name: "Arc Agent Pay Wallet Set" });
    const walletSetId = walletSetResponse.data?.walletSet?.id;
    if (!walletSetId) throw new Error("Wallet set creation failed");
    const walletResponse = await client.createWallets({
      walletSetId, blockchains: ["ARC-TESTNET"], count: 1, accountType: "EOA",
    });
    const wallet = walletResponse.data?.wallets?.[0];
    if (!wallet) throw new Error("Wallet creation failed");
    res.json({ success: true, walletSetId, walletId: wallet.id, walletAddress: wallet.address });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ───────── Balance & Deposit (already per-wallet) ─────────

app.get("/api/balance/user/:address", async (req, res) => {
  try {
    const { data, error } = await supabase.from("user_balances").select("*").eq("wallet_address", normalizeAddress(req.params.address)).single();
    if (error || !data) return res.json({ wallet_address: req.params.address, balance: 0, total_deposited: 0, total_spent: 0 });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/deposit", async (req, res) => {
  try {
    const { wallet_address, amount, tx_hash } = req.body;
    if (!wallet_address || !amount) return res.status(400).json({ error: "Missing fields" });
    const address = normalizeAddress(wallet_address);

    await supabase.from("deposits").insert({
      wallet_address: address, amount: parseFloat(amount),
      tx_hash, status: "confirmed", created_at: new Date().toISOString(),
    });

    const { data: existing } = await supabase.from("user_balances").select("*").eq("wallet_address", address).single();
    if (existing) {
      await supabase.from("user_balances").update({
        balance: parseFloat(existing.balance) + parseFloat(amount),
        total_deposited: parseFloat(existing.total_deposited) + parseFloat(amount),
        updated_at: new Date().toISOString(),
      }).eq("wallet_address", address);
    } else {
      await supabase.from("user_balances").insert({
        wallet_address: address, balance: parseFloat(amount),
        total_deposited: parseFloat(amount), total_spent: 0,
        created_at: new Date().toISOString(),
      });
    }
    res.json({ success: true, message: `Deposited ${amount} USDC` });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/deposits/:address", async (req, res) => {
  try {
    const { data, error } = await supabase.from("deposits").select("*").eq("wallet_address", normalizeAddress(req.params.address)).order("created_at", { ascending: false });
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`\n🚀 Arc Agent Pay Backend running on port ${PORT}`);
  console.log(`📡 Arc Testnet (Chain ID: 5042002)`);
  console.log(`🔵 Circle SDK: ${process.env.CIRCLE_API_KEY ? "✅ Connected" : "❌ Missing!"}`);
  console.log(`🗄️  Supabase: ${process.env.SUPABASE_URL}\n`);
  checkAndRunDueRules();
});
