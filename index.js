require("dotenv").config();
const express = require("express");
const cors = require("cors");
const cron = require("node-cron");
const { createClient } = require("@supabase/supabase-js");
const {
  initiateDeveloperControlledWalletsClient,
} = require("@circle-fin/developer-controlled-wallets");

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

async function executeRule(rule) {
  console.log(`\n[SCHEDULER] Executing rule: ${rule.id}`);
  try {
    const { data: agent } = await supabase.from("agents").select("*").eq("id", rule.agent_id).single();
    if (!agent) { await logTransaction(rule, null, "failed", "Agent not found"); return; }
    if (agent.status !== "active") { console.log(`[SKIP] Agent not active`); return; }
    if (!rule.circle_wallet_id) { await logTransaction(rule, agent, "failed", "No Circle wallet linked"); return; }

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
      await supabase.from("payment_rules").update({
        last_executed_at: new Date().toISOString(),
        execution_count: (rule.execution_count || 0) + 1,
      }).eq("id", rule.id);
      console.log(`[SUCCESS] TX: ${txHash}`);
    } else {
      await logTransaction(rule, agent, "failed", `State: ${state}`);
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

app.get("/api/agents", async (req, res) => {
  const { data, error } = await supabase.from("agents").select("*").order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post("/api/agents", async (req, res) => {
  const { name, description, wallet_address, alert_threshold, status } = req.body;
  const { data, error } = await supabase.from("agents").insert({
    name, description, wallet_address,
    alert_threshold: alert_threshold || 10,
    status: status || "active",
    created_at: new Date().toISOString(),
  }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.put("/api/agents/:id", async (req, res) => {
  const { data, error } = await supabase.from("agents").update(req.body).eq("id", req.params.id).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.delete("/api/agents/:id", async (req, res) => {
  const { error } = await supabase.from("agents").delete().eq("id", req.params.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.get("/api/rules", async (req, res) => {
  const { agent_id } = req.query;
  let query = supabase.from("payment_rules").select("*, agents(name, wallet_address)").order("created_at", { ascending: false });
  if (agent_id) query = query.eq("agent_id", agent_id);
  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post("/api/rules", async (req, res) => {
  const { agent_id, name, amount, interval, recipient_address, circle_wallet_id } = req.body;
  const { data, error } = await supabase.from("payment_rules").insert({
    agent_id, name, amount, interval, recipient_address,
    circle_wallet_id: circle_wallet_id || null,
    is_active: true, status: "active", execution_count: 0,
    created_at: new Date().toISOString(),
  }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.patch("/api/rules/:id/toggle", async (req, res) => {
  const { data: rule } = await supabase.from("payment_rules").select("is_active").eq("id", req.params.id).single();
  const { data, error } = await supabase.from("payment_rules")
    .update({ is_active: !rule.is_active, status: !rule.is_active ? "active" : "paused" })
    .eq("id", req.params.id).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.delete("/api/rules/:id", async (req, res) => {
  const { error } = await supabase.from("payment_rules").delete().eq("id", req.params.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.post("/api/rules/:id/execute", async (req, res) => {
  const { data: rule, error } = await supabase.from("payment_rules").select("*").eq("id", req.params.id).single();
  if (error || !rule) return res.status(404).json({ error: "Rule not found" });
  executeRule(rule);
  res.json({ success: true, message: "Payment execution triggered!" });
});

app.get("/api/transactions", async (req, res) => {
  const { agent_id, limit = 50 } = req.query;
  let query = supabase.from("transactions").select("*, agents(name)").order("created_at", { ascending: false }).limit(limit);
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

const PORT = process.env.PORT || 3001;
// One-time setup: Create Circle wallet set and wallet
app.post("/api/setup-circle-wallet", async (req, res) => {
  try {
    const client = getCircleClient();
    
    // Create wallet set
    const walletSetResponse = await client.createWalletSet({
      name: "Arc Agent Pay Wallet Set",
    });
    const walletSetId = walletSetResponse.data?.walletSet?.id;
    if (!walletSetId) throw new Error("Wallet set creation failed");

    // Create wallet on Arc Testnet
    const walletResponse = await client.createWallets({
      walletSetId,
      blockchains: ["ARC-TESTNET"],
      count: 1,
      accountType: "EOA",
    });

    const wallet = walletResponse.data?.wallets?.[0];
    if (!wallet) throw new Error("Wallet creation failed");

    console.log("✅ Circle Wallet Created:", wallet);

    res.json({
      success: true,
      walletSetId,
      walletId: wallet.id,
      walletAddress: wallet.address,
      message: "Save these IDs! You need walletId for payment rules."
    });
  } catch (err) {
    console.error("Setup error:", err.message);
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
