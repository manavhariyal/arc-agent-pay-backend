require("dotenv").config();
const express = require("express");
const cors = require("cors");
const cron = require("node-cron");
const { createClient } = require("@supabase/supabase-js");
const { ethers } = require("ethers");

const app = express();
app.use(cors({ origin: process.env.FRONTEND_URL || "*" }));
app.use(express.json());

// ─── Supabase Client ───────────────────────────────────────────────
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

// ─── Arc Testnet Provider ──────────────────────────────────────────
const provider = new ethers.JsonRpcProvider(
  process.env.ARC_RPC_URL || "https://rpc.testnet.arc.network",
  {
    chainId: parseInt(process.env.ARC_CHAIN_ID || "5042002"),
    name: "arc-testnet",
  }
);

// ─── USDC Contract ABI (minimal) ──────────────────────────────────
const USDC_ABI = [
  "function transfer(address to, uint256 amount) returns (bool)",
  "function balanceOf(address owner) view returns (uint256)",
  "function decimals() view returns (uint8)",
  "function allowance(address owner, address spender) view returns (uint256)",
];

const USDC_ADDRESS = process.env.USDC_CONTRACT || "0x3600000000000000000000000000000000000000";

// ─── Execute a Single Payment Rule ────────────────────────────────
async function executeRule(rule) {
  console.log(`\n[SCHEDULER] Executing rule: ${rule.id} for agent: ${rule.agent_id}`);

  try {
    // Get agent details
    const { data: agent, error: agentError } = await supabase
      .from("agents")
      .select("*")
      .eq("id", rule.agent_id)
      .single();

    if (agentError || !agent) {
      console.error(`[ERROR] Agent not found for rule ${rule.id}`);
      await logTransaction(rule, null, "failed", "Agent not found");
      return;
    }

    if (agent.status !== "active") {
      console.log(`[SKIP] Agent ${agent.name} is not active. Skipping.`);
      return;
    }

    // Decrypt and use the private key stored for this agent
    if (!rule.signer_private_key) {
      console.error(`[ERROR] No private key for rule ${rule.id}`);
      await logTransaction(rule, agent, "failed", "No signer key configured");
      return;
    }

    const wallet = new ethers.Wallet(rule.signer_private_key, provider);
    const usdc = new ethers.Contract(USDC_ADDRESS, USDC_ABI, wallet);
    const decimals = await usdc.decimals(); // 6 for ERC20 interface

    // Convert amount to proper decimals
    const amount = ethers.parseUnits(rule.amount.toString(), decimals);

    // Check balance before sending
    const balance = await usdc.balanceOf(wallet.address);
    if (balance < amount) {
      console.warn(`[WARN] Insufficient balance for rule ${rule.id}`);
      await logTransaction(rule, agent, "failed", "Insufficient balance");
      return;
    }

    // Send the transaction
    console.log(`[TX] Sending ${rule.amount} USDC to ${rule.recipient_address}`);
    const tx = await usdc.transfer(rule.recipient_address, amount);
    console.log(`[TX] Hash: ${tx.hash}`);

    // Wait for confirmation
    const receipt = await tx.wait();
    console.log(`[TX] Confirmed in block ${receipt.blockNumber}`);

    // Log success
    await logTransaction(rule, agent, "success", null, tx.hash, receipt.blockNumber);

    // Update rule last_executed_at
    await supabase
      .from("payment_rules")
      .update({ last_executed_at: new Date().toISOString(), execution_count: (rule.execution_count || 0) + 1 })
      .eq("id", rule.id);

    console.log(`[SUCCESS] Rule ${rule.id} executed successfully!`);

  } catch (err) {
    console.error(`[ERROR] Rule ${rule.id} failed:`, err.message);
    await logTransaction(rule, null, "failed", err.message);
  }
}

// ─── Log Transaction to Supabase ──────────────────────────────────
async function logTransaction(rule, agent, status, errorMsg = null, txHash = null, blockNumber = null) {
  await supabase.from("transactions").insert({
    agent_id: rule.agent_id,
    rule_id: rule.id,
    from_address: agent?.wallet_address || "unknown",
    to_address: rule.recipient_address,
    amount: rule.amount,
    status,
    tx_hash: txHash,
    block_number: blockNumber,
    error_message: errorMsg,
    type: "scheduled",
    created_at: new Date().toISOString(),
  });
}

// ─── Check & Run Due Rules ─────────────────────────────────────────
async function checkAndRunDueRules() {
  console.log(`\n[CRON] Checking for due payment rules at ${new Date().toISOString()}`);

  try {
    const now = new Date();

    // Get all active rules
    const { data: rules, error } = await supabase
      .from("payment_rules")
      .select("*")
      .eq("is_active", true)
      .eq("status", "active");

    if (error) {
      console.error("[CRON ERROR]", error.message);
      return;
    }

    if (!rules || rules.length === 0) {
      console.log("[CRON] No active rules found.");
      return;
    }

    console.log(`[CRON] Found ${rules.length} active rules.`);

    for (const rule of rules) {
      const shouldRun = isRuleDue(rule, now);
      if (shouldRun) {
        await executeRule(rule);
      }
    }
  } catch (err) {
    console.error("[CRON FATAL]", err.message);
  }
}

// ─── Check if Rule is Due ──────────────────────────────────────────
function isRuleDue(rule, now) {
  const lastRun = rule.last_executed_at ? new Date(rule.last_executed_at) : null;

  if (!lastRun) return true; // Never run before, run now

  const diffMs = now - lastRun;
  const diffHours = diffMs / (1000 * 60 * 60);

  switch (rule.interval) {
    case "hourly":   return diffHours >= 1;
    case "every6h":  return diffHours >= 6;
    case "every12h": return diffHours >= 12;
    case "daily":    return diffHours >= 24;
    case "weekly":   return diffHours >= 168;
    case "monthly":  return diffHours >= 720;
    default:         return false;
  }
}

// ─── Cron Jobs ────────────────────────────────────────────────────
// Run every hour to check due rules
cron.schedule("0 * * * *", checkAndRunDueRules);

// Also run every 5 minutes for hourly rules precision
cron.schedule("*/5 * * * *", async () => {
  const { data: rules } = await supabase
    .from("payment_rules")
    .select("*")
    .eq("is_active", true)
    .eq("interval", "hourly")
    .eq("status", "active");

  if (rules && rules.length > 0) {
    for (const rule of rules) {
      if (isRuleDue(rule, new Date())) {
        await executeRule(rule);
      }
    }
  }
});

// ─── REST API Routes ───────────────────────────────────────────────

// Health check
app.get("/health", (req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString(), network: "Arc Testnet" });
});

// Get all agents
app.get("/api/agents", async (req, res) => {
  const { data, error } = await supabase.from("agents").select("*").order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// Create agent
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

// Update agent
app.put("/api/agents/:id", async (req, res) => {
  const { id } = req.params;
  const { data, error } = await supabase.from("agents").update(req.body).eq("id", id).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// Delete agent
app.delete("/api/agents/:id", async (req, res) => {
  const { error } = await supabase.from("agents").delete().eq("id", id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// Get payment rules
app.get("/api/rules", async (req, res) => {
  const { agent_id } = req.query;
  let query = supabase.from("payment_rules").select("*, agents(name, wallet_address)");
  if (agent_id) query = query.eq("agent_id", agent_id);
  const { data, error } = await query.order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// Create payment rule
app.post("/api/rules", async (req, res) => {
  const { agent_id, name, amount, interval, recipient_address, signer_private_key } = req.body;
  const { data, error } = await supabase.from("payment_rules").insert({
    agent_id, name, amount, interval,
    recipient_address, signer_private_key,
    is_active: true,
    status: "active",
    execution_count: 0,
    created_at: new Date().toISOString(),
  }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// Toggle rule on/off
app.patch("/api/rules/:id/toggle", async (req, res) => {
  const { id } = req.params;
  const { data: rule } = await supabase.from("payment_rules").select("is_active").eq("id", id).single();
  const { data, error } = await supabase
    .from("payment_rules")
    .update({ is_active: !rule.is_active, status: !rule.is_active ? "active" : "paused" })
    .eq("id", id).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// Delete rule
app.delete("/api/rules/:id", async (req, res) => {
  const { id } = req.params;
  const { error } = await supabase.from("payment_rules").delete().eq("id", id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// Get transactions
app.get("/api/transactions", async (req, res) => {
  const { agent_id, limit = 50 } = req.query;
  let query = supabase.from("transactions").select("*, agents(name)").order("created_at", { ascending: false }).limit(limit);
  if (agent_id) query = query.eq("agent_id", agent_id);
  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// Manual trigger rule (for testing)
app.post("/api/rules/:id/execute", async (req, res) => {
  const { id } = req.params;
  const { data: rule, error } = await supabase.from("payment_rules").select("*").eq("id", id).single();
  if (error || !rule) return res.status(404).json({ error: "Rule not found" });
  await executeRule(rule);
  res.json({ success: true, message: "Rule execution triggered" });
});

// Get USDC balance for an address
app.get("/api/balance/:address", async (req, res) => {
  try {
    const { address } = req.params;
    const usdc = new ethers.Contract(USDC_ADDRESS, USDC_ABI, provider);
    const balance = await usdc.balanceOf(address);
    const decimals = await usdc.decimals();
    const formatted = ethers.formatUnits(balance, decimals);
    res.json({ address, balance: formatted, raw: balance.toString() });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Start Server ──────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`\n🚀 Arc Agent Pay Backend running on port ${PORT}`);
  console.log(`📡 Connected to Arc Testnet (Chain ID: 5042002)`);
  console.log(`🗄️  Supabase: ${process.env.SUPABASE_URL}`);
  console.log(`⏰  Scheduler: Running (checks every 5 minutes)\n`);
  
  // Run once on startup
  checkAndRunDueRules();
});
