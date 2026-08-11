# Arc Agent Pay — Backend

Backend API and payment scheduler for [Arc Agent Pay](https://arcagentpay.xyz), a payments infrastructure layer for autonomous AI agents on Arc Testnet.

## What this does

- Runs scheduled USDC payments on behalf of registered AI agents, via Circle's Developer Controlled Wallets SDK
- Auto-pauses agents when funded balance runs too low to avoid failed or overdrawn payments
- Serves per-wallet agent, rule, transaction, deposit, and balance data (Supabase-backed, isolated per owner)
- Implements a working **x402** payment flow using Circle's Gateway facilitator:
  - A paywalled demo endpoint (`/api/x402-demo/gold-price`) that requires a $0.001 USDC payment
  - A buyer-side flow that autonomously pays and unlocks the resource, no manual signing

## Live

- API: [arc-agent-pay-backend.onrender.com](https://arc-agent-pay-backend.onrender.com)
- Demo of the full x402 flow: [arcagentpay.xyz/x402-demo](https://arcagentpay.xyz/x402-demo)

## Tech stack

- Node.js + Express
- Circle Developer Controlled Wallets SDK
- `@circle-fin/x402-batching` (Circle Gateway) for autonomous micropayments
- Supabase (Postgres) for data
- `node-cron` for the payment scheduler
- Deployed on Render

## Network

Arc Testnet — Chain ID `5042002`

## Related repos

- Frontend: [arc-agent-pay-frontend](https://github.com/manavhariyal/arc-agent-pay-frontend)

## Local development

```bash
npm install
npm start
```

Requires a `.env` matching the variables in `.env.example` (Supabase, Circle API credentials, RPC URL).
