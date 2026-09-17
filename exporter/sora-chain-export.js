// Coinpanda generic-import CSV for a SORA address, built straight from the
// chain (archive node mof2.sora.org). No third-party indexer.
//
// 1. Bisect block history on the account's balance state (system.account +
//    every tokens.accounts entry) to find each block where it changed.
// 2. Decode those blocks' events into rows (swaps from liquidityProxy.Exchange,
//    fees from xorFee.FeeWithdrawn, transfers split per counterparty).
// 3. Verify every change block: the rows' net movement must equal the real
//    balance difference between block-1 and block. The only accepted
//    difference is SORA's governance XOR redenomination (denomination pallet),
//    which changes balances without per-account events; those go to a
//    separate <out>_redenominations.csv.
// 4. Reconcile totals against the on-chain balance at the scan head.
//
// Usage: node sora-chain-export.js <address> <out.csv>

const { ApiPromise, WsProvider } = require('@polkadot/api');
const { TypeRegistry, Metadata } = require('@polkadot/types');
const soraTypes = require('@sora-substrate/type-definitions');
const path = require('path');
const { blake2AsHex, decodeAddress, encodeAddress } = require('@polkadot/util-crypto');
const { hexToU8a, u8aToHex } = require('@polkadot/util');
const crypto = require('crypto');
const fs = require('fs');

const HTTP = 'https://mof2.sora.org';
const WS = 'wss://mof2.sora.org';
const BATCH = 200;
const PARALLEL = 12;
const GRID_STEP = 100000;
const SS58 = 69;
const META_CACHE = path.join(__dirname, 'meta_cache');
fs.mkdirSync(META_CACHE, { recursive: true });

const EVENTS_KEY = '0x26aa394eea5630e07c48ae0c9558cef780d41e5e16056765bc8461851072c9d7';
const NOW_KEY = '0xf0c365c3cf59d671eb72da0e7a4113c49f1f0515f462cdcf84e0f1d6045dfcbb';
const XOR = '0x0200000000000000000000000000000000000000000000000000000000000000';

const HEADER = ['Timestamp (UTC)', 'Type', 'Sent Amount', 'Sent Currency', 'Received Amount', 'Received Currency', 'Fee Amount', 'Fee Currency', 'Net Worth Amount', 'Net Worth Currency', 'Label', 'Description', 'TxHash'];

// Principal moved into/out of a pool or lock, not a disposal.
const PRINCIPAL_CALLS = new Set([
  'demeterFarmingPlatform.deposit', 'demeterFarmingPlatform.withdraw',
  'poolXYK.depositLiquidity', 'poolXYK.withdrawLiquidity', 'poolXYK.initializePool',
  'referrals.reserve', 'referrals.unreserve',
  'staking.bond', 'staking.bondExtra', 'staking.unbond', 'staking.withdrawUnbonded', 'staking.rebond',
]);

const log = (...a) => console.log('>>', ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function post(payload) {
  for (let attempt = 1; ; attempt++) {
    try {
      const res = await fetch(HTTP, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload), signal: AbortSignal.timeout(120000) });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const json = await res.json();
      const err = json.find((r) => r.error);
      if (err) throw new Error(JSON.stringify(err.error));
      return json;
    } catch (e) {
      if (attempt >= 12) throw e;
      await sleep(Math.min(2000 * attempt, 30000));
    }
  }
}

// Heavy calls (full blocks, event lists) must go in small batches or the
// node's gateway times out (HTTP 504) on busy blocks.
async function rpcMany(calls, { batch = BATCH, parallel = PARALLEL } = {}) {
  const out = new Array(calls.length);
  const starts = [];
  for (let i = 0; i < calls.length; i += batch) starts.push(i);
  let next = 0;
  async function worker() {
    while (next < starts.length) {
      const start = starts[next++];
      const slice = calls.slice(start, start + batch);
      const resp = await post(slice.map(([method, params], j) => ({ jsonrpc: '2.0', id: start + j, method, params })));
      for (const r of resp) out[r.id] = r.result;
    }
  }
  await Promise.all(Array.from({ length: Math.min(parallel, starts.length) }, worker));
  return out;
}

// Runtime that executed each block, from System.LastRuntimeUpgrade (a cheap
// storage read). state_getRuntimeVersion costs ~1s per historical block and
// on an upgrade block reports the new runtime rather than the one that ran it.
const LAST_UPGRADE_KEY = '0x26aa394eea5630e07c48ae0c9558cef7f9cce9c888469bb1a0dceaa129672ef8';
function compactU32(hex) {
  const b = hexToU8a(hex);
  const mode = b[0] & 3;
  if (mode === 0) return b[0] >> 2;
  if (mode === 1) return (b[0] | (b[1] << 8)) >> 2;
  if (mode === 2) return ((b[0] | (b[1] << 8) | (b[2] << 16) | (b[3] << 24)) >>> 0) >>> 2;
  return (b[1] | (b[2] << 8) | (b[3] << 16) | (b[4] << 24)) >>> 0;
}
async function specVersionsFor(hashes) {
  const stored = await rpcMany(hashes.map((h) => ['state_getStorage', [LAST_UPGRADE_KEY, h]]));
  const missing = hashes.map((h, i) => (stored[i] ? null : i)).filter((i) => i !== null);
  const fallback = missing.length ? await rpcMany(missing.map((i) => ['state_getRuntimeVersion', [hashes[i]]])) : [];
  const out = stored.map((s) => (s ? compactU32(s) : null));
  missing.forEach((i, j) => { out[i] = fallback[j].specVersion; });
  return out;
}

const hashCache = new Map();
async function hashesFor(blocks) {
  const missing = [...new Set(blocks)].filter((b) => !hashCache.has(b));
  const res = await rpcMany(missing.map((b) => ['chain_getBlockHash', [b]]));
  missing.forEach((b, i) => hashCache.set(b, res[i]));
  return blocks.map((b) => hashCache.get(b));
}

// Raw account state at each block: system.account value + every token entry.
async function rawStates(blocks, sysKey, tokPrefix) {
  const hashes = await hashesFor(blocks);
  const r1 = await rpcMany(hashes.flatMap((h) => [['state_getStorage', [sysKey, h]], ['state_getKeysPaged', [tokPrefix, 1000, null, h]]]));
  const withKeys = [];
  hashes.forEach((h, i) => {
    const keys = r1[2 * i + 1];
    if (keys && keys.length >= 1000) throw new Error('more than 1000 token entries - paging not implemented');
    if (keys && keys.length) withKeys.push([i, keys]);
  });
  const r2 = await rpcMany(withKeys.map(([i, keys]) => ['state_queryStorageAt', [keys, hashes[i]]]), { batch: 50 });
  const tokens = new Map(withKeys.map(([i], j) => [i, r2[j][0]?.changes || []]));
  return hashes.map((h, i) => ({ hash: h, sys: r1[2 * i], tokens: tokens.get(i) || [] }));
}

async function fingerprints(blocks, sysKey, tokPrefix) {
  return (await rawStates(blocks, sysKey, tokPrefix)).map((s) => {
    const str = String(s.sys) + '|' + s.tokens.map(([k, v]) => `${k}:${v}`).sort().join(',');
    return crypto.createHash('sha1').update(str).digest('hex');
  });
}

async function findChangeBlocks(sysKey, tokPrefix, from, head) {
  const fp = new Map();
  const grid = [];
  for (let b = from; b < head; b += GRID_STEP) grid.push(b);
  grid.push(head);
  (await fingerprints(grid, sysKey, tokPrefix)).forEach((f, i) => fp.set(grid[i], f));

  let intervals = [];
  for (let i = 0; i + 1 < grid.length; i++) if (fp.get(grid[i]) !== fp.get(grid[i + 1])) intervals.push([grid[i], grid[i + 1]]);

  const changes = [];
  let probes = grid.length;
  let round = 0;
  while (intervals.length) {
    const mids = [];
    const split = [];
    for (const [lo, hi] of intervals) {
      if (hi - lo === 1) { changes.push(hi); continue; }
      const m = Math.floor((lo + hi) / 2);
      mids.push(m);
      split.push([lo, m, hi]);
    }
    if (mids.length) (await fingerprints(mids, sysKey, tokPrefix)).forEach((f, i) => fp.set(mids[i], f));
    probes += mids.length;
    intervals = [];
    for (const [lo, m, hi] of split) {
      if (fp.get(lo) !== fp.get(m)) intervals.push([lo, m]);
      if (fp.get(m) !== fp.get(hi)) intervals.push([m, hi]);
    }
    if (++round % 5 === 0 || !intervals.length) log(`bisect: ${probes} probes, ${intervals.length} open intervals, ${changes.length} change blocks found`);
  }
  return changes.sort((a, b) => a - b);
}

function assetIdOf(codec) {
  const j = codec.toJSON();
  return typeof j === 'string' ? j : j.code;
}

function formatAmount(raw, decimals) {
  const neg = raw < 0n;
  const abs = (neg ? -raw : raw).toString();
  if (decimals === 0) return (neg ? '-' : '') + abs;
  const s = abs.padStart(decimals + 1, '0');
  const frac = s.slice(-decimals).replace(/0+$/, '');
  return (neg ? '-' : '') + s.slice(0, -decimals) + (frac ? '.' + frac : '');
}

// Scales any decimal string to a 1e18-fixed-point BigInt. Used only to sum
// same-currency amounts for the negative-running-balance sanity check, so the
// asset's real decimal count doesn't matter as long as it's applied consistently.
function parseDecimal(str) {
  if (!str) return 0n;
  const neg = str.startsWith('-');
  const s = neg ? str.slice(1) : str;
  const [intPart, fracPart = ''] = s.split('.');
  const frac = (fracPart + '0'.repeat(18)).slice(0, 18);
  const v = BigInt(intPart || '0') * 10n ** 18n + BigInt(frac);
  return neg ? -v : v;
}

function csvCell(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

function writeCsv(file, rows) {
  const csv = [HEADER.join(','), ...rows.map((r) => HEADER.map((h) => csvCell(r[h])).join(','))].join('\n');
  fs.writeFileSync(file, csv, 'utf8');
}

async function main() {
  const address = process.argv[2];
  const outFile = process.argv[3];
  if (!address || !outFile) {
    console.error('Usage: node sora-chain-export.js <address> <out.csv>');
    process.exit(1);
  }
  const me = u8aToHex(decodeAddress(address));
  const short = (hex) => { const a = encodeAddress(hexToU8a('0x' + hex), SS58); return a.slice(0, 6) + '…' + a.slice(-4); };

  const api = await ApiPromise.create({ provider: new WsProvider(WS), noInitWarn: true });
  const sysKey = api.query.system.account.key(address);
  const tokPrefix = api.query.tokens.accounts.keyPrefix(address);

  // Finalized head only: a best-but-unfinalized block can be reorged away and its state discarded.
  const [finalizedHash] = await rpcMany([['chain_getFinalizedHead', []]]);
  const head = Number((await rpcMany([['chain_getHeader', [finalizedHash]]]))[0].number);
  const headHash = (await hashesFor([head]))[0];
  log(`${address}: scan head block ${head}`);

  const assets = new Map();
  for (const [key, value] of await api.query.assets.assetInfos.entries()) {
    const [symbol, , precision] = value;
    assets.set(assetIdOf(key.args[0]), { symbol: symbol.toHuman(), decimals: Number(precision.toString()) });
  }
  // The chain renamed native XOR during the redenominations (TXOR, 1SXOR); keep the ticker Coinpanda knows.
  assets.set(XOR, { symbol: 'XOR', decimals: 18 });
  const assetInfo = (id) => assets.get(id) || { symbol: id, decimals: 18 };

  // 1. Change blocks (cached per address so a restart never re-scans).
  const blocksFile = outFile.replace(/\.csv$/, '') + '.blocks.json';
  let changeBlocks = [];
  let scanFrom = 0;
  if (fs.existsSync(blocksFile)) {
    const cached = JSON.parse(fs.readFileSync(blocksFile, 'utf8'));
    changeBlocks = cached.blocks;
    scanFrom = cached.head;
    log(`loaded ${changeBlocks.length} change blocks up to block ${scanFrom}; scanning only newer blocks`);
  }
  const t0 = Date.now();
  changeBlocks = changeBlocks.concat(await findChangeBlocks(sysKey, tokPrefix, scanFrom, head));
  fs.writeFileSync(blocksFile, JSON.stringify({ head, blocks: changeBlocks }));
  log(`found ${changeBlocks.length} change blocks in ${((Date.now() - t0) / 1000).toFixed(0)}s`);
  // Debug: ONLY_BLOCKS=1,2,3 verifies just those change blocks (no files written).
  const onlyBlocks = process.env.ONLY_BLOCKS ? new Set(process.env.ONLY_BLOCKS.split(',').map(Number)) : null;
  if (onlyBlocks) changeBlocks = changeBlocks.filter((b) => onlyBlocks.has(b));

  const registries = new Map();
  async function registryFor(specVersion, blockHash) {
    if (registries.has(specVersion)) return registries.get(specVersion);
    const cacheFile = path.join(META_CACHE, `${specVersion}.hex`);
    if (!fs.existsSync(cacheFile)) {
      log(`downloading metadata for runtime ${specVersion}...`);
      // The code that executed a block is the code in its parent's state.
      const [header] = await rpcMany([['chain_getHeader', [blockHash]]]);
      const [[hex], [version]] = await Promise.all([
        rpcMany([['state_getMetadata', [header.parentHash]]]),
        rpcMany([['state_getRuntimeVersion', [header.parentHash]]]),
      ]);
      if (version.specVersion !== specVersion) throw new Error(`metadata version mismatch: wanted ${specVersion}, parent state has ${version.specVersion}`);
      fs.writeFileSync(cacheFile, hex);
    }
    const metaHex = fs.readFileSync(cacheFile, 'utf8');
    const registry = new TypeRegistry();
    const legacy = new Metadata(new TypeRegistry(), metaHex).version < 14;
    if (legacy) {
      // Pre-v14 metadata (SORA runtimes <= 41, 2021-2022) carries type names
      // only: use SORA's own definitions plus legacy encodings of that era.
      // Runtimes 1-19 named the bridge multisig's BridgeTimepoint "Timepoint".
      registry.setKnownTypes({ typesAlias: { bridgeMultisig: { Timepoint: 'BridgeTimepoint' } } });
      registry.register(soraTypes.typesBundle.spec.sora.types);
      registry.register({ DispatchErrorModule: 'DispatchErrorModuleU8', Weight: 'WeightV1' });
    }
    registry.setMetadata(new Metadata(registry, metaHex));
    const md = registry.metadata;
    const item = (pallet, name) => md.pallets.find((p) => p.name.toString() === pallet).storage.unwrap().items.find((i) => i.name.toString() === name);
    const entry = {
      legacy,
      registry,
      eventsType: registry.createLookupType(item('System', 'Events').type.asPlain),
      accountType: registry.createLookupType(item('System', 'Account').type.asMap.value),
      tokenType: registry.createLookupType(item('Tokens', 'Accounts').type.asMap.value),
    };
    registries.set(specVersion, entry);
    return entry;
  }

  async function balancesAt(blockNums) {
    const states = await rawStates(blockNums, sysKey, tokPrefix);
    const versions = await specVersionsFor(states.map((s) => s.hash));
    const out = [];
    for (let i = 0; i < states.length; i++) {
      const { registry, accountType, tokenType } = await registryFor(versions[i], states[i].hash);
      const bal = new Map();
      if (states[i].sys) {
        const acc = registry.createType(accountType, hexToU8a(states[i].sys));
        bal.set(XOR, BigInt(acc.data.free.toString()) + BigInt(acc.data.reserved.toString()));
      }
      for (const [k, v] of states[i].tokens) {
        if (!v) continue;
        const d = registry.createType(tokenType, hexToU8a(v));
        bal.set('0x' + k.slice(-64), BigInt(d.free.toString()) + BigInt(d.reserved.toString()));
      }
      out.push(bal);
    }
    return out;
  }

  const totals = new Map();
  const rows = [];
  const problems = [];

  // 2 + 3. Decode and verify.
  const CHUNK = 1000;
  for (let c = 0; c < changeBlocks.length; c += CHUNK) {
    const blocks = changeBlocks.slice(c, c + CHUNK);
    const hashes = await hashesFor(blocks);
    const heavy = await rpcMany(hashes.flatMap((h) => [['chain_getBlock', [h]], ['state_getStorage', [EVENTS_KEY, h]]]), { batch: 10, parallel: 8 });
    const nows = await rpcMany(hashes.map((h) => ['state_getStorage', [NOW_KEY, h]]));
    const specs = await specVersionsFor(hashes);
    const prevSpecs = await specVersionsFor(await hashesFor(blocks.map((b) => b - 1)));
    const res = hashes.flatMap((h, k) => [heavy[2 * k], heavy[2 * k + 1], nows[k], specs[k]]);
    const balAfter = await balancesAt(blocks);
    const balBefore = await balancesAt(blocks.map((b) => b - 1));

    for (let i = 0; i < blocks.length; i++) {
      const [blockRes, eventsHex, nowHex, specVersion] = res.slice(4 * i, 4 * i + 4);
      const { registry, eventsType, legacy } = await registryFor(specVersion, hashes[i]);
      const ts = new Date(Number(registry.createType('u64', hexToU8a(nowHex)).toString())).toISOString().slice(0, 19).replace('T', ' ');
      const extrinsics = blockRes.block.extrinsics;
      const records = registry.createType(eventsType, hexToU8a(eventsHex));

      const blockTotals = new Map();
      const addTotal = (id, v) => {
        totals.set(id, (totals.get(id) || 0n) + v);
        blockTotals.set(id, (blockTotals.get(id) || 0n) + v);
      };

      const groups = new Map();
      const group = (idx) => {
        if (!groups.has(idx)) groups.set(idx, { deltas: new Map(), flows: new Map(), fee: 0n, withdraws: [], exchanges: [], failed: false, tokenAssets: new Set(), currencyFlows: [], stakingReward: 0n });
        return groups.get(idx);
      };
      const hexOf = (codec) => u8aToHex(codec.toU8a()).slice(-64);
      const myHex = me.slice(-64);
      const isMe = (codec) => hexOf(codec) === myHex;
      let denominator = null;
      // Slashes often debit without any balances event, and in holds-based
      // runtimes the debit can happen in a later block than the first event.
      // The amount actually debited is taken from the real balance change,
      // bounded by the slash amounts reported for this wallet in the block.
      let slashedMe = 0n;
      let slashKind = '';
      const eventLog = [];
      const pendingOtherWithdraws = {};

      for (const rec of records) {
        const idx = rec.phase.isApplyExtrinsic ? rec.phase.asApplyExtrinsic.toNumber() : rec.phase.type;
        const { section, method, data } = rec.event;
        const name = `${section}.${method}`;
        const amt = (codec) => BigInt(codec.toString());
        const flow = (asset, cp, v) => {
          const g = group(idx);
          g.deltas.set(asset, (g.deltas.get(asset) || 0n) + v);
          const k = `${asset}|${cp}`;
          g.flows.set(k, (g.flows.get(k) || 0n) + v);
          if (asset !== XOR) g.tokenAssets.add(asset);
        };
        // Early runtimes report non-native tokens only via orml currencies events;
        // held back and used only if no tokens.* event covers the same asset.
        const currencyFlow = (asset, cp, v) => { if (asset !== XOR) group(idx).currencyFlows.push([asset, cp, v]); };
        const mentionsMe = data.some((d) => { try { return isMe(d); } catch { return false; } });
        if (mentionsMe) {
          eventLog.push(`${idx} ${name} ${JSON.stringify(data.toHuman()).slice(0, 300)}`);
          if (typeof idx !== 'number' && !name.startsWith('balances.') && !name.startsWith('tokens.')) group(idx).names = (group(idx).names || new Set()).add(name);
        }

        switch (name) {
          case 'balances.Transfer':
            if (isMe(data[0])) flow(XOR, hexOf(data[1]), -amt(data[2]));
            if (isMe(data[1])) flow(XOR, hexOf(data[0]), amt(data[2]));
            break;
          case 'balances.Deposit':
          case 'balances.Minted':
            if (isMe(data[0])) flow(XOR, 'none', amt(data[1]));
            break;
          case 'balances.Withdraw':
            if (isMe(data[0])) { flow(XOR, 'none', -amt(data[1])); group(idx).withdraws.push(amt(data[1])); }
            else if (groups.has(idx)) group(idx).otherWithdraws = (group(idx).otherWithdraws || []).concat(amt(data[1]));
            else (pendingOtherWithdraws[idx] = pendingOtherWithdraws[idx] || []).push(amt(data[1]));
            break;
          case 'balances.Slashed':
            if (isMe(data[0])) { flow(XOR, 'none', -amt(data[1])); group(idx).balancesSlashed = true; }
            break;
          case 'balances.DustLost':
          case 'balances.Burned':
            if (isMe(data[0])) flow(XOR, 'none', -amt(data[1]));
            break;
          case 'staking.Slash':
          case 'staking.Slashed':
          case 'electionsPhragmen.CandidateSlashed':
          case 'electionsPhragmen.SeatHolderSlashed':
            // Old runtimes emit only these (no balances.Slashed) when XOR is slashed.
            if (isMe(data[0]) && !group(idx).balancesSlashed) {
              slashedMe += amt(data[data.length - 1]);
              slashKind = name.startsWith('electionsPhragmen') ? 'council candidate bond slashed' : 'staking slash (validator penalty)';
            }
            break;
          case 'balances.ReserveRepatriated':
            if (isMe(data[0])) flow(XOR, hexOf(data[1]), -amt(data[2]));
            if (isMe(data[1])) flow(XOR, hexOf(data[0]), amt(data[2]));
            break;
          case 'tokens.Transfer':
          case 'tokens.Transferred':
            if (isMe(data[1])) flow(assetIdOf(data[0]), hexOf(data[2]), -amt(data[3]));
            if (isMe(data[2])) flow(assetIdOf(data[0]), hexOf(data[1]), amt(data[3]));
            break;
          case 'tokens.Deposited':
            if (isMe(data[1])) flow(assetIdOf(data[0]), 'none', amt(data[2]));
            break;
          case 'tokens.Withdrawn':
          case 'tokens.DustLost':
            if (isMe(data[1])) flow(assetIdOf(data[0]), 'none', -amt(data[2]));
            break;
          case 'tokens.Slashed':
            if (isMe(data[1])) flow(assetIdOf(data[0]), 'none', -(amt(data[2]) + amt(data[3])));
            break;
          case 'tokens.ReserveRepatriated':
            if (isMe(data[1])) flow(assetIdOf(data[0]), hexOf(data[2]), -amt(data[3]));
            if (isMe(data[2])) flow(assetIdOf(data[0]), hexOf(data[1]), amt(data[3]));
            break;
          case 'currencies.Transferred':
            if (isMe(data[1])) currencyFlow(assetIdOf(data[0]), hexOf(data[2]), -amt(data[3]));
            if (isMe(data[2])) currencyFlow(assetIdOf(data[0]), hexOf(data[1]), amt(data[3]));
            break;
          case 'currencies.Deposited':
            if (isMe(data[1])) currencyFlow(assetIdOf(data[0]), 'none', amt(data[2]));
            break;
          case 'currencies.Withdrawn':
            if (isMe(data[1])) currencyFlow(assetIdOf(data[0]), 'none', -amt(data[2]));
            break;
          case 'staking.Reward':
          case 'staking.Rewarded':
            // SORA pays staking rewards in VAL; early runtimes emit no token event for the credit.
            if (isMe(data[0])) group(idx).stakingReward += amt(data[data.length - 1]);
            break;
          case 'xorFee.FeeWithdrawn':
            if (isMe(data[0])) group(idx).fee += amt(data[1]);
            break;
          case 'liquidityProxy.Exchange':
            if (isMe(data[0])) group(idx).exchanges.push({ input: assetIdOf(data[2]), output: assetIdOf(data[3]), inAmt: amt(data[4]), outAmt: amt(data[5]) });
            break;
          case 'system.ExtrinsicFailed':
            if (groups.has(idx)) group(idx).failed = true;
            break;
          case 'denomination.Denominated':
            denominator = data[0].toString();
            break;
          default:
            break;
        }
      }

      const VAL = '0x0200040000000000000000000000000000000000000000000000000000000000';
      for (const [idx, g] of groups) {
        // xorFee.FeeWithdrawn names the account the fee is for, but the fee can be
        // paid from another account (e.g. a referral reserve). If the matching
        // balances.Withdraw came from someone else, this wallet did not pay it.
        const others = (g.otherWithdraws || []).concat(pendingOtherWithdraws[idx] || []);
        if (g.fee > 0n && g.withdraws.length === 0 && others.includes(g.fee)) g.fee = 0n;
      }
      for (const g of groups.values()) {
        for (const [asset, cp, v] of g.currencyFlows) {
          if (g.tokenAssets.has(asset)) continue;
          g.deltas.set(asset, (g.deltas.get(asset) || 0n) + v);
          const k = `${asset}|${cp}`;
          g.flows.set(k, (g.flows.get(k) || 0n) + v);
        }
        if (g.stakingReward > 0n && !((g.deltas.get(VAL) || 0n) > 0n)) {
          g.deltas.set(VAL, (g.deltas.get(VAL) || 0n) + g.stakingReward);
          g.flows.set(`${VAL}|none`, (g.flows.get(`${VAL}|none`) || 0n) + g.stakingReward);
        }
      }

      for (const [idx, g] of groups) {
        const extHex = typeof idx === 'number' ? extrinsics[idx] : null;
        let txHash = hashes[i];
        const callNames = new Set();
        const phaseEvents = g.names ? [...g.names].join(', ') : '';
        let callLabel = phaseEvents.includes('staking.Slashed')
          ? 'staking slash (validator penalty, no transaction)'
          : `${phaseEvents || 'chain event'} (block ${idx}, no transaction)`;
        let signedByMe = false;
        if (extHex) {
          txHash = blake2AsHex(hexToU8a(extHex), 256);
          try {
            const ext = registry.createType('Extrinsic', hexToU8a(extHex));
            callLabel = `${ext.method.section}.${ext.method.method}`;
            callNames.add(callLabel);
            if (ext.method.section === 'utility' && ['batch', 'batchAll', 'forceBatch'].includes(ext.method.method)) {
              for (const inner of ext.method.args[0]) callNames.add(`${inner.section}.${inner.method}`);
            }
            signedByMe = ext.isSigned && u8aToHex(decodeAddress(ext.signer.toString())) === me;
          } catch {
            callLabel = 'undecodable extrinsic';
          }
        }

        const { deltas, flows } = g;
        for (const [id, v] of deltas) addTotal(id, v);
        if (g.fee > 0n) {
          // Runtimes that emit balances.Withdraw for fees (sometimes an estimate
          // plus a balances.Deposit refund) already have the fee in the deltas.
          if (g.withdraws.length) {
            deltas.set(XOR, (deltas.get(XOR) || 0n) + g.fee);
            flows.set(`${XOR}|none`, (flows.get(`${XOR}|none`) || 0n) + g.fee);
          } else {
            addTotal(XOR, -g.fee);
          }
        }

        let description = `SORA ${callLabel}`;
        if (extHex && !signedByMe) description += ' (signed by other account)';
        if (g.failed) description += ' (failed)';
        const groupRows = [];

        let hadExchange = false;
        for (const ex of g.exchanges) {
          const din = deltas.get(ex.input) || 0n;
          const dout = deltas.get(ex.output) || 0n;
          if (din < 0n && dout > 0n) {
            const sent = -din < ex.inAmt ? -din : ex.inAmt;
            const recv = dout < ex.outAmt ? dout : ex.outAmt;
            deltas.set(ex.input, din + sent);
            deltas.set(ex.output, dout - recv);
            hadExchange = true;
            const a = assetInfo(ex.input);
            const b = assetInfo(ex.output);
            groupRows.push({
              Type: 'Trade',
              'Sent Amount': formatAmount(sent, a.decimals), 'Sent Currency': a.symbol,
              'Received Amount': formatAmount(recv, b.decimals), 'Received Currency': b.symbol,
            });
          }
        }

        const principal = signedByMe && [...callNames].some((n) => PRINCIPAL_CALLS.has(n));
        for (const [id, residual] of deltas) {
          if (residual === 0n) continue;
          const a = assetInfo(id);
          const assetFlows = [...flows].filter(([k, v]) => k.startsWith(id + '|') && v !== 0n).map(([k, v]) => [k.split('|')[1], v]);
          const flowSum = assetFlows.reduce((s, [, v]) => s + v, 0n);
          const parts = !hadExchange && assetFlows.length > 1 && flowSum === residual ? assetFlows : [['', residual]];
          const largest = parts.reduce((m, p) => ((p[1] < 0n ? -p[1] : p[1]) > (m[1] < 0n ? -m[1] : m[1]) ? p : m), parts[0]);
          for (const [cp, v] of parts) {
            let label = principal && (parts.length === 1 || cp === largest[0]) ? 'Staking Pool' : '';
            // Referral reserve/unreserve and legacy LP receipt tokens are not pool deposits.
            if (label === 'Staking Pool' && (callLabel.startsWith('referrals.') || a.symbol === 'XYKPOOL')) label = '';
            if (callLabel === 'staking.payoutStakers' && v > 0n) label = 'Staking';
            const cpNote = parts.length > 1 && cp !== 'none' ? ` ${v < 0n ? 'to' : 'from'} ${short(cp)}` : '';
            groupRows.push(v < 0n
              ? { Type: 'Send', 'Sent Amount': formatAmount(-v, a.decimals), 'Sent Currency': a.symbol, Label: label, _note: cpNote }
              : { Type: 'Receive', 'Received Amount': formatAmount(v, a.decimals), 'Received Currency': a.symbol, Label: label, _note: cpNote });
          }
        }

        if (g.fee > 0n) {
          if (groupRows.length) {
            groupRows[0]['Fee Amount'] = formatAmount(g.fee, 18);
            groupRows[0]['Fee Currency'] = 'XOR';
          } else {
            groupRows.push({ Type: 'Send', 'Sent Amount': formatAmount(g.fee, 18), 'Sent Currency': 'XOR', Label: 'Cost', _note: ' (network fee only)' });
          }
        }

        for (const r of groupRows) {
          const { _note, ...row } = r;
          rows.push({ 'Timestamp (UTC)': ts, Description: description + (_note || ''), TxHash: txHash, _block: blocks[i], _idx: typeof idx === 'number' ? idx : -1, ...row });
        }
      }

      const ids = new Set([...balAfter[i].keys(), ...balBefore[i].keys(), ...blockTotals.keys()]);
      for (const id of ids) {
        const diff = (balAfter[i].get(id) || 0n) - (balBefore[i].get(id) || 0n) - (blockTotals.get(id) || 0n);
        if (diff === 0n) continue;
        const a = assetInfo(id);
        if (id === XOR && diff < 0n && slashedMe > 0n && -diff <= slashedMe) {
          totals.set(id, (totals.get(id) || 0n) + diff);
          rows.push({
            'Timestamp (UTC)': ts, Type: 'Send', 'Sent Amount': formatAmount(-diff, 18), 'Sent Currency': 'XOR',
            Description: `SORA ${slashKind} (no transaction)`, TxHash: hashes[i], _block: blocks[i], _idx: -1,
          });
          continue;
        }
        const runtimeUpgrade = prevSpecs[i] !== specs[i];
        if (denominator || runtimeUpgrade) {
          const before = totals.get(id) || 0n;
          const after = before + diff;
          totals.set(id, after);
          // Adjustments happen at block on_initialize, before any extrinsic in the
          // block runs, so they must sort ahead of this block's transaction rows.
          if (denominator && after > 0n) {
            const denomN = BigInt(denominator);
            if (before < 0n || (before / denomN - after < -2n || before / denomN - after > 2n)) {
              throw new Error(`redenomination check failed at block ${blocks[i]} ${a.symbol}: ${before} / ${denominator} != ${after}`);
            }
            rows.push({
              'Timestamp (UTC)': ts, Type: 'Trade', Label: 'Swap',
              'Sent Amount': formatAmount(before, a.decimals), 'Sent Currency': a.symbol,
              'Received Amount': formatAmount(after, a.decimals), 'Received Currency': a.symbol,
              Description: `${a.symbol} redenomination ÷${denominator} by SORA governance (no transaction; cost basis carries over)`,
              TxHash: hashes[i], _block: blocks[i], _idx: -2,
            });
          } else {
            rows.push({
              'Timestamp (UTC)': ts,
              Type: diff < 0n ? 'Send' : 'Receive',
              ...(diff < 0n ? { 'Sent Amount': formatAmount(-diff, a.decimals), 'Sent Currency': a.symbol } : { 'Received Amount': formatAmount(diff, a.decimals), 'Received Currency': a.symbol }),
              Description: denominator
                ? `${a.symbol} redenomination ÷${denominator} by SORA governance rounded this balance to zero (no transaction)`
                : `${a.symbol} LP pool-share tokens removed by SORA runtime upgrade ${prevSpecs[i]}→${specs[i]} migration (no transaction)`,
              TxHash: hashes[i], _block: blocks[i], _idx: -2,
            });
          }
          log(`adjustment at block ${blocks[i]} ${a.symbol} ${formatAmount(diff, a.decimals)} (${denominator ? 'redenomination' : `runtime upgrade ${prevSpecs[i]}->${specs[i]}`})`);
        } else {
          problems.push(blocks[i]);
          log(`BLOCK MISMATCH ${blocks[i]} ${a.symbol}: rows are off by ${formatAmount(diff, a.decimals)}`);
          for (const line of eventLog) log(`    ${line}`);
        }
      }
    }
    log(`processed ${Math.min(c + CHUNK, changeBlocks.length)}/${changeBlocks.length} change blocks, ${rows.length} rows`);
  }

  if (onlyBlocks) {
    log(`DEBUG ONLY_BLOCKS: ${changeBlocks.length} blocks checked, ${problems.length} mismatched`);
    await api.disconnect();
    return;
  }
  rows.sort((a, b) => a._block - b._block || a._idx - b._idx);

  // Sanity check: Coinpanda replays rows in this exact order, so no currency's
  // running balance may go negative mid-file even though totals reconcile.
  const running = new Map();
  const add = (cur, amt) => running.set(cur, (running.get(cur) || 0n) + amt);
  for (const r of rows) {
    if (r['Sent Currency']) add(r['Sent Currency'], -parseDecimal(r['Sent Amount']));
    if (r['Received Currency']) add(r['Received Currency'], parseDecimal(r['Received Amount']));
    if (r['Fee Currency']) add(r['Fee Currency'], -parseDecimal(r['Fee Amount']));
  }
  for (const [cur, v] of running) if (v < 0n) throw new Error(`negative running balance for ${cur}: ${v} at end of scan`);

  writeCsv(outFile, rows);
  log(`wrote ${rows.length} rows -> ${outFile}`);

  // 4. Reconcile at scan head.
  const [actual] = await balancesAt([head]);
  const ids = new Set([...actual.keys(), ...totals.keys()]);
  let mismatches = 0;
  for (const id of ids) {
    const want = actual.get(id) || 0n;
    const got = totals.get(id) || 0n;
    if (want !== got) {
      mismatches++;
      const a = assetInfo(id);
      log(`RECONCILE MISMATCH ${a.symbol}: on-chain ${formatAmount(want, a.decimals)} vs rows ${formatAmount(got, a.decimals)}`);
    }
  }
  const adjustmentCount = rows.filter((r) => r._idx === -2).length;
  log(`RESULT ${address}: ${rows.length} rows, ${adjustmentCount} non-transaction adjustments (redenominations / runtime migrations), ${problems.length} unexplained block mismatches, ${mismatches ? mismatches + ' asset(s) NOT reconciled' : `all ${ids.size} assets reconcile exactly to on-chain balances`}`);
  await api.disconnect();
}

main().catch((e) => { console.error('ERROR', e.stack || e.message); process.exit(1); });
