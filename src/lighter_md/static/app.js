const columns = [
  { key: "symbol" },
  { key: "best_bid_price" },
  { key: "best_bid_size" },
  { key: "best_ask_price" },
  { key: "best_ask_size" },
  { key: "mid_price" },
  { key: "spread" },
  { key: "last_price" },
  { key: "mark_price" },
  { key: "index_price" },
  { key: "basis" },
  { key: "markout" },
  { key: "daily_volume" },
  { key: "funding_rate" },
  { key: "open_interest" },
];

const columnIndex = Object.fromEntries(columns.map((col, idx) => [col.key, idx]));

const NUMERIC_FIELDS = new Set([
  "best_bid_price",
  "best_bid_size",
  "best_ask_price",
  "best_ask_size",
  "last_price",
  "mark_price",
  "index_price",
  "mid_price",
  "funding_rate",
  "basis",
  "markout",
  "daily_volume",
  "open_interest",
  "spread",
  "updated_ms",
]);

const formatters = {
  price: new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }),
  size: new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 4,
    maximumFractionDigits: 4,
  }),
  volume: new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 2,
  }),
  funding: new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 6,
    maximumFractionDigits: 6,
    signDisplay: "auto",
  }),
  diff: new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
    signDisplay: "auto",
  }),
  openInterest: new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }),
  spread: new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }),
};

const statusElement = document.getElementById("status");
const tbody = document.getElementById("markets-body");

const state = {
  rows: new Map(),
  elements: new Map(),
  retryDelay: 1000,
};

function formatValue(key, value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  switch (key) {
    case "best_bid_price":
    case "best_ask_price":
    case "mid_price":
    case "last_price":
    case "mark_price":
    case "index_price":
      return formatters.price.format(value);
    case "best_bid_size":
    case "best_ask_size":
      return formatters.size.format(value);
    case "basis":
    case "markout":
      return formatters.diff.format(value);
    case "daily_volume":
      return formatters.volume.format(value);
    case "funding_rate":
      return formatters.funding.format(value);
    case "open_interest":
      return formatters.openInterest.format(value);
    case "spread":
      return formatters.spread.format(value);
    default:
      return value;
  }
}

function normaliseValue(key, value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (NUMERIC_FIELDS.has(key)) {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }
  if (key === "symbol" && typeof value === "string" && value.trim().length) {
    return value;
  }
  return value;
}

function emptyRow(marketId) {
  return {
    market_id: marketId,
    symbol: `MKT-${marketId}`,
    best_bid_price: null,
    best_bid_size: null,
    best_ask_price: null,
    best_ask_size: null,
    mid_price: null,
    last_price: null,
    mark_price: null,
    index_price: null,
    basis: null,
    markout: null,
    daily_volume: null,
    funding_rate: null,
    open_interest: null,
    spread: null,
    updated_ms: null,
  };
}

function buildRowElement(row) {
  const tr = document.createElement("tr");
  tr.dataset.id = String(row.market_id);
  columns.forEach((column) => {
    const td = document.createElement("td");
    td.textContent = formatValue(column.key, row[column.key]);
    tr.appendChild(td);
  });
  return tr;
}

function compareRows(a, b) {
  const oiA = Number.isFinite(a.open_interest) ? a.open_interest : Number.NEGATIVE_INFINITY;
  const oiB = Number.isFinite(b.open_interest) ? b.open_interest : Number.NEGATIVE_INFINITY;
  if (oiA !== oiB) {
    return oiB - oiA;
  }
  return a.market_id - b.market_id;
}

function placeRowElement(row, element) {
  if (element.parentElement === tbody) {
    tbody.removeChild(element);
  }
  const siblings = Array.from(tbody.children);
  for (let i = 0; i < siblings.length; i += 1) {
    const sibling = siblings[i];
    if (sibling === element) {
      continue;
    }
    const siblingId = Number(sibling.dataset.id);
    const other = state.rows.get(siblingId);
    if (!other) {
      tbody.insertBefore(element, sibling);
      return;
    }
    if (compareRows(row, other) < 0) {
      tbody.insertBefore(element, sibling);
      return;
    }
  }
  tbody.appendChild(element);
}

function renderSnapshot(rows) {
  const processed = [];
  rows.forEach((input) => {
    const marketId = Number(input.market_id);
    if (!Number.isFinite(marketId)) {
      return;
    }
    const row = emptyRow(marketId);
    Object.keys(input).forEach((key) => {
      if (key === "market_id") {
        row.market_id = marketId;
      } else if (key in row) {
        row[key] = normaliseValue(key, input[key]);
      }
    });
    if (!row.symbol || row.symbol === "—") {
      row.symbol = `MKT-${marketId}`;
    }
    processed.push(row);
  });
  processed.sort(compareRows);

  state.rows.clear();
  state.elements.clear();
  const fragment = document.createDocumentFragment();
  processed.forEach((row) => {
    state.rows.set(row.market_id, row);
    const tr = buildRowElement(row);
    state.elements.set(row.market_id, tr);
    fragment.appendChild(tr);
  });
  tbody.replaceChildren(fragment);
  updateStatus(`Connected (${state.rows.size} markets)`, true);
}

function renderRow(row, changedKeys = null) {
  let tr = state.elements.get(row.market_id);
  if (!tr) {
    tr = buildRowElement(row);
    state.elements.set(row.market_id, tr);
  }
  const keysToUpdate =
    changedKeys && changedKeys.length > 0 ? changedKeys : columns.map((col) => col.key);
  keysToUpdate.forEach((key) => {
    if (!(key in columnIndex)) {
      return;
    }
    const idx = columnIndex[key];
    const cell = tr.children[idx];
    if (cell) {
      cell.textContent = formatValue(key, row[key]);
    }
  });
  placeRowElement(row, tr);
}

function applyUpdate(update) {
  if (!update || typeof update.market_id === "undefined") {
    return;
  }
  const marketId = Number(update.market_id);
  if (!Number.isFinite(marketId)) {
    return;
  }
  const row = state.rows.get(marketId) || emptyRow(marketId);
  const changedKeys = [];
  Object.keys(update).forEach((key) => {
    if (key === "market_id") {
      row.market_id = marketId;
      return;
    }
    if (!(key in row)) {
      return;
    }
    const nextValue = normaliseValue(key, update[key]);
    row[key] = nextValue;
    changedKeys.push(key);
  });
  if (!row.symbol || row.symbol === "—") {
    row.symbol = `MKT-${marketId}`;
  }
  if (!state.rows.has(marketId)) {
    state.rows.set(marketId, row);
    renderRow(row);
    updateStatus(`Connected (${state.rows.size} markets)`, true);
  } else {
    renderRow(row, changedKeys);
  }
}

function updateStatus(text, up) {
  if (!statusElement) {
    return;
  }
  statusElement.innerHTML = "";
  const span = document.createElement("span");
  const dot = document.createElement("span");
  dot.className = `status-dot${up ? "" : " down"}`;
  span.appendChild(dot);
  span.appendChild(document.createTextNode(text));
  statusElement.appendChild(span);
}

function connect() {
  const scheme = window.location.protocol === "https:" ? "wss" : "ws";
  const socketUrl = `${scheme}://${window.location.host}/ws`;
  const socket = new WebSocket(socketUrl);

  socket.addEventListener("open", () => {
    state.retryDelay = 1000;
    updateStatus("Connected", true);
  });

  socket.addEventListener("message", (event) => {
    let payload;
    try {
      payload = JSON.parse(event.data);
    } catch (_err) {
      return;
    }
    if (!payload || !payload.type) {
      return;
    }
    if (payload.type === "snapshot" && Array.isArray(payload.rows)) {
      renderSnapshot(payload.rows);
    } else if (payload.type === "update" && payload.row) {
      applyUpdate(payload.row);
    }
  });

  socket.addEventListener("close", () => {
    updateStatus("Disconnected – retrying…", false);
    state.rows.clear();
    tbody.replaceChildren();
    state.elements.clear();
    setTimeout(() => connect(), state.retryDelay);
    state.retryDelay = Math.min(state.retryDelay * 2, 15000);
  });

  socket.addEventListener("error", () => {
    socket.close();
  });
}

connect();
