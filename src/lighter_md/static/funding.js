const chart = document.getElementById("funding-chart");
const timestampEl = document.getElementById("funding-updated");

const formatter = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
  signDisplay: "auto",
});

const fundingFormatter = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 6,
  maximumFractionDigits: 6,
  signDisplay: "auto",
});

let retryDelay = 1000;

function setTimestamp(ms) {
  if (!timestampEl) {
    return;
  }
  if (!ms) {
    timestampEl.textContent = "Waiting for data…";
    return;
  }
  const date = new Date(ms);
  timestampEl.textContent = `Updated ${date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  })}`;
}

function renderEmpty(message = "Waiting for funding updates…") {
  if (!chart) {
    return;
  }
  chart.innerHTML = "";
  const placeholder = document.createElement("div");
  placeholder.className = "empty-state";
  placeholder.textContent = message;
  chart.appendChild(placeholder);
}

function renderRows(rows, timestamp) {
  if (!chart) {
    return;
  }
  if (!rows || rows.length === 0) {
    setTimestamp(timestamp);
    renderEmpty("No funding data available.");
    return;
  }
  const maxAbs = rows.reduce((acc, row) => {
    const z = Math.abs(row.zscore ?? 0);
    return z > acc ? z : acc;
  }, 0);
  const scale = maxAbs > 0 ? maxAbs : 1;

  const fragment = document.createDocumentFragment();

  rows.forEach((row) => {
    const z = row.zscore ?? 0;
    const ratio = Math.min(Math.abs(z) / scale, 1);
    const widthPercent = ratio * 50;

    const rowEl = document.createElement("div");
    rowEl.className = "bar-row";

    const symbolEl = document.createElement("div");
    symbolEl.className = "symbol";
    symbolEl.textContent = row.symbol ?? `MKT-${row.market_id}`;

    const barWrapper = document.createElement("div");
    barWrapper.className = "bar-wrapper";

    const zeroLine = document.createElement("div");
    zeroLine.className = "zero-line";
    barWrapper.appendChild(zeroLine);

    const segment = document.createElement("div");
    segment.className = z >= 0 ? "bar-positive" : "bar-negative";
    segment.style.width = `${widthPercent}%`;
    if (z >= 0) {
      segment.style.left = "50%";
    } else {
      segment.style.left = `${50 - widthPercent}%`;
    }
    barWrapper.appendChild(segment);

    const valueEl = document.createElement("div");
    valueEl.className = "value";
    const zText = Number.isFinite(z) && maxAbs > 0 ? formatter.format(z) : "—";
    const fundingText = Number.isFinite(row.funding_rate)
      ? fundingFormatter.format(row.funding_rate)
      : "—";
    valueEl.textContent = `${zText}σ / ${fundingText}`;

    rowEl.appendChild(symbolEl);
    rowEl.appendChild(barWrapper);
    rowEl.appendChild(valueEl);
    fragment.appendChild(rowEl);
  });

  chart.innerHTML = "";
  chart.appendChild(fragment);
  setTimestamp(timestamp);
}

function connect() {
  const scheme = window.location.protocol === "https:" ? "wss" : "ws";
  const socketUrl = `${scheme}://${window.location.host}/ws/funding`;
  const socket = new WebSocket(socketUrl);

  socket.addEventListener("open", () => {
    retryDelay = 1000;
  });

  socket.addEventListener("message", (event) => {
    let payload;
    try {
      payload = JSON.parse(event.data);
    } catch (err) {
      return;
    }
    if (!payload || payload.type !== "snapshot") {
      return;
    }
    renderRows(payload.rows ?? [], payload.timestamp);
  });

  socket.addEventListener("close", () => {
    renderEmpty("Disconnected – retrying…");
    setTimeout(() => connect(), retryDelay);
    retryDelay = Math.min(retryDelay * 2, 15000);
  });

  socket.addEventListener("error", () => {
    socket.close();
  });
}

renderEmpty();
connect();
