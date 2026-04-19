import { readFile } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

import { chromium } from "playwright";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const WEB_DIR = dirname(SCRIPT_DIR);
const REPO_ROOT = dirname(WEB_DIR);
const DEFAULT_URL = "https://mpaladin.com/forecast/";
const DEFAULT_GPX = resolve(REPO_ROOT, "tests/fixtures/sample_route.gpx");
const WAIT_INTERVAL_MS = 5_000;
const DEFAULT_WAIT_TIMEOUT_MS = 180_000;
const DEFAULT_PAGE_TIMEOUT_MS = 120_000;

function parseArgs(argv) {
  const options = {
    url: DEFAULT_URL,
    gpx: DEFAULT_GPX,
    waitTimeoutMs: DEFAULT_WAIT_TIMEOUT_MS,
    pageTimeoutMs: DEFAULT_PAGE_TIMEOUT_MS,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const current = argv[index];
    if (current === "--url") {
      options.url = argv[index + 1] ?? options.url;
      index += 1;
      continue;
    }
    if (current === "--gpx") {
      options.gpx = resolve(process.cwd(), argv[index + 1] ?? options.gpx);
      index += 1;
      continue;
    }
    if (current === "--wait-timeout-ms") {
      options.waitTimeoutMs = Number(argv[index + 1] ?? options.waitTimeoutMs);
      index += 1;
      continue;
    }
    if (current === "--page-timeout-ms") {
      options.pageTimeoutMs = Number(argv[index + 1] ?? options.pageTimeoutMs);
      index += 1;
    }
  }

  if (!options.url.endsWith("/")) {
    options.url += "/";
  }
  return options;
}

function readExpectedAssets(indexHtml) {
  const jsMatch = indexHtml.match(/src="\.\/assets\/([^"]+\.js)"/);
  const cssMatch = indexHtml.match(/href="\.\/assets\/([^"]+\.css)"/);
  if (!jsMatch || !cssMatch) {
    throw new Error("Could not determine expected asset names from web/dist/index.html");
  }
  return {
    jsAsset: jsMatch[1],
    cssAsset: cssMatch[1],
  };
}

async function waitForLiveAssets(url, expectedAssets, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const response = await fetch(url, {
      headers: {
        "user-agent": "trailintel-deployed-smoke/0.1",
      },
    });
    if (response.ok) {
      const body = await response.text();
      if (
        body.includes(expectedAssets.jsAsset) &&
        body.includes(expectedAssets.cssAsset)
      ) {
        return;
      }
    }
    await new Promise((resolveDelay) => {
      setTimeout(resolveDelay, WAIT_INTERVAL_MS);
    });
  }

  throw new Error(
    `Timed out waiting for ${expectedAssets.jsAsset} and ${expectedAssets.cssAsset} at ${url}`,
  );
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const indexHtml = await readFile(resolve(WEB_DIR, "dist/index.html"), "utf-8");
  const expectedAssets = readExpectedAssets(indexHtml);
  await waitForLiveAssets(options.url, expectedAssets, options.waitTimeoutMs);

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1900 } });
  const consoleMessages = [];
  const pageErrors = [];

  page.on("console", (message) => {
    consoleMessages.push(`${message.type()}: ${message.text()}`);
  });
  page.on("pageerror", (error) => {
    pageErrors.push(String(error));
  });

  try {
    await page.goto(options.url, {
      waitUntil: "networkidle",
      timeout: options.pageTimeoutMs,
    });
    await page.setInputFiles("#gpx-input", options.gpx);
    await page.click("#generate-button");
    await page.waitForSelector("text=Forecast Report", {
      timeout: options.pageTimeoutMs,
    });
    await page.waitForSelector("text=uPlot Forecast Charts", {
      timeout: options.pageTimeoutMs,
    });
    await page.waitForSelector("text=OpenStreetMap Overview", {
      timeout: options.pageTimeoutMs,
    });
    await page.waitForFunction(
      () => document.querySelectorAll(".uplot").length >= 6,
      null,
      { timeout: options.pageTimeoutMs },
    );
    await page.waitForFunction(
      () => !!document.querySelector(".leaflet-container"),
      null,
      { timeout: options.pageTimeoutMs },
    );

    const reportTitle = await page.locator(".report-hero h2").textContent();
    const statusText = await page.locator("#status-panel").textContent();
    const chartHeadings = await page
      .locator(".forecast-chart-card h3")
      .allTextContents();
    const mapNote = await page.locator("#forecast-map-note").textContent();
    const chartCount = await page.locator(".uplot").count();

    if (pageErrors.length) {
      throw new Error(`Browser page errors: ${pageErrors.join(" | ")}`);
    }

    console.log(
      JSON.stringify(
        {
          ok: true,
          url: options.url,
          reportTitle,
          statusText,
          chartCount,
          chartHeadings,
          mapNote,
          expectedAssets,
          consoleMessages,
        },
        null,
        2,
      ),
    );
  } finally {
    await browser.close();
  }
}

await main();
