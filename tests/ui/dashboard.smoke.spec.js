import { test, expect } from '@playwright/test';

const BASE_URL = process.env.UI_SMOKE_BASE_URL || 'http://127.0.0.1:3000';
const REQUIRED_MODULE_ASSETS = [
  '/app/shared/dashboard_runtime.js',
  '/app/shared/workspace_requests.js',
  '/app/research/index.js',
  '/app/models/index.js',
  '/app/governance/index.js',
  '/app/replay/index.js',
  '/app/ops/index.js',
];

test.describe('Dashboard UI smoke', () => {
  test('dashboard boots and module assets are reachable', async ({ page }) => {
    const pageErrors = [];
    page.on('pageerror', (error) => pageErrors.push(error.message));

    await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' });
    await expect(page.locator('#insight-tabs')).toBeVisible();

    await expect.poll(
      async () => page.evaluate(() => ({
        runtime: typeof window.PQDashboardRuntime?.getApiOrigin === 'function',
        requests: typeof window.PQWorkspaceRequests?.beginWorkspaceRequest === 'function',
        research: typeof window.PQResearchWorkspace?.createResearchWorkspace === 'function',
        models: typeof window.PQModelsWorkspace?.createModelsWorkspace === 'function',
        governance: typeof window.PQGovernanceWorkspace?.createGovernanceWorkspace === 'function',
        replay: typeof window.PQReplayWorkspace?.createReplayWorkspace === 'function',
        ops: typeof window.PQOpsWorkspace?.createOpsWorkspace === 'function',
      })),
      { message: 'Expected all dashboard module globals to be available' }
    ).toEqual({
      runtime: true,
      requests: true,
      research: true,
      models: true,
      governance: true,
      replay: true,
      ops: true,
    });

    for (const assetPath of REQUIRED_MODULE_ASSETS) {
      const assetUrl = new URL(assetPath, BASE_URL).toString();
      const response = await page.request.get(assetUrl);
      expect(response.status(), `${assetPath} should be reachable`).toBe(200);
    }

    expect(pageErrors, `Unexpected runtime errors: ${pageErrors.join(' | ')}`).toEqual([]);
  });

  test('basic navigation and simple interaction stay stable', async ({ page }) => {
    const pageErrors = [];
    page.on('pageerror', (error) => pageErrors.push(error.message));

    await page.goto(BASE_URL, { waitUntil: 'domcontentloaded' });

    await page.click('#tab-research');
    await expect(page.locator('#panel-research')).toBeVisible();
    await expect(page.locator('#tab-research')).toHaveAttribute('aria-selected', 'true');

    await page.click('#research-reset-btn');
    await expect(page.locator('#research-status')).toBeVisible();

    expect(pageErrors, `Unexpected runtime errors: ${pageErrors.join(' | ')}`).toEqual([]);
  });
});

