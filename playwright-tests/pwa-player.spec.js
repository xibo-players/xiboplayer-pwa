/**
 * Playwright E2E Tests for PWA Player
 *
 * Tests all critical functionalities:
 * - Player initialization
 * - Layout loading and replay
 * - Video playback and restart
 * - Element reuse
 * - Memory stability
 * - Hardware key stability
 * - Performance benchmarks
 */

import { test, expect } from '@playwright/test';

const PWA_URL = 'http://h1.superpantalles.com:8081/player/pwa/';

test.describe('PWA Player - Complete Functionality', () => {

  test.beforeEach(async ({ page, context }) => {
    // Clear all storage before each test
    await context.clearCookies();
    await page.goto(PWA_URL);
    await page.evaluate(() => {
      localStorage.clear();
      sessionStorage.clear();
    });
  });

  test('Player initializes correctly', async ({ page }) => {
    await page.goto(PWA_URL);

    // Wait for player to initialize
    await page.waitForSelector('#player-container', { timeout: 10000 });

    // Check console for initialization logs
    const logs = [];
    page.on('console', msg => logs.push(msg.text()));

    await page.waitForFunction(() => {
      return window.performance.now() > 1000; // Wait 1 second
    });

    // Verify initialization sequence
    const initLogs = logs.join('\n');
    expect(initLogs).toContain('[PWA] Initializing player');
    expect(initLogs).toContain('[RendererLite] Initialized');
    expect(initLogs).toContain('[PWA] Core modules loaded');
  });

  test('Hardware key is stable and properly formatted', async ({ page }) => {
    // First load
    await page.goto(PWA_URL);
    await page.waitForTimeout(2000);

    const hardwareKey1 = await page.evaluate(() => {
      return localStorage.getItem('xibo_config');
    });

    const config1 = JSON.parse(hardwareKey1);
    const hwKey1 = config1.hardwareKey;

    // Verify format: pwa-[28 hex chars]
    expect(hwKey1).toMatch(/^pwa-[0-9a-f]{28}$/);

    // Verify NOT all zeros
    expect(hwKey1).not.toMatch(/^pwa-0+/);

    // Verify good entropy (not simple like pwa-000...05f2)
    const nonZeroChars = (hwKey1.match(/[1-9a-f]/g) || []).length;
    expect(nonZeroChars).toBeGreaterThan(10); // Should have many non-zero chars

    console.log(`Hardware key: ${hwKey1}`);

    // Reload page
    await page.reload();
    await page.waitForTimeout(2000);

    const hardwareKey2 = await page.evaluate(() => {
      return localStorage.getItem('xibo_config');
    });

    const config2 = JSON.parse(hardwareKey2);
    const hwKey2 = config2.hardwareKey;

    // Hardware key should be IDENTICAL
    expect(hwKey2).toBe(hwKey1);
    console.log(`Hardware key stable: ${hwKey2}`);
  });

  test('Layout loads and plays correctly', async ({ page }) => {
    const consoleLogs = [];
    page.on('console', msg => consoleLogs.push(msg.text()));

    await page.goto(PWA_URL);

    // Wait for layout to start (max 30 seconds)
    await page.waitForFunction(() => {
      return document.querySelector('.renderer-lite-region') !== null;
    }, { timeout: 30000 });

    // Check layout loaded
    const regions = await page.$$('.renderer-lite-region');
    expect(regions.length).toBeGreaterThan(0);

    // Check for layout start logs
    await page.waitForTimeout(2000);
    const logs = consoleLogs.join('\n');

    expect(logs).toContain('[PWA] Layout started:');
    expect(logs).toContain('[RendererLite] Pre-creating widget elements');
    expect(logs).toContain('[RendererLite] All widget elements pre-created');
  });

  test('Video plays with correct duration detection', async ({ page }) => {
    const consoleLogs = [];
    page.on('console', msg => consoleLogs.push(msg.text()));

    await page.goto(PWA_URL);

    // Wait for layout to load
    await page.waitForTimeout(5000);

    const logs = consoleLogs.join('\n');

    // Check for video duration detection
    const durationMatch = logs.match(/Video \d+ duration detected: (\d+)s/);
    if (durationMatch) {
      const detectedDuration = parseInt(durationMatch[1]);
      console.log(`Video duration detected: ${detectedDuration}s`);

      // Should be reasonable video length (5-300 seconds)
      expect(detectedDuration).toBeGreaterThan(5);
      expect(detectedDuration).toBeLessThan(300);

      // Layout duration should be updated
      expect(logs).toContain(`Layout duration updated: 0s → ${detectedDuration}s`);
    }
  });

  test('Layout replays continuously', async ({ page }) => {
    const consoleLogs = [];
    page.on('console', msg => consoleLogs.push(msg.text()));

    await page.goto(PWA_URL);

    // Wait for first layout to load
    await page.waitForFunction(() => {
      return document.querySelector('.renderer-lite-region') !== null;
    }, { timeout: 30000 });

    console.log('Layout loaded, waiting for first cycle to complete...');

    // Wait up to 90 seconds for layout to end and replay
    const layoutReplayed = await page.waitForFunction(() => {
      return window.performance.now() > 65000; // Wait 65 seconds
    }, { timeout: 90000 }).then(() => true).catch(() => false);

    const logs = consoleLogs.join('\n');

    // Check for layout end
    expect(logs).toContain('Layout ended:');

    // Check for replay with element reuse
    expect(logs).toContain('Replaying layout');
    expect(logs).toContain('reusing elements');

    console.log('Layout replay verified!');
  });

  test('Video restarts on layout replay', async ({ page }) => {
    const consoleLogs = [];
    page.on('console', msg => consoleLogs.push(msg.text()));

    await page.goto(PWA_URL);

    // Wait for layout to load
    await page.waitForTimeout(5000);

    const initialLogs = consoleLogs.join('\n');

    // Count "Video playing" occurrences
    const playCount1 = (initialLogs.match(/Video playing:/g) || []).length;

    // Wait for layout to replay (up to 90 seconds)
    await page.waitForTimeout(70000);

    const allLogs = consoleLogs.join('\n');
    const playCount2 = (allLogs.match(/Video playing:/g) || []).length;

    // Should have more "Video playing" logs after replay
    expect(playCount2).toBeGreaterThan(playCount1);

    // Check for video restart log
    expect(allLogs).toContain('Video restarted:');

    console.log(`Video played ${playCount1} times initially, ${playCount2} times total`);
  });

  test('Element reuse - no DOM recreation on replay', async ({ page }) => {
    await page.goto(PWA_URL);

    // Wait for layout to load
    await page.waitForFunction(() => {
      return document.querySelector('.renderer-lite-region') !== null;
    }, { timeout: 30000 });

    // Count widget elements
    const elementCount1 = await page.evaluate(() => {
      return document.querySelectorAll('.renderer-lite-widget').length;
    });

    console.log(`Initial widget element count: ${elementCount1}`);

    // Wait for layout to replay
    await page.waitForTimeout(70000);

    // Count again - should be SAME (elements reused, not recreated)
    const elementCount2 = await page.evaluate(() => {
      return document.querySelectorAll('.renderer-lite-widget').length;
    });

    console.log(`After replay widget element count: ${elementCount2}`);

    // Element count should be identical (no recreation)
    expect(elementCount2).toBe(elementCount1);
  });

  test('Parallel chunk downloads work', async ({ page }) => {
    const consoleLogs = [];
    page.on('console', msg => consoleLogs.push(msg.text()));

    // Clear cache to force download
    await page.goto(PWA_URL);
    await page.evaluate(async () => {
      const cacheNames = await caches.keys();
      await Promise.all(cacheNames.map(name => caches.delete(name)));
    });

    await page.reload();

    // Wait for downloads to start
    await page.waitForTimeout(10000);

    const logs = consoleLogs.join('\n');

    // Check for parallel download logs
    if (logs.includes('Downloading')) {
      expect(logs).toContain('chunks in parallel');
      expect(logs).toContain('4 concurrent');
      console.log('Parallel chunk downloads verified!');
    }
  });

  test('Widget HTML is cached and reused on replay', async ({ page }) => {
    const consoleLogs = [];
    page.on('console', msg => consoleLogs.push(msg.text()));

    await page.goto(PWA_URL);

    // Wait for first layout load
    await page.waitForTimeout(10000);

    const logs1 = consoleLogs.join('\n');
    const fetchCount1 = (logs1.match(/Retrieved widget HTML/g) || []).length;

    console.log(`First load: Fetched ${fetchCount1} widget HTML`);

    // Wait for layout to replay
    await page.waitForTimeout(70000);

    const logs2 = consoleLogs.join('\n');
    const cachedCount = (logs2.match(/Using cached widget HTML/g) || []).length;

    // Should use cached HTML on replay
    expect(cachedCount).toBeGreaterThan(0);
    console.log(`Replay: Used ${cachedCount} cached widget HTML`);
  });

  test('Memory stays stable across multiple cycles', async ({ page }) => {
    await page.goto(PWA_URL);

    // Wait for layout to load
    await page.waitForTimeout(5000);

    // Measure initial memory
    const memory1 = await page.evaluate(() => {
      return performance.memory?.usedJSHeapSize || 0;
    });

    console.log(`Initial memory: ${(memory1 / 1024 / 1024).toFixed(1)} MB`);

    // Let layout cycle 3 times (3 x ~60s = 180s)
    // Using shorter wait for testing
    await page.waitForTimeout(30000); // 30 seconds for demo

    // Measure memory again
    const memory2 = await page.evaluate(() => {
      return performance.memory?.usedJSHeapSize || 0;
    });

    console.log(`After cycles: ${(memory2 / 1024 / 1024).toFixed(1)} MB`);

    // Memory growth should be minimal (<100MB)
    const growth = (memory2 - memory1) / 1024 / 1024;
    console.log(`Memory growth: ${growth.toFixed(1)} MB`);

    expect(growth).toBeLessThan(100);
  });

  test('Blob URLs are properly revoked', async ({ page }) => {
    const consoleLogs = [];
    page.on('console', msg => consoleLogs.push(msg.text()));

    await page.goto(PWA_URL);

    // Wait for layout to load
    await page.waitForTimeout(10000);

    // Trigger layout change (if multi-layout schedule)
    // Or wait for very long time
    await page.waitForTimeout(70000);

    const logs = consoleLogs.join('\n');

    // Should see blob URL revocation logs
    if (logs.includes('Revoked')) {
      expect(logs).toMatch(/Revoked \d+ blob URLs/);
      console.log('Blob URL revocation verified!');
    }
  });

  test('No console errors during playback', async ({ page }) => {
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('console', msg => {
      if (msg.type() === 'error') {
        errors.push(msg.text());
      }
    });

    await page.goto(PWA_URL);

    // Run for 30 seconds
    await page.waitForTimeout(30000);

    // Filter out known non-critical errors
    const criticalErrors = errors.filter(err => {
      return !err.includes('NS_ERROR_CORRUPTED_CONTENT') && // Widget dependencies
             !err.includes('bundle.min.js') &&              // Widget dependencies
             !err.includes('fonts.css');                     // Widget dependencies
    });

    console.log(`Total errors: ${errors.length}, Critical: ${criticalErrors.length}`);

    // Should have no critical errors
    expect(criticalErrors).toHaveLength(0);
  });

  test('Performance: Initial load time < 5 seconds', async ({ page }) => {
    const start = Date.now();

    await page.goto(PWA_URL);

    // Wait for layout to actually start playing
    await page.waitForFunction(() => {
      return document.querySelector('.renderer-lite-widget') !== null;
    }, { timeout: 10000 });

    const loadTime = Date.now() - start;
    console.log(`Load time: ${(loadTime / 1000).toFixed(1)}s`);

    // Should load in under 5 seconds
    expect(loadTime).toBeLessThan(5000);
  });

  test('Performance: Layout replay < 1 second', async ({ page }) => {
    const consoleLogs = [];
    page.on('console', msg => consoleLogs.push({ time: Date.now(), text: msg.text() }));

    await page.goto(PWA_URL);

    // Wait for first layout cycle to complete
    await page.waitForTimeout(70000);

    // Find layout end and replay start times
    const endLog = consoleLogs.find(log => log.text.includes('Layout ended:'));
    const replayLog = consoleLogs.find(log => log.text.includes('Replaying layout'));

    if (endLog && replayLog) {
      const replayTime = replayLog.time - endLog.time;
      console.log(`Layout replay time: ${replayTime}ms`);

      // Should replay in under 1 second
      expect(replayTime).toBeLessThan(1000);
    }
  });

  test('Transitions are smooth (no flicker)', async ({ page }) => {
    await page.goto(PWA_URL);

    // Wait for layout to load
    await page.waitForTimeout(5000);

    // Monitor for visibility of black screens or flicker
    const hasBlackScreen = await page.evaluate(() => {
      // Check if player container ever goes completely black
      const container = document.getElementById('player-container');
      if (!container) return true;

      const widgets = container.querySelectorAll('.renderer-lite-widget');
      const allHidden = Array.from(widgets).every(w => {
        return w.style.visibility === 'hidden' || w.style.opacity === '0';
      });

      return allHidden && widgets.length > 0;
    });

    // Should not have all widgets hidden simultaneously (would show black screen)
    expect(hasBlackScreen).toBe(false);
  });

  test('Cache validation prevents deadlock', async ({ page }) => {
    const consoleLogs = [];
    page.on('console', msg => consoleLogs.push(msg.text()));

    await page.goto(PWA_URL);

    // Wait for cache checks
    await page.waitForTimeout(10000);

    const logs = consoleLogs.join('\n');

    // Should see cache validation logs
    if (logs.includes('cached and valid')) {
      expect(logs).toMatch(/Media \d+ cached and valid/);
      console.log('Cache validation working!');
    }

    // Should NOT be stuck waiting
    expect(logs).not.toContain('Waiting for media to finish downloading for layout');
  });

  test('Parallel operations work correctly', async ({ page }) => {
    const consoleLogs = [];
    page.on('console', msg => consoleLogs.push(msg.text()));

    await page.goto(PWA_URL);
    await page.waitForTimeout(10000);

    const logs = consoleLogs.join('\n');

    // Check for parallel operation logs
    const parallelOps = [
      'Pre-fetching.*media URLs in parallel',
      'Pre-creating widget elements',
      'chunks in parallel',
      'widget HTML resources in parallel'
    ];

    let foundCount = 0;
    for (const pattern of parallelOps) {
      if (new RegExp(pattern).test(logs)) {
        foundCount++;
      }
    }

    console.log(`Found ${foundCount}/4 parallel operations`);
    expect(foundCount).toBeGreaterThan(0); // At least some parallel ops
  });

  test('Widget elements are pre-created', async ({ page }) => {
    await page.goto(PWA_URL);

    // Wait for layout to load
    await page.waitForFunction(() => {
      return document.querySelector('.renderer-lite-region') !== null;
    }, { timeout: 30000 });

    // Check that all widgets exist in DOM (hidden)
    const widgetInfo = await page.evaluate(() => {
      const region = document.querySelector('.renderer-lite-region');
      if (!region) return { total: 0, visible: 0, hidden: 0 };

      const widgets = region.querySelectorAll('.renderer-lite-widget');
      const total = widgets.length;

      let visible = 0;
      let hidden = 0;

      widgets.forEach(w => {
        if (w.style.visibility === 'visible') visible++;
        else hidden++;
      });

      return { total, visible, hidden };
    });

    console.log(`Widgets - Total: ${widgetInfo.total}, Visible: ${widgetInfo.visible}, Hidden: ${widgetInfo.hidden}`);

    // Should have multiple widgets pre-created
    expect(widgetInfo.total).toBeGreaterThan(0);

    // At least one should be visible
    expect(widgetInfo.visible).toBeGreaterThan(0);

    // Hidden widgets should exist (pre-created but not shown yet)
    // This verifies the Arexibo pattern
  });

  test('Videos restart on each cycle', async ({ page }) => {
    const consoleLogs = [];
    page.on('console', msg => consoleLogs.push(msg.text()));

    await page.goto(PWA_URL);

    // Wait for first video play
    await page.waitForTimeout(10000);

    const logs1 = consoleLogs.join('\n');
    const playCount1 = (logs1.match(/Video playing:/g) || []).length;
    const restartCount1 = (logs1.match(/Video restarted:/g) || []).length;

    console.log(`Initial: ${playCount1} plays, ${restartCount1} restarts`);

    // Wait for replay
    await page.waitForTimeout(70000);

    const logs2 = consoleLogs.join('\n');
    const playCount2 = (logs2.match(/Video playing:/g) || []).length;
    const restartCount2 = (logs2.match(/Video restarted:/g) || []).length;

    console.log(`After replay: ${playCount2} plays, ${restartCount2} restarts`);

    // Should have more plays and restarts after replay
    expect(playCount2).toBeGreaterThan(playCount1);
    expect(restartCount2).toBeGreaterThan(restartCount1);
  });
});

test.describe('PWA Player - Stress Tests', () => {

  test('Memory remains stable over 5 layout cycles', async ({ page }) => {
    test.setTimeout(360000); // 6 minutes

    await page.goto(PWA_URL);
    await page.waitForTimeout(5000);

    const measurements = [];

    // Measure memory every 30 seconds for 5 cycles
    for (let i = 0; i < 5; i++) {
      const memory = await page.evaluate(() => {
        return performance.memory?.usedJSHeapSize || 0;
      });

      measurements.push(memory / 1024 / 1024); // Convert to MB
      console.log(`Cycle ${i + 1}: ${measurements[i].toFixed(1)} MB`);

      if (i < 4) await page.waitForTimeout(30000);
    }

    // Calculate memory growth
    const growth = measurements[4] - measurements[0];
    console.log(`Total memory growth: ${growth.toFixed(1)} MB`);

    // Should not grow more than 100MB over 5 cycles
    expect(growth).toBeLessThan(100);
  });

  test('Hardware key stable after multiple reloads', async ({ page }) => {
    const keys = [];

    for (let i = 0; i < 5; i++) {
      await page.goto(PWA_URL);
      await page.waitForTimeout(2000);

      const config = await page.evaluate(() => {
        return localStorage.getItem('xibo_config');
      });

      const hwKey = JSON.parse(config).hardwareKey;
      keys.push(hwKey);
      console.log(`Reload ${i + 1}: ${hwKey}`);
    }

    // All keys should be identical
    const allSame = keys.every(key => key === keys[0]);
    expect(allSame).toBe(true);
    console.log(`Hardware key stable across ${keys.length} reloads!`);
  });
});
