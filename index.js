/**
 * Repository Module
 *
 * Loads step templates and workflow templates from:
 * 1. GitHub repository (primary, with caching)
 * 2. Local files (fallback)
 *
 * Environment variables:
 * - GITHUB_REPO: Repository in format "owner/repo" (default: CEREMA/airjobs)
 * - GITHUB_BRANCH: Branch to fetch from (default: main)
 * - GITHUB_TOKEN: Optional token for private repos
 * - REPOSITORY_PATH: Path within repo to repository folder (default: src/repository)
 * - REPOSITORY_CACHE_TTL: Cache TTL in seconds (default: 300 = 5 minutes)
 * - REPOSITORY_SOURCE: "github" or "local" (default: github)
 */

import { readdir, readFile, writeFile, mkdir } from 'fs/promises';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { existsSync } from 'fs';

const __dirname = dirname(fileURLToPath(import.meta.url));

// Configuration
const config = {
  repo: process.env.GITHUB_REPO || 'CEREMA/airjobs',
  branch: process.env.GITHUB_BRANCH || 'main',
  token: process.env.GITHUB_TOKEN || null,
  repoPath: process.env.REPOSITORY_PATH || 'src/repository',
  cacheTTL: parseInt(process.env.REPOSITORY_CACHE_TTL || '300', 10) * 1000, // Convert to ms
  source: process.env.REPOSITORY_SOURCE || 'local', // Start with local until GitHub is populated
};

// Cache state
let stepsCache = null;
let workflowsCache = null;
let lastFetchTime = 0;
let isFetching = false;

// Local cache directory for GitHub-fetched content
const cacheDir = join(__dirname, '.cache');

/**
 * Fetch a file from GitHub API
 */
async function fetchFromGitHub(path) {
  const url = `https://api.github.com/repos/${config.repo}/contents/${path}?ref=${config.branch}`;

  const headers = {
    'Accept': 'application/vnd.github.v3+json',
    'User-Agent': 'AirJobs-Repository-Sync',
  };

  if (config.token) {
    headers['Authorization'] = `token ${config.token}`;
  }

  const response = await fetch(url, { headers });

  if (!response.ok) {
    throw new Error(`GitHub API error: ${response.status} ${response.statusText}`);
  }

  return response.json();
}

/**
 * Fetch directory listing from GitHub
 */
async function fetchGitHubDirectory(subPath) {
  const fullPath = `${config.repoPath}/${subPath}`;
  const contents = await fetchFromGitHub(fullPath);

  if (!Array.isArray(contents)) {
    throw new Error(`Expected directory listing for ${fullPath}`);
  }

  return contents;
}

/**
 * Fetch and decode a JSON file from GitHub
 */
async function fetchGitHubFile(filePath) {
  const fullPath = `${config.repoPath}/${filePath}`;
  const file = await fetchFromGitHub(fullPath);

  if (file.type !== 'file' || !file.content) {
    throw new Error(`Expected file content for ${fullPath}`);
  }

  // GitHub returns base64 encoded content
  const content = Buffer.from(file.content, 'base64').toString('utf-8');
  return JSON.parse(content);
}

/**
 * Load templates from GitHub
 */
async function loadFromGitHub(type) {
  const items = [];

  try {
    const directory = await fetchGitHubDirectory(type);

    for (const file of directory) {
      if (file.type === 'file' && file.name.endsWith('.json')) {
        try {
          const item = await fetchGitHubFile(`${type}/${file.name}`);
          items.push(item);
        } catch (err) {
          console.error(`Failed to fetch ${type}/${file.name}:`, err.message);
        }
      }
    }

    // Save to local cache for fallback
    await saveToCacheDir(type, items);

  } catch (err) {
    console.error(`Failed to load ${type} from GitHub:`, err.message);

    // Try to load from local cache
    const cached = await loadFromCacheDir(type);
    if (cached) {
      console.log(`Using cached ${type} from previous fetch`);
      return cached;
    }

    // Fall back to bundled files
    console.log(`Falling back to bundled ${type}`);
    return loadFromLocal(type);
  }

  return items;
}

/**
 * Save items to local cache directory
 */
async function saveToCacheDir(type, items) {
  try {
    const typeDir = join(cacheDir, type);
    await mkdir(typeDir, { recursive: true });

    for (const item of items) {
      const filePath = join(typeDir, `${item.id}.json`);
      await writeFile(filePath, JSON.stringify(item, null, 2));
    }
  } catch (err) {
    console.error(`Failed to save ${type} to cache:`, err.message);
  }
}

/**
 * Load items from local cache directory
 */
async function loadFromCacheDir(type) {
  try {
    const typeDir = join(cacheDir, type);
    if (!existsSync(typeDir)) return null;

    const files = await readdir(typeDir);
    const items = [];

    for (const file of files) {
      if (!file.endsWith('.json')) continue;
      const content = await readFile(join(typeDir, file), 'utf-8');
      items.push(JSON.parse(content));
    }

    return items.length > 0 ? items : null;
  } catch (err) {
    return null;
  }
}

/**
 * Load templates from local bundled files
 */
async function loadFromLocal(type) {
  const typeDir = join(__dirname, type);
  const items = [];

  try {
    const files = await readdir(typeDir);

    for (const file of files) {
      if (!file.endsWith('.json')) continue;
      try {
        const content = await readFile(join(typeDir, file), 'utf-8');
        items.push(JSON.parse(content));
      } catch (err) {
        console.error(`Failed to load ${type}/${file}:`, err.message);
      }
    }
  } catch (err) {
    console.error(`Failed to read local ${type} directory:`, err.message);
  }

  return items;
}

/**
 * Check if cache needs refresh
 */
function isCacheStale() {
  return Date.now() - lastFetchTime > config.cacheTTL;
}

/**
 * Load templates with caching
 */
async function loadWithCache(type, cache) {
  // Return cache if still valid
  if (cache && !isCacheStale()) {
    return cache;
  }

  // Prevent concurrent fetches
  if (isFetching) {
    return cache || [];
  }

  isFetching = true;

  try {
    let items;

    if (config.source === 'github') {
      items = await loadFromGitHub(type);
    } else {
      items = await loadFromLocal(type);
    }

    lastFetchTime = Date.now();
    return items;
  } finally {
    isFetching = false;
  }
}

/**
 * Load all step templates
 */
export async function loadSteps() {
  stepsCache = await loadWithCache('steps', stepsCache);
  return stepsCache;
}

/**
 * Load all workflow templates
 */
export async function loadWorkflows() {
  workflowsCache = await loadWithCache('workflows', workflowsCache);
  return workflowsCache;
}

/**
 * Get a step template by ID
 */
export async function getStepById(id) {
  const steps = await loadSteps();
  return steps.find(s => s.id === id);
}

/**
 * Get a workflow template by ID
 */
export async function getWorkflowById(id) {
  const workflows = await loadWorkflows();
  return workflows.find(w => w.id === id);
}

/**
 * Force refresh the cache from source
 */
export async function refreshCache() {
  stepsCache = null;
  workflowsCache = null;
  lastFetchTime = 0;

  // Reload both
  await Promise.all([loadSteps(), loadWorkflows()]);

  return {
    steps: stepsCache?.length || 0,
    workflows: workflowsCache?.length || 0,
    source: config.source,
    timestamp: new Date().toISOString(),
  };
}

/**
 * Get cache status
 */
export function getCacheStatus() {
  return {
    source: config.source,
    repo: config.source === 'github' ? config.repo : 'local',
    branch: config.source === 'github' ? config.branch : null,
    cacheTTL: config.cacheTTL / 1000,
    lastFetch: lastFetchTime ? new Date(lastFetchTime).toISOString() : null,
    isStale: isCacheStale(),
    stepsCount: stepsCache?.length || 0,
    workflowsCount: workflowsCache?.length || 0,
  };
}

/**
 * Update configuration at runtime
 */
export function updateConfig(newConfig) {
  if (newConfig.repo) config.repo = newConfig.repo;
  if (newConfig.branch) config.branch = newConfig.branch;
  if (newConfig.token) config.token = newConfig.token;
  if (newConfig.repoPath) config.repoPath = newConfig.repoPath;
  if (newConfig.cacheTTL) config.cacheTTL = newConfig.cacheTTL * 1000;
  if (newConfig.source) config.source = newConfig.source;

  // Clear cache to force reload with new config
  stepsCache = null;
  workflowsCache = null;
  lastFetchTime = 0;
}
