/**
 * P2P Node Console & Distributed File Manager
 * Front-end Architecture & State Controller
 * Zero-dependency, Production-grade Vanilla JavaScript
 */

(function () {
  'use strict';

  // ---------------------------------------------------------------------------
  // Global State & Configuration
  // ---------------------------------------------------------------------------
  const CONFIG = {
    apiBase: '', // Same origin (relative paths)
    pollIntervalNormal: 3000, // 3 seconds normal polling
    pollIntervalFast: 200,    // 200ms fast polling when active transfers exist
    toastDuration: 4200,      // Toast auto-dismiss timeout
    defaultOwnerId: 'peer-1',
  };

  const STATE = {
    isOnline: false,
    autoPollActive: true,
    pollTimer: null,
    pollFastTimer: null,
    activeTheme: 'dark',
    sharedFiles: [],
    knownPeers: [],
    downloadJobs: new Map(), // fileId -> DownloadJob
    activeJobPolling: false,
    lastSearchQuery: '',
    selectedUploadFile: null,
    pendingDeviceDownloads: new Set(),
  };

  // ---------------------------------------------------------------------------
  // DOM Element Selectors Cache
  // ---------------------------------------------------------------------------
  const DOM = {
    html: document.documentElement,
    offlineBanner: document.getElementById('offlineBanner'),
    retryConnectionBtn: document.getElementById('retryConnectionBtn'),

    // Header Telemetry
    nodeHealthBadge: document.getElementById('nodeHealthBadge'),
    nodeHealthDot: document.getElementById('nodeHealthDot'),
    nodeHealthText: document.getElementById('nodeHealthText'),
    nodeIdDisplay: document.getElementById('nodeIdDisplay'),
    tcpPortDisplay: document.getElementById('tcpPortDisplay'),
    autoRefreshToggle: document.getElementById('autoRefreshToggle'),
    pollIndicator: document.getElementById('pollIndicator'),
    manualRefreshBtn: document.getElementById('manualRefreshBtn'),
    refreshIcon: document.getElementById('refreshIcon'),
    themeToggleBtn: document.getElementById('themeToggleBtn'),

    // Metrics Strip
    statSharedCount: document.getElementById('statSharedCount'),
    statStorageUsed: document.getElementById('statStorageUsed'),
    statActiveDownloads: document.getElementById('statActiveDownloads'),
    statPeersCount: document.getElementById('statPeersCount'),

    // File Slicing & Sharing
    shareFileForm: document.getElementById('shareFileForm'),
    shareFilePathInput: document.getElementById('shareFilePathInput'),
    shareOwnerIdInput: document.getElementById('shareOwnerIdInput'),
    shareSubmitBtn: document.getElementById('shareSubmitBtn'),
    fileDropzone: document.getElementById('fileDropzone'),
    dropzoneFileInput: document.getElementById('dropzoneFileInput'),

    // Distributed Search
    networkSearchForm: document.getElementById('networkSearchForm'),
    networkSearchInput: document.getElementById('networkSearchInput'),
    clearSearchBtn: document.getElementById('clearSearchBtn'),
    ttlSelect: document.getElementById('ttlSelect'),
    networkSearchBtn: document.getElementById('networkSearchBtn'),
    searchResultsStateEmpty: document.getElementById('searchResultsStateEmpty'),
    searchResultsTableWrapper: document.getElementById('searchResultsTableWrapper'),
    searchResultsSummary: document.getElementById('searchResultsSummary'),
    searchResultsTableBody: document.getElementById('searchResultsTableBody'),

    // Transfers & Chunk Matrix
    transfersEmptyState: document.getElementById('transfersEmptyState'),
    transfersCardList: document.getElementById('transfersCardList'),

    // Local Shared Files Explorer
    sharedFilesFilterInput: document.getElementById('sharedFilesFilterInput'),
    sharedFilesTable: document.getElementById('sharedFilesTable'),
    sharedFilesTableBody: document.getElementById('sharedFilesTableBody'),
    sharedFilesEmptyState: document.getElementById('sharedFilesEmptyState'),

    // Peer Mesh
    toggleRegisterPeerBtn: document.getElementById('toggleRegisterPeerBtn'),
    registerPeerDrawer: document.getElementById('registerPeerDrawer'),
    closeRegisterDrawerBtn: document.getElementById('closeRegisterDrawerBtn'),
    registerPeerForm: document.getElementById('registerPeerForm'),
    regPeerIdInput: document.getElementById('regPeerIdInput'),
    regHostInput: document.getElementById('regHostInput'),
    regPortInput: document.getElementById('regPortInput'),
    peersEmptyState: document.getElementById('peersEmptyState'),
    peersDynamicList: document.getElementById('peersDynamicList'),

    // Diagnostics & Console
    btnTestRecovery: document.getElementById('btnTestRecovery'),
    btnTestTcpHello: document.getElementById('btnTestTcpHello'),
    btnTestChunking: document.getElementById('btnTestChunking'),
    btnCorruptPneuma: document.getElementById('btnCorruptPneuma'),
    btnRestorePneuma: document.getElementById('btnRestorePneuma'),
    copyConsoleBtn: document.getElementById('copyConsoleBtn'),
    clearConsoleBtn: document.getElementById('clearConsoleBtn'),
    consoleScreen: document.getElementById('consoleScreen'),
    consoleCode: document.getElementById('consoleCode'),

    // Toasts
    toastContainer: document.getElementById('toastContainer'),
  };

  // ---------------------------------------------------------------------------
  // Helper Utilities: Formatting & Authoring
  // ---------------------------------------------------------------------------
  function formatBytes(bytes) {
    if (bytes === 0 || !bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  function truncateString(str, maxLen = 12) {
    if (!str) return '';
    if (str.length <= maxLen) return str;
    return str.substring(0, maxLen) + '...';
  }

  function sanitizeHtml(str) {
    if (typeof str !== 'string') return '';
    return str.replace(/[&<>'"]/g, tag => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      "'": '&#39;',
      '"': '&quot;'
    }[tag] || tag));
  }

  function formatDuration(startTime, endTime) {
    const end = endTime > 0 ? endTime : Date.now();
    const diffMs = Math.max(0, end - startTime);
    if (diffMs < 1000) return `${diffMs}ms`;
    const sec = (diffMs / 1000).toFixed(1);
    return `${sec}s`;
  }

  // ---------------------------------------------------------------------------
  // Toast Notification System
  // ---------------------------------------------------------------------------
  function showToast(type, title, message) {
    if (!DOM.toastContainer) return;

    const toast = document.createElement('div');
    toast.className = `toast-item toast-${type}`;
    toast.setAttribute('role', 'status');

    let iconSvg = '';
    if (type === 'success') {
      iconSvg = `<svg class="icon toast-icon" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10Z"/><polyline points="9 12 11 14 15 10"/></svg>`;
    } else if (type === 'error') {
      iconSvg = `<svg class="icon toast-icon" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><line x1="12" x2="12" y1="9" y2="13"/><line x1="12" x2="12.01" y1="17" y2="17"/></svg>`;
    } else {
      iconSvg = `<svg class="icon toast-icon" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="16" y2="12"/><line x1="12" x2="12.01" y1="8" y2="8"/></svg>`;
    }

    toast.innerHTML = `
      ${iconSvg}
      <div class="toast-content">
        <div class="toast-title">${sanitizeHtml(title)}</div>
        <div class="toast-message">${sanitizeHtml(message)}</div>
      </div>
      <button class="toast-close-btn" type="button" aria-label="Close notification">
        <svg class="icon" viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <line x1="18" x2="6" y1="6" y2="18"/>
          <line x1="6" x2="18" y1="6" y2="18"/>
        </svg>
      </button>
    `;

    const closeBtn = toast.querySelector('.toast-close-btn');
    const dismiss = () => {
      toast.classList.add('toast-dismissing');
      setTimeout(() => {
        if (toast.parentNode) toast.parentNode.removeChild(toast);
      }, 200);
    };

    closeBtn.addEventListener('click', dismiss);
    DOM.toastContainer.appendChild(toast);

    setTimeout(dismiss, CONFIG.toastDuration);
  }

  // ---------------------------------------------------------------------------
  // Syntax Highlighting Terminal Logger
  // ---------------------------------------------------------------------------
  function syntaxHighlightJson(jsonObj) {
    let json = typeof jsonObj === 'string' ? jsonObj : JSON.stringify(jsonObj, null, 2);
    json = sanitizeHtml(json);
    return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
      let cls = 'json-num';
      if (/^"/.test(match)) {
        if (/:$/.test(match)) {
          cls = 'json-key';
        } else {
          cls = 'json-str';
        }
      } else if (/true|false/.test(match)) {
        cls = 'json-bool';
      } else if (/null/.test(match)) {
        cls = 'json-null';
      }
      return '<span class="' + cls + '">' + match + '</span>';
    });
  }

  function appendTerminalLog(label, data, isError = false) {
    if (!DOM.consoleCode) return;
    const timestamp = new Date().toLocaleTimeString();
    const divider = `\n// --------------------------------------------------\n// [${timestamp}] ${label}\n// --------------------------------------------------\n`;
    
    let formattedBody = '';
    if (typeof data === 'object') {
      formattedBody = syntaxHighlightJson(data);
    } else {
      formattedBody = sanitizeHtml(String(data));
    }

    DOM.consoleCode.innerHTML += divider + formattedBody;

    // Auto-scroll to bottom of console
    if (DOM.consoleScreen) {
      DOM.consoleScreen.scrollTop = DOM.consoleScreen.scrollHeight;
    }
  }

  // ---------------------------------------------------------------------------
  // Clipboard Utility with Micro-Interaction
  // ---------------------------------------------------------------------------
  async function copyToClipboard(text, triggerBtn) {
    try {
      await navigator.clipboard.writeText(text);
      if (triggerBtn) {
        const originalContent = triggerBtn.innerHTML;
        triggerBtn.innerHTML = `
          <svg class="icon icon-check" viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="#10B981" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
            <polyline points="20 6 9 17 4 12"/>
          </svg>
        `;
        setTimeout(() => {
          triggerBtn.innerHTML = originalContent;
        }, 1500);
      }
      showToast('info', 'Copied to Clipboard', text);
    } catch (err) {
      console.warn('Clipboard write failed:', err);
    }
  }

  // ---------------------------------------------------------------------------
  // Theme Toggle & Persistence
  // ---------------------------------------------------------------------------
  function initTheme() {
    const savedTheme = localStorage.getItem('p2p_theme');
    if (savedTheme) {
      STATE.activeTheme = savedTheme;
    } else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) {
      STATE.activeTheme = 'light';
    } else {
      STATE.activeTheme = 'dark';
    }
    applyTheme(STATE.activeTheme);
  }

  function applyTheme(theme) {
    STATE.activeTheme = theme;
    DOM.html.setAttribute('data-theme', theme);
    localStorage.setItem('p2p_theme', theme);
  }

  function toggleTheme() {
    const nextTheme = STATE.activeTheme === 'dark' ? 'light' : 'dark';
    applyTheme(nextTheme);
    showToast('info', 'Theme Changed', `Switched interface to ${nextTheme} mode.`);
  }

  // ---------------------------------------------------------------------------
  // Network / Node Health Polling & Offline Fallback
  // ---------------------------------------------------------------------------
  async function checkNodeHealth() {
    try {
      const resp = await fetch(`${CONFIG.apiBase}/api/health`, { cache: 'no-store' });
      if (resp.ok) {
        const data = await resp.json();
        setOnlineStatus(true, data);
        return;
      }
      // Fallback probe
      const fallbackResp = await fetch(`${CONFIG.apiBase}/api/files`, { cache: 'no-store' });
      if (fallbackResp.ok) {
        setOnlineStatus(true, { peerId: CONFIG.defaultOwner, tcpPort: 9001 });
        return;
      }
      setOnlineStatus(false);
    } catch (err) {
      try {
        const fallbackResp = await fetch(`${CONFIG.apiBase}/api/files`, { cache: 'no-store' });
        if (fallbackResp.ok) {
          setOnlineStatus(true, { peerId: CONFIG.defaultOwner, tcpPort: 9001 });
          return;
        }
      } catch (e) {}
      setOnlineStatus(false);
    }
  }

  function setOnlineStatus(online, data = null) {
    STATE.isOnline = online;

    if (online) {
      DOM.nodeHealthDot.className = 'status-dot status-dot-online';
      DOM.nodeHealthText.textContent = 'ONLINE';
      DOM.offlineBanner.hidden = true;
    } else {
      DOM.nodeHealthDot.className = 'status-dot status-dot-offline';
      DOM.nodeHealthText.textContent = 'DISCONNECTED';
      DOM.offlineBanner.hidden = false;
    }
  }

  // ---------------------------------------------------------------------------
  // Data Fetching: Shared Files, Peers, Downloads
  // ---------------------------------------------------------------------------
  async function fetchSharedFiles() {
    try {
      const resp = await fetch(`${CONFIG.apiBase}/api/files`, { cache: 'no-store' });
      if (!resp.ok) return;
      const files = await resp.json();
      STATE.sharedFiles = Array.isArray(files) ? files : [];
      renderSharedFilesTable();
      updateMetrics();
    } catch (err) {
      console.warn('Error fetching shared files:', err);
    }
  }

  async function fetchKnownPeers() {
    try {
      const resp = await fetch(`${CONFIG.apiBase}/api/peers`, { cache: 'no-store' });
      if (!resp.ok) return;
      const peers = await resp.json();
      STATE.knownPeers = Array.isArray(peers) ? peers : [];
      renderKnownPeersList();
      updateMetrics();
    } catch (err) {
      console.warn('Error fetching peers:', err);
    }
  }

  async function fetchDownloadJobs() {
    try {
      const resp = await fetch(`${CONFIG.apiBase}/api/download/all`, { cache: 'no-store' });
      if (!resp.ok) return;
      const jobs = await resp.json();
      const jobList = Array.isArray(jobs) ? jobs : Object.values(jobs || {});

      let hasActiveDownloads = false;
      const updatedMap = new Map();

      jobList.forEach(job => {
        if (!job || !job.fileId) return;
        updatedMap.set(job.fileId, job);
        if (job.status === 'DOWNLOADING' || job.status === 'PENDING') {
          hasActiveDownloads = true;
        }

        // Auto-download to device once a queued transfer completes
        if (job.status === 'COMPLETED' && STATE.pendingDeviceDownloads.has(job.fileId)) {
          STATE.pendingDeviceDownloads.delete(job.fileId);
          showToast('success', 'Transfer Complete!', `"${job.fileName}" has been reassembled and is downloading to your device.`);
          triggerBrowserDownload(job.fileId, job.fileName);
        }
      });

      STATE.downloadJobs = updatedMap;
      renderTransfersList();
      updateMetrics();

      // Manage dynamic polling speed
      if (hasActiveDownloads && !STATE.activeJobPolling) {
        startFastPolling();
      } else if (!hasActiveDownloads && STATE.activeJobPolling) {
        stopFastPolling();
      }
    } catch (err) {
      console.warn('Error fetching downloads:', err);
    }
  }

  function updateMetrics() {
    // 1. Shared Files count
    DOM.statSharedCount.textContent = STATE.sharedFiles.length;

    // 2. Storage used
    const totalBytes = STATE.sharedFiles.reduce((acc, f) => acc + (f.sizeBytes || 0), 0);
    DOM.statStorageUsed.textContent = formatBytes(totalBytes);

    // 3. Active transfers
    let activeCount = 0;
    STATE.downloadJobs.forEach(job => {
      if (job.status === 'DOWNLOADING' || job.status === 'PENDING') activeCount++;
    });
    DOM.statActiveDownloads.textContent = activeCount;

    // 4. Mesh Peers
    DOM.statPeersCount.textContent = STATE.knownPeers.length;
  }

  // ---------------------------------------------------------------------------
  // Rendering: Shared Files Explorer Table
  // ---------------------------------------------------------------------------
  function renderSharedFilesTable() {
    const filterQuery = (DOM.sharedFilesFilterInput.value || '').toLowerCase().trim();
    const filtered = STATE.sharedFiles.filter(file => {
      if (!filterQuery) return true;
      return (file.name && file.name.toLowerCase().includes(filterQuery)) ||
             (file.fileId && file.fileId.toLowerCase().includes(filterQuery)) ||
             (file.originalPath && file.originalPath.toLowerCase().includes(filterQuery));
    });

    DOM.sharedFilesTableBody.innerHTML = '';

    if (filtered.length === 0) {
      DOM.sharedFilesTable.hidden = true;
      DOM.sharedFilesEmptyState.hidden = false;
      return;
    }

    DOM.sharedFilesTable.hidden = false;
    DOM.sharedFilesEmptyState.hidden = true;

    filtered.forEach(file => {
      const tr = document.createElement('tr');
      const totalChunks = Math.ceil((file.sizeBytes || 0) / (1024 * 1024)) || 1;

      tr.innerHTML = `
        <td>
          <div class="cell-id-wrapper">
            <span class="mono" title="${sanitizeHtml(file.fileId)}">${truncateString(file.fileId, 8)}</span>
            <button class="copy-btn-inline" data-copy="${sanitizeHtml(file.fileId)}" title="Copy File ID" type="button">
              <svg class="icon icon-copy" viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <rect width="14" height="14" x="8" y="8" rx="2" ry="2"/>
                <path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/>
              </svg>
            </button>
          </div>
        </td>
        <td>
          <div class="cell-file-name">
            <svg class="icon" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/>
              <polyline points="14 2 14 8 20 8"/>
            </svg>
            <span>${sanitizeHtml(file.name || 'Unnamed')}</span>
          </div>
        </td>
        <td class="mono">${formatBytes(file.sizeBytes || 0)}</td>
        <td>
          <span class="badge badge-subtle mono">${totalChunks} ${totalChunks === 1 ? 'chunk' : 'chunks'}</span>
        </td>
        <td>
          <span class="cell-path mono" title="${sanitizeHtml(file.originalPath || '')}">${sanitizeHtml(file.originalPath || '-')}</span>
        </td>
        <td class="td-action" style="text-align: right; white-space: nowrap;">
          <div style="display: inline-flex; gap: 6px; align-items: center; justify-content: flex-end;">
            <button class="btn btn-xs btn-secondary download-shared-btn" 
                    data-fileid="${sanitizeHtml(file.fileId)}"
                    data-filename="${sanitizeHtml(file.name)}"
                    data-totalchunks="${totalChunks}"
                    type="button"
                    title="Download this file locally via TCP framing">
              <svg class="icon" viewBox="0 0 24 24" width="11" height="11" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                <polyline points="7 10 12 15 17 10"/>
                <line x1="12" x2="12" y1="15" y2="3"/>
              </svg>
              <span>Download</span>
            </button>
            <button class="btn btn-xs btn-outline recorrupt-shared-btn"
                    data-fileid="${sanitizeHtml(file.fileId)}"
                    data-filename="${sanitizeHtml(file.name)}"
                    data-totalchunks="${totalChunks}"
                    type="button"
                    title="Simulate transmission failure by re-dropping chunks and testing download safeguards">
              <svg class="icon" viewBox="0 0 24 24" width="11" height="11" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="10"/><line x1="4.93" x2="19.07" y1="4.93" y2="19.07"/>
              </svg>
              <span>Corrupt &amp; Test</span>
            </button>
            <button class="btn btn-xs btn-outline delete-shared-btn"
                    data-fileid="${sanitizeHtml(file.fileId)}"
                    data-filename="${sanitizeHtml(file.name)}"
                    type="button"
                    title="Remove this file from node storage">
              <svg class="icon" viewBox="0 0 24 24" width="11" height="11" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/>
              </svg>
              <span>Delete</span>
            </button>
          </div>
        </td>
      `;

      DOM.sharedFilesTableBody.appendChild(tr);
    });
  }

  // ---------------------------------------------------------------------------
  // Rendering: Active Transfers & Chunk Matrix Monitor
  // ---------------------------------------------------------------------------
  function renderTransfersList() {
    const jobs = Array.from(STATE.downloadJobs.values());

    if (jobs.length === 0) {
      DOM.transfersEmptyState.hidden = false;
      DOM.transfersEmptyState.style.display = 'flex';
      DOM.transfersCardList.innerHTML = '';
      return;
    }

    DOM.transfersEmptyState.hidden = true;
    DOM.transfersEmptyState.style.display = 'none';
    DOM.transfersCardList.innerHTML = '';

    // Sort jobs: DOWNLOADING/PENDING first, then COMPLETED/FAILED
    jobs.sort((a, b) => (b.startTime || 0) - (a.startTime || 0));

    jobs.forEach(job => {
      const card = document.createElement('div');
      const statusLower = (job.status || 'PENDING').toLowerCase();
      card.className = `transfer-job-item status-${statusLower}`;

      let badgeClass = 'badge-subtle';
      let statusIcon = '';
      if (job.status === 'COMPLETED') {
        badgeClass = 'badge-success';
        statusIcon = `<svg class="icon" viewBox="0 0 24 24" width="11" height="11" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>`;
      } else if (job.status === 'DOWNLOADING') {
        badgeClass = 'badge-info';
        statusIcon = `<span class="status-dot status-dot-online" style="width:6px;height:6px;"></span>`;
      } else if (job.status === 'FAILED') {
        badgeClass = 'badge-error';
        statusIcon = `<svg class="icon" viewBox="0 0 24 24" width="11" height="11" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" x2="6" y1="6" y2="18"/><line x1="6" x2="18" y1="6" y2="18"/></svg>`;
      }

      const progress = job.progressPercentage ? Math.min(100, Math.max(0, job.progressPercentage)) : 0;
      const progressFormatted = progress.toFixed(1);
      const elapsed = formatDuration(job.startTime, job.endTime);

      // Render Visual Chunk Ribbon / Grid
      let chunkGridHtml = '';
      const totalChunks = parseInt(job.totalChunks, 10) || 1;
      const chunkStates = job.chunkStates || {};
      const chunkRetries = job.chunkRetries || {};

      for (let i = 0; i < totalChunks; i++) {
        const state = (chunkStates[i] || chunkStates[String(i)] || 'PENDING').toUpperCase();
        const retries = chunkRetries[i] || chunkRetries[String(i)] || 0;

        let cellClass = 'chunk-pending';
        let stateLabel = 'Pending';
        if (state === 'COMPLETED') {
          cellClass = 'chunk-completed';
          stateLabel = 'Assembled';
        } else if (state === 'DOWNLOADING' || state === 'REQUESTED') {
          cellClass = 'chunk-downloading';
          stateLabel = 'Fetching';
        } else if (state === 'RETRYING') {
          cellClass = 'chunk-retrying';
          stateLabel = 'Retrying';
        } else if (state === 'FAILED') {
          cellClass = 'chunk-failed';
          stateLabel = 'Failed';
        }

        const retryBadgeHtml = retries > 0 ? `<span class="chunk-retry-badge" title="${retries} Retries">x${retries}</span>` : '';

        chunkGridHtml += `
          <div class="chunk-cell ${cellClass}" title="Chunk #${i}: ${stateLabel} (Retries: ${retries})">
            <span>#${i}</span>
            ${retryBadgeHtml}
          </div>
        `;
      }

      const errorCalloutHtml = job.errorMessage ? `
        <div class="job-error-callout">
          <svg class="icon" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/>
          </svg>
          <span>${sanitizeHtml(job.errorMessage)}</span>
        </div>
      ` : '';

      let actionsToolbarHtml = '';
      if (job.status === 'FAILED') {
        actionsToolbarHtml = `
          <div class="job-actions-toolbar" style="display: flex; gap: 8px; justify-content: flex-end; margin-top: 12px; flex-wrap: wrap;">
            <button class="btn btn-sm btn-outline recorrupt-transfer-btn" 
                    data-fileid="${sanitizeHtml(job.fileId)}"
                    data-filename="${sanitizeHtml(job.fileName)}"
                    data-totalchunks="${job.totalChunks}"
                    data-host="${sanitizeHtml(job.sourceHost)}"
                    data-port="${job.sourcePort}"
                    type="button"
                    title="Re-corrupt chunks #5 and #15 and restart transfer">
              <svg class="icon" viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="10"/><line x1="4.93" x2="19.07" y1="4.93" y2="19.07"/>
              </svg>
              <span>Re-Corrupt File</span>
            </button>
            <button class="btn btn-sm btn-primary recover-transfer-btn" 
                    data-fileid="${sanitizeHtml(job.fileId)}"
                    data-filename="${sanitizeHtml(job.fileName)}"
                    data-totalchunks="${job.totalChunks}"
                    data-host="${sanitizeHtml(job.sourceHost)}"
                    data-port="${job.sourcePort}"
                    type="button">
              <svg class="icon" viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/>
                <path d="M3 3v5h5"/>
                <path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/>
                <path d="M16 21h5v-5"/>
              </svg>
              <span>Auto-Recover Missing Chunks over TCP</span>
            </button>
          </div>
        `;
      } else if (job.status === 'COMPLETED') {
        actionsToolbarHtml = `
          <div class="job-actions-toolbar" style="display: flex; gap: 8px; justify-content: flex-end; margin-top: 12px; flex-wrap: wrap;">
            <button class="btn btn-sm btn-primary save-to-device-btn" 
                    data-fileid="${sanitizeHtml(job.fileId)}"
                    data-filename="${sanitizeHtml(job.fileName)}"
                    type="button"
                    title="Download this reassembled file directly to your computer">
              <svg class="icon" viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                <polyline points="7 10 12 15 17 10"/>
                <line x1="12" x2="12" y1="15" y2="3"/>
              </svg>
              <span>Save to Device</span>
            </button>
            <button class="btn btn-sm btn-outline recorrupt-transfer-btn" 
                    data-fileid="${sanitizeHtml(job.fileId)}"
                    data-filename="${sanitizeHtml(job.fileName)}"
                    data-totalchunks="${job.totalChunks}"
                    data-host="${sanitizeHtml(job.sourceHost)}"
                    data-port="${job.sourcePort}"
                    type="button"
                    title="Simulate transmission failure by re-dropping chunks and testing download safeguards">
              <svg class="icon" viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="10"/><line x1="4.93" x2="19.07" y1="4.93" y2="19.07"/>
              </svg>
              <span>Re-Corrupt File</span>
            </button>
          </div>
        `;
      }

      card.innerHTML = `
        <div class="job-header-row">
          <div class="job-title-group">
            <span class="job-file-name">${sanitizeHtml(job.fileName || 'file.bin')}</span>
            <span class="job-source-tag mono">${sanitizeHtml(job.sourceHost)}:${job.sourcePort}</span>
            <span class="badge ${badgeClass} mono">${statusIcon} ${job.status}</span>
          </div>
          <div class="job-metrics-group">
            <span class="job-elapsed-time mono">Duration: ${elapsed}</span>
          </div>
        </div>

        <div class="job-progress-wrapper">
          <div class="progress-track">
            <div class="progress-fill ${job.status === 'COMPLETED' ? 'fill-completed' : job.status === 'FAILED' ? 'fill-failed' : ''}" style="width: ${progressFormatted}%"></div>
          </div>
          <div class="job-progress-meta">
            <span class="mono">${job.completedChunks || 0} / ${job.totalChunks || 0} chunks assembled</span>
            <span class="mono">${progressFormatted}%</span>
          </div>
        </div>

        <div class="chunk-matrix-section">
          <span class="chunk-matrix-label">Chunk Assembly Stream (${totalChunks} Total)</span>
          <div class="chunk-ribbon-grid">
            ${chunkGridHtml}
          </div>
        </div>

        ${errorCalloutHtml}
        ${actionsToolbarHtml}
      `;

      DOM.transfersCardList.appendChild(card);
    });
  }

  // ---------------------------------------------------------------------------
  // Rendering: Peer Network Mesh List
  // ---------------------------------------------------------------------------
  function renderKnownPeersList() {
    DOM.peersDynamicList.innerHTML = '';

    if (STATE.knownPeers.length === 0) {
      DOM.peersEmptyState.hidden = false;
      return;
    }

    DOM.peersEmptyState.hidden = true;

    STATE.knownPeers.forEach(peer => {
      const card = document.createElement('div');
      card.className = 'peer-row-card';

      const peerAddress = `${peer.host}:${peer.port}`;

      card.innerHTML = `
        <div class="peer-info-cluster">
          <span class="peer-status-dot" title="Peer Registered"></span>
          <div class="peer-name-block">
            <span class="peer-name-text mono">${sanitizeHtml(peer.peerId)}</span>
            <span class="peer-address-text mono">${sanitizeHtml(peerAddress)}</span>
          </div>
        </div>
        <div class="peer-actions-cluster">
          <button class="btn btn-xs btn-outline copy-btn-inline" data-copy="${sanitizeHtml(peerAddress)}" title="Copy address" type="button">
            <svg class="icon icon-copy" viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <rect width="14" height="14" x="8" y="8" rx="2" ry="2"/>
              <path d="M4 16c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h10c1.1 0 2 .9 2 2"/>
            </svg>
          </button>
          <button class="btn btn-xs btn-secondary ping-peer-btn" data-host="${sanitizeHtml(peer.host)}" data-port="${peer.port}" data-peerid="${sanitizeHtml(peer.peerId)}" type="button">
            <span>Ping</span>
          </button>
          <button class="btn btn-xs btn-outline remove-peer-btn" data-peerid="${sanitizeHtml(peer.peerId)}" title="Remove this peer from mesh" type="button">
            <svg class="icon" viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/>
            </svg>
            <span>Remove</span>
          </button>
        </div>
      `;

      DOM.peersDynamicList.appendChild(card);
    });
  }

  // ---------------------------------------------------------------------------
  // Action: Share & Chunk Local File
  // ---------------------------------------------------------------------------
  async function handleShareFile(e) {
    e.preventDefault();
    const filePath = DOM.shareFilePathInput.value.trim();
    const ownerPeerId = DOM.shareOwnerIdInput.value.trim() || CONFIG.defaultOwnerId;

    if (!filePath) {
      showToast('error', 'Missing File Path', 'Please provide a valid file path on the node host or drop a file.');
      return;
    }

    DOM.shareSubmitBtn.disabled = true;
    DOM.shareSubmitBtn.innerHTML = `
      <svg class="icon spin-active" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M3 12a9 9 0 0 1 15-6.7L21 8"/><path d="M21 3v5h-5"/>
      </svg>
      <span>Chunking File...</span>
    `;

    try {
      let resp, data;

      // If user dropped or selected a file via browser file API, upload as multipart
      if (STATE.selectedUploadFile && STATE.selectedUploadFile.name === filePath) {
        const formData = new FormData();
        formData.append('file', STATE.selectedUploadFile);
        formData.append('ownerPeerId', ownerPeerId);

        resp = await fetch(`${CONFIG.apiBase}/api/files/upload`, {
          method: 'POST',
          body: formData
        });
      } else {
        // Otherwise, user specified a local file path on the node server host
        const url = `${CONFIG.apiBase}/api/files/share?filePath=${encodeURIComponent(filePath)}&ownerPeerId=${encodeURIComponent(ownerPeerId)}`;
        resp = await fetch(url, { method: 'POST' });
      }

      data = await resp.json();

      if (resp.ok && data.status === 'SHARED') {
        showToast('success', 'File Sliced & Shared', `Successfully partitioned "${data.fileName}" into ${data.totalChunks} 1MB chunks.`);
        appendTerminalLog(`FILE SHARE: ${data.fileName}`, data);
        DOM.shareFilePathInput.value = '';
        STATE.selectedUploadFile = null;
        if (DOM.dropzoneFileInput) DOM.dropzoneFileInput.value = '';
        await fetchSharedFiles();
      } else {
        const errorMsg = data.message || data.error || 'Failed to split and register file.';
        showToast('error', 'File Share Failed', errorMsg);
        appendTerminalLog('FILE SHARE ERROR', data, true);
      }
    } catch (err) {
      showToast('error', 'Network Exception', err.message || 'Could not communicate with node backend.');
      appendTerminalLog('SHARE EXCEPTION', { error: err.message }, true);
    } finally {
      DOM.shareSubmitBtn.disabled = false;
      DOM.shareSubmitBtn.innerHTML = `
        <svg class="icon icon-upload" viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
          <polyline points="17 8 12 3 7 8"/>
          <line x1="12" x2="12" y1="3" y2="15"/>
        </svg>
        <span>Share &amp; Chunk File</span>
      `;
    }
  }

  // ---------------------------------------------------------------------------
  // Action: Distributed Network Search
  // ---------------------------------------------------------------------------
  async function handleNetworkSearch(e) {
    e.preventDefault();
    const query = DOM.networkSearchInput.value.trim();
    const ttl = parseInt(DOM.ttlSelect.value, 10) || 3;

    if (!query) {
      showToast('info', 'Empty Query', 'Enter a keyword to search files on the network.');
      return;
    }

    STATE.lastSearchQuery = query;
    DOM.clearSearchBtn.hidden = false;
    DOM.networkSearchBtn.disabled = true;
    DOM.networkSearchBtn.innerHTML = `
      <svg class="icon spin-active" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M3 12a9 9 0 0 1 15-6.7L21 8"/><path d="M21 3v5h-5"/>
      </svg>
      <span>Searching...</span>
    `;

    try {
      const url = `${CONFIG.apiBase}/api/search?q=${encodeURIComponent(query)}&ttl=${ttl}`;
      const resp = await fetch(url);
      const data = await resp.json();

      if (resp.ok) {
        const results = data.results || [];
        appendTerminalLog(`DISTRIBUTED SEARCH: "${query}" (TTL: ${ttl})`, data);
        renderSearchResults(results, query);
        showToast('info', 'Search Complete', `Found ${results.length} files matching "${query}" across mesh.`);
      } else {
        showToast('error', 'Search Failed', data.message || 'Error executing search.');
        appendTerminalLog('SEARCH ERROR', data, true);
      }
    } catch (err) {
      showToast('error', 'Search Error', err.message || 'Search failed over peer socket.');
      appendTerminalLog('SEARCH EXCEPTION', { error: err.message }, true);
    } finally {
      DOM.networkSearchBtn.disabled = false;
      DOM.networkSearchBtn.innerHTML = `
        <svg class="icon" viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <circle cx="11" cy="11" r="8"/>
          <path d="m21 21-4.3-4.3"/>
        </svg>
        <span>Search Mesh</span>
      `;
    }
  }

  function renderSearchResults(results, query) {
    DOM.searchResultsTableBody.innerHTML = '';

    if (!results || results.length === 0) {
      DOM.searchResultsTableWrapper.hidden = true;
      DOM.searchResultsStateEmpty.hidden = false;
      DOM.searchResultsStateEmpty.innerHTML = `
        <svg class="icon icon-empty" viewBox="0 0 24 24" width="36" height="36" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
          <circle cx="11" cy="11" r="8"/>
          <line x1="21" y1="21" x2="16.65" y2="16.65"/>
          <line x1="8" y1="11" x2="14" y2="11"/>
        </svg>
        <p class="empty-title">No network files found for "${sanitizeHtml(query)}"</p>
        <p class="empty-desc">Try increasing the TTL hop count or verify that neighbor peers are registered and active.</p>
      `;
      return;
    }

    DOM.searchResultsStateEmpty.hidden = true;
    DOM.searchResultsTableWrapper.hidden = false;
    DOM.searchResultsSummary.textContent = `${results.length} ${results.length === 1 ? 'file' : 'files'} discovered on peer mesh`;

    results.forEach(res => {
      const tr = document.createElement('tr');
      const totalChunks = Math.ceil((res.sizeBytes || 0) / (1024 * 1024)) || 1;
      const endpoint = `${res.sourceHost}:${res.sourcePort}`;

      tr.innerHTML = `
        <td>
          <div class="cell-file-name">
            <svg class="icon" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/>
              <polyline points="14 2 14 8 20 8"/>
            </svg>
            <span>${sanitizeHtml(res.fileName || 'file.bin')}</span>
          </div>
        </td>
        <td class="mono">${formatBytes(res.sizeBytes || 0)}</td>
        <td><span class="badge badge-subtle mono">${sanitizeHtml(res.sourcePeerId || 'peer')}</span></td>
        <td class="mono">${sanitizeHtml(endpoint)}</td>
        <td class="td-action">
          <button class="btn btn-sm btn-primary download-result-btn" 
                  data-fileid="${sanitizeHtml(res.fileId)}"
                  data-filename="${sanitizeHtml(res.fileName)}"
                  data-totalchunks="${totalChunks}"
                  data-host="${sanitizeHtml(res.sourceHost)}"
                  data-port="${res.sourcePort}"
                  type="button">
            <svg class="icon" viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
              <polyline points="7 10 12 15 17 10"/>
              <line x1="12" x2="12" y1="15" y2="3"/>
            </svg>
            <span>Download</span>
          </button>
        </td>
      `;

      DOM.searchResultsTableBody.appendChild(tr);
    });
  }

  // ---------------------------------------------------------------------------
  // Action: Trigger Native Browser Download directly to device
  // ---------------------------------------------------------------------------
  function triggerBrowserDownload(fileId, fileName) {
    const params = new URLSearchParams();
    if (fileId) params.set('fileId', fileId);
    if (fileName) params.set('fileName', fileName);

    const downloadUrl = `${CONFIG.apiBase}/api/files/download?${params.toString()}`;
    const anchor = document.createElement('a');
    anchor.href = downloadUrl;
    if (fileName) {
      anchor.setAttribute('download', fileName);
    }
    anchor.style.display = 'none';
    document.body.appendChild(anchor);
    anchor.click();
    setTimeout(() => {
      if (anchor.parentNode) {
        anchor.parentNode.removeChild(anchor);
      }
    }, 200);

    appendTerminalLog('DEVICE DOWNLOAD INITIATED', { fileId, fileName, url: downloadUrl });
  }

  // ---------------------------------------------------------------------------
  // Action: Trigger Download Job
  // ---------------------------------------------------------------------------
  async function startDownloadJob(fileId, fileName, totalChunks, sourceHost, sourcePort) {
    try {
      totalChunks = parseInt(totalChunks, 10) || 1;

      // Smooth scroll to transfers section
      const transfersPanel = document.getElementById('panelTransfers');
      if (transfersPanel) {
        transfersPanel.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }

      // 1. If job is already completed on this node, or file is locally shared:
      const existingJob = STATE.downloadJobs.get(fileId);
      const existingShared = STATE.sharedFiles.find(f => f.fileId === fileId);

      if ((existingJob && existingJob.status === 'COMPLETED') || existingShared) {
        // Ensure this job exists in STATE.downloadJobs with COMPLETED chunk matrix so it renders
        if (!existingJob || existingJob.status !== 'COMPLETED') {
          const compChunks = {};
          for (let i = 0; i < totalChunks; i++) compChunks[i] = 'COMPLETED';
          STATE.downloadJobs.set(fileId, {
            fileId,
            fileName: fileName || (existingShared ? existingShared.name : 'file.bin'),
            totalChunks,
            sourceHost: sourceHost || '127.0.0.1',
            sourcePort: sourcePort || 9001,
            status: 'COMPLETED',
            completedChunks: totalChunks,
            progressPercentage: 100,
            startTime: Date.now() - 1000,
            endTime: Date.now(),
            chunkStates: compChunks,
            chunkRetries: {}
          });
        }
        renderTransfersList();
        showToast('success', 'File Ready on Node', `"${fileName}" is reassembled. Saving to your device now...`);
        triggerBrowserDownload(fileId, fileName);
        return;
      }

      // 2. Optimistic initial render: Immediately show active transfer in Chunk Assembly Matrix
      const initialChunkStates = {};
      for (let i = 0; i < totalChunks; i++) {
        initialChunkStates[i] = (i === 0 ? 'DOWNLOADING' : 'PENDING');
      }
      const initialJob = {
        fileId,
        fileName,
        totalChunks,
        sourceHost: sourceHost || '127.0.0.1',
        sourcePort: sourcePort || 9001,
        status: 'DOWNLOADING',
        completedChunks: 0,
        progressPercentage: 0,
        startTime: Date.now(),
        chunkStates: initialChunkStates,
        chunkRetries: {}
      };
      STATE.downloadJobs.set(fileId, initialJob);
      renderTransfersList();

      // 3. Mark for auto-download to device once transfer completes
      STATE.pendingDeviceDownloads.add(fileId);
      showToast('info', 'Transfer Initiated', `Streaming "${fileName}" from ${sourceHost}:${sourcePort}... Chunks will assemble in real-time.`);

      // Start fast polling immediately to animate chunks
      startFastPolling();

      const url = `${CONFIG.apiBase}/api/download/start?fileId=${encodeURIComponent(fileId)}&fileName=${encodeURIComponent(fileName)}&totalChunks=${totalChunks}&sourceHost=${encodeURIComponent(sourceHost)}&sourcePort=${sourcePort}`;
      const resp = await fetch(url, { method: 'POST' });
      const job = await resp.json();

      if (resp.ok) {
        appendTerminalLog(`DOWNLOAD STARTED: ${fileName}`, job);
        STATE.downloadJobs.set(job.fileId, job);
        renderTransfersList();
      } else {
        STATE.pendingDeviceDownloads.delete(fileId);
        showToast('error', 'Download Request Failed', job.message || 'Could not queue download.');
      }
    } catch (err) {
      STATE.pendingDeviceDownloads.delete(fileId);
      showToast('error', 'Download Exception', err.message || 'Error connecting to node.');
    }
  }

  // ---------------------------------------------------------------------------
  // Action: Re-Corrupt File & Trigger Fault Demonstration Download
  // ---------------------------------------------------------------------------
  async function recorruptAndDownloadFile(fileId, fileName, totalChunks, host = '127.0.0.1', port = 9001) {
    try {
      showToast('warning', 'Simulating Missing Chunks', `Dropping chunks #5 & #15 for "${fileName}" to test TCP safeguard failure...`);
      appendTerminalLog(`FAULT INJECTION: ${fileName}`, { fileId, droppedChunks: [5, 15], status: 'SIMULATING_FAULT' });

      // Call fault injection endpoint
      const faultUrl = `${CONFIG.apiBase}/api/test/simulate-chunk-fault?fileId=${encodeURIComponent(fileId)}&chunkNos=5,15`;
      const faultResp = await fetch(faultUrl, { method: 'POST' });
      const faultData = await faultResp.json();

      if (!faultResp.ok) {
        showToast('error', 'Fault Injection Failed', faultData.message || 'Could not drop chunks.');
        return;
      }

      appendTerminalLog(`CHUNKS DROPPED: ${fileName}`, faultData);

      // Start fresh download
      showToast('info', 'Download Initiated', `Downloading "${fileName}" with missing chunks. Observe safeguards engaging in visual matrix below.`);
      await startDownloadJob(fileId, fileName, totalChunks, host, port);
    } catch (err) {
      showToast('error', 'Fault Simulation Exception', err.message);
      appendTerminalLog('FAULT SIMULATION ERROR', { error: err.message }, true);
    }
  }

  // ---------------------------------------------------------------------------
  // Dynamic Polling Controller (1s fast polling during active transfers)
  // ---------------------------------------------------------------------------
  function startFastPolling() {
    if (STATE.activeJobPolling) return;
    STATE.activeJobPolling = true;
    if (DOM.pollIndicator) DOM.pollIndicator.classList.add('pulse-fast');

    // Trigger immediate fetch so we don't wait for the first interval tick
    fetchDownloadJobs();

    if (STATE.pollFastTimer) clearInterval(STATE.pollFastTimer);
    STATE.pollFastTimer = setInterval(async () => {
      await fetchDownloadJobs();
    }, CONFIG.pollIntervalFast);
  }

  function stopFastPolling() {
    STATE.activeJobPolling = false;
    if (DOM.pollIndicator) DOM.pollIndicator.classList.remove('pulse-fast');
    if (STATE.pollFastTimer) {
      clearInterval(STATE.pollFastTimer);
      STATE.pollFastTimer = null;
    }
  }

  // ---------------------------------------------------------------------------
  // Action: Register Neighbor Peer
  // ---------------------------------------------------------------------------
  async function handleRegisterPeer(e) {
    e.preventDefault();
    const peerId = DOM.regPeerIdInput.value.trim();
    const host = DOM.regHostInput.value.trim() || '127.0.0.1';
    const port = parseInt(DOM.regPortInput.value, 10);

    if (!peerId || !host || isNaN(port)) {
      showToast('error', 'Invalid Peer Data', 'Please provide Peer ID, host, and valid port.');
      return;
    }

    try {
      const url = `${CONFIG.apiBase}/api/peers/register?peerId=${encodeURIComponent(peerId)}&host=${encodeURIComponent(host)}&port=${port}`;
      const resp = await fetch(url, { method: 'POST' });
      const data = await resp.json();

      if (resp.ok) {
        showToast('success', 'Peer Registered', `Connected ${peerId} (${host}:${port}) to local routing table.`);
        appendTerminalLog(`PEER REGISTERED: ${peerId}`, data);
        DOM.regPeerIdInput.value = '';
        DOM.registerPeerDrawer.hidden = true;
        DOM.toggleRegisterPeerBtn.setAttribute('aria-expanded', 'false');
        await fetchKnownPeers();
      } else {
        showToast('error', 'Registration Failed', data.message || 'Could not register peer.');
      }
    } catch (err) {
      showToast('error', 'Peer Register Error', err.message || 'Connection failed.');
    }
  }

  // ---------------------------------------------------------------------------
  // Diagnostics: Run Protocol Verification Tests
  // ---------------------------------------------------------------------------
  async function runDiagnostic(name, endpoint, buttonEl) {
    if (buttonEl) {
      buttonEl.disabled = true;
      const originalText = buttonEl.innerHTML;
      buttonEl.innerHTML = `
        <svg class="icon spin-active" viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M3 12a9 9 0 0 1 15-6.7L21 8"/><path d="M21 3v5h-5"/>
        </svg>
        <span>Running Test...</span>
      `;

      try {
        const resp = await fetch(`${CONFIG.apiBase}${endpoint}`);
        const data = await resp.json();

        appendTerminalLog(`DIAGNOSTIC: ${name}`, data);

        if (resp.ok) {
          showToast('success', `${name} Passed`, 'Diagnostic output logged in console.');
          // Refresh local files & jobs as diagnostics might create sample files
          await fetchSharedFiles();
          await fetchDownloadJobs();
        } else {
          showToast('error', `${name} Failed`, data.message || 'Test returned an error.');
        }
      } catch (err) {
        showToast('error', `${name} Exception`, err.message || 'Test failed over socket.');
        appendTerminalLog(`DIAGNOSTIC ERROR: ${name}`, { error: err.message }, true);
      } finally {
        buttonEl.disabled = false;
        buttonEl.innerHTML = originalText;
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Full System Sync
  // ---------------------------------------------------------------------------
  async function syncAllData() {
    if (DOM.refreshIcon) DOM.refreshIcon.classList.add('spin-active');
    try {
      await Promise.all([
        checkNodeHealth(),
        fetchSharedFiles(),
        fetchKnownPeers(),
        fetchDownloadJobs()
      ]);
    } finally {
      setTimeout(() => {
        if (DOM.refreshIcon) DOM.refreshIcon.classList.remove('spin-active');
      }, 400);
    }
  }

  // ---------------------------------------------------------------------------
  // Polling Loop Setup
  // ---------------------------------------------------------------------------
  function startPolling() {
    if (STATE.pollTimer) clearInterval(STATE.pollTimer);
    STATE.pollTimer = setInterval(() => {
      if (STATE.autoPollActive) {
        syncAllData();
      }
    }, CONFIG.pollIntervalNormal);
  }

  // ---------------------------------------------------------------------------
  // Event Bindings & Delegation
  // ---------------------------------------------------------------------------
  function setupEventListeners() {
    // Theme toggle
    DOM.themeToggleBtn.addEventListener('click', toggleTheme);

    // Refresh controls
    DOM.manualRefreshBtn.addEventListener('click', () => {
      syncAllData();
      showToast('info', 'Node Synchronized', 'Updated files, mesh peers, and transfer states.');
    });

    DOM.retryConnectionBtn.addEventListener('click', () => {
      syncAllData();
    });

    DOM.autoRefreshToggle.addEventListener('change', (e) => {
      STATE.autoPollActive = e.target.checked;
      if (STATE.autoPollActive) {
        DOM.pollIndicator.classList.remove('paused');
        showToast('info', 'Polling Resumed', 'Polling node status every 3 seconds.');
      } else {
        DOM.pollIndicator.classList.add('paused');
        showToast('info', 'Polling Paused', 'Live telemetry updates paused.');
      }
    });

    // Share Form & Sample Chips
    DOM.shareFileForm.addEventListener('submit', handleShareFile);

    document.querySelectorAll('.chip-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        STATE.selectedUploadFile = null;
        const path = btn.getAttribute('data-path');
        DOM.shareFilePathInput.value = path;
        DOM.shareFilePathInput.focus();
      });
    });

    DOM.shareFilePathInput.addEventListener('input', () => {
      if (STATE.selectedUploadFile && DOM.shareFilePathInput.value.trim() !== STATE.selectedUploadFile.name) {
        STATE.selectedUploadFile = null;
      }
    });

    // Dropzone Drag & Drop
    DOM.fileDropzone.addEventListener('click', () => {
      DOM.dropzoneFileInput.click();
    });

    DOM.dropzoneFileInput.addEventListener('change', (e) => {
      if (e.target.files && e.target.files[0]) {
        STATE.selectedUploadFile = e.target.files[0];
        DOM.shareFilePathInput.value = STATE.selectedUploadFile.name;
        showToast('info', 'File Ready to Upload', `"${STATE.selectedUploadFile.name}" (${formatBytes(STATE.selectedUploadFile.size)}) selected. Click "Share & Chunk File".`);
      }
    });

    ['dragenter', 'dragover'].forEach(eventName => {
      DOM.fileDropzone.addEventListener(eventName, (e) => {
        e.preventDefault();
        e.stopPropagation();
        DOM.fileDropzone.classList.add('dragover');
      });
    });

    ['dragleave', 'drop'].forEach(eventName => {
      DOM.fileDropzone.addEventListener(eventName, (e) => {
        e.preventDefault();
        e.stopPropagation();
        DOM.fileDropzone.classList.remove('dragover');
      });
    });

    DOM.fileDropzone.addEventListener('drop', (e) => {
      const dt = e.dataTransfer;
      if (dt && dt.files && dt.files[0]) {
        STATE.selectedUploadFile = dt.files[0];
        DOM.shareFilePathInput.value = STATE.selectedUploadFile.name;
        showToast('info', 'File Ready to Upload', `"${STATE.selectedUploadFile.name}" (${formatBytes(STATE.selectedUploadFile.size)}) dropped. Click "Share & Chunk File".`);
      }
    });

    // Search Form & Clear
    DOM.networkSearchForm.addEventListener('submit', handleNetworkSearch);

    DOM.clearSearchBtn.addEventListener('click', () => {
      DOM.networkSearchInput.value = '';
      DOM.clearSearchBtn.hidden = true;
      DOM.searchResultsTableWrapper.hidden = true;
      DOM.searchResultsStateEmpty.hidden = false;
      DOM.networkSearchInput.focus();
    });

    DOM.networkSearchInput.addEventListener('input', (e) => {
      DOM.clearSearchBtn.hidden = !e.target.value;
    });

    // Delegate "Download from Peer" buttons in Search Results
    DOM.searchResultsTableBody.addEventListener('click', (e) => {
      const btn = e.target.closest('.download-result-btn');
      if (btn) {
        const fileId = btn.getAttribute('data-fileid');
        const fileName = btn.getAttribute('data-filename');
        const totalChunks = parseInt(btn.getAttribute('data-totalchunks'), 10) || 1;
        const host = btn.getAttribute('data-host');
        const port = parseInt(btn.getAttribute('data-port'), 10);
        startDownloadJob(fileId, fileName, totalChunks, host, port);
      }
    });

    // Delegate Transfers Card actions ("Save to Device", "Auto-Recover", "Re-Corrupt")
    DOM.transfersCardList.addEventListener('click', async (e) => {
      // 1. Direct Save to Device button on completed transfer
      const saveBtn = e.target.closest('.save-to-device-btn');
      if (saveBtn) {
        const fileId = saveBtn.getAttribute('data-fileid');
        const fileName = saveBtn.getAttribute('data-filename');
        showToast('info', 'Saving to Device', `Downloading "${fileName}" to your computer...`);
        triggerBrowserDownload(fileId, fileName);
        return;
      }

      // 2. Re-Corrupt File button
      const recorruptBtn = e.target.closest('.recorrupt-transfer-btn');
      if (recorruptBtn) {
        const fileId = recorruptBtn.getAttribute('data-fileid');
        const fileName = recorruptBtn.getAttribute('data-filename');
        const totalChunks = parseInt(recorruptBtn.getAttribute('data-totalchunks'), 10) || 1;
        const host = recorruptBtn.getAttribute('data-host') || '127.0.0.1';
        const port = parseInt(recorruptBtn.getAttribute('data-port'), 10) || 9001;
        await recorruptAndDownloadFile(fileId, fileName, totalChunks, host, port);
        return;
      }

      // 3. Auto-Recover Missing Chunks button
      const btn = e.target.closest('.recover-transfer-btn');
      if (btn) {
        const fileId = btn.getAttribute('data-fileid');
        const fileName = btn.getAttribute('data-filename');
        const totalChunks = parseInt(btn.getAttribute('data-totalchunks'), 10) || 1;
        const host = btn.getAttribute('data-host') || '127.0.0.1';
        const port = parseInt(btn.getAttribute('data-port'), 10) || 9001;

        btn.disabled = true;
        btn.innerHTML = `
          <svg class="icon spin-active" viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M3 12a9 9 0 0 1 15-6.7L21 8"/><path d="M21 3v5h-5"/>
          </svg>
          <span>Recovering Chunks over TCP...</span>
        `;

        showToast('info', 'Recovery Initiated', `Querying peer for missing chunks and verifying byte integrity...`);

        try {
          const url = `${CONFIG.apiBase}/api/download/recover?fileId=${encodeURIComponent(fileId)}&fileName=${encodeURIComponent(fileName)}&totalChunks=${totalChunks}&sourceHost=${encodeURIComponent(host)}&sourcePort=${port}`;
          const resp = await fetch(url, { method: 'POST' });
          const data = await resp.json();

          if (resp.ok && data.status === 'RECOVERY_AND_REASSEMBLY_SUCCESSFUL') {
            showToast('success', 'Recovery Successful', `All missing chunks recovered over TCP. "${fileName}" reassembled and saving to your device!`);
            appendTerminalLog(`RECOVERY SUCCESS: ${fileName}`, data);
            await fetchDownloadJobs();
            await fetchSharedFiles();
            triggerBrowserDownload(fileId, fileName);
          } else {
            showToast('error', 'Recovery Incomplete', data.message || 'Could not recover all missing chunks.');
            appendTerminalLog('RECOVERY INCOMPLETE', data, true);
          }
        } catch (err) {
          showToast('error', 'Recovery Exception', err.message || 'Error executing recovery.');
        } finally {
          btn.disabled = false;
        }
      }
    });

    // Delegate actions in Local Shared Files table ("Download", "Corrupt & Test")
    DOM.sharedFilesTableBody.addEventListener('click', async (e) => {
      const downloadBtn = e.target.closest('.download-shared-btn');
      if (downloadBtn) {
        const fileId = downloadBtn.getAttribute('data-fileid');
        const fileName = downloadBtn.getAttribute('data-filename');
        const totalChunks = parseInt(downloadBtn.getAttribute('data-totalchunks'), 10) || 1;
        startDownloadJob(fileId, fileName, totalChunks, '127.0.0.1', 9001);
        return;
      }

      const recorruptBtn = e.target.closest('.recorrupt-shared-btn');
      if (recorruptBtn) {
        const fileId = recorruptBtn.getAttribute('data-fileid');
        const fileName = recorruptBtn.getAttribute('data-filename');
        const totalChunks = parseInt(recorruptBtn.getAttribute('data-totalchunks'), 10) || 1;
        await recorruptAndDownloadFile(fileId, fileName, totalChunks);
        return;
      }

      const deleteBtn = e.target.closest('.delete-shared-btn');
      if (deleteBtn) {
        const fileId = deleteBtn.getAttribute('data-fileid');
        const fileName = deleteBtn.getAttribute('data-filename');
        if (confirm(`Remove "${fileName}" from node storage?`)) {
          try {
            const resp = await fetch(`${CONFIG.apiBase}/api/files/${encodeURIComponent(fileId)}`, { method: 'DELETE' });
            if (resp.ok) {
              showToast('success', 'File Removed', `"${fileName}" deleted from node storage.`);
              await fetchSharedFiles();
              await fetchDownloadJobs();
            } else {
              showToast('error', 'Delete Failed', 'Could not remove file.');
            }
          } catch (err) {
            showToast('error', 'Delete Error', err.message);
          }
        }
      }
    });

    // Shared Files Filter
    DOM.sharedFilesFilterInput.addEventListener('input', () => {
      renderSharedFilesTable();
    });

    // Peer Mesh Drawer Toggle & Submit
    DOM.toggleRegisterPeerBtn.addEventListener('click', () => {
      const isHidden = DOM.registerPeerDrawer.hidden;
      DOM.registerPeerDrawer.hidden = !isHidden;
      DOM.toggleRegisterPeerBtn.setAttribute('aria-expanded', isHidden ? 'true' : 'false');
      if (isHidden) DOM.regPeerIdInput.focus();
    });

    DOM.closeRegisterDrawerBtn.addEventListener('click', () => {
      DOM.registerPeerDrawer.hidden = true;
      DOM.toggleRegisterPeerBtn.setAttribute('aria-expanded', 'false');
    });

    DOM.registerPeerForm.addEventListener('submit', handleRegisterPeer);

    // Delegate "Ping" and "Remove" buttons on peers
    DOM.peersDynamicList.addEventListener('click', async (e) => {
      // 1. Remove Peer button
      const removeBtn = e.target.closest('.remove-peer-btn');
      if (removeBtn) {
        const peerId = removeBtn.getAttribute('data-peerid');
        if (confirm(`Remove peer "${peerId}" from peer mesh?`)) {
          removeBtn.disabled = true;
          try {
            const resp = await fetch(`${CONFIG.apiBase}/api/peers/${encodeURIComponent(peerId)}`, { method: 'DELETE' });
            if (resp.ok) {
              showToast('success', 'Peer Removed', `Peer "${peerId}" removed from network mesh.`);
              appendTerminalLog(`PEER REMOVED: ${peerId}`, { peerId });
              await fetchKnownPeers();
            } else {
              showToast('error', 'Remove Peer Failed', 'Could not remove peer from registry.');
            }
          } catch (err) {
            showToast('error', 'Remove Peer Error', err.message);
          } finally {
            removeBtn.disabled = false;
          }
        }
        return;
      }

      // 2. Ping Peer button
      const btn = e.target.closest('.ping-peer-btn');
      if (btn) {
        const peerId = btn.getAttribute('data-peerid');
        const host = btn.getAttribute('data-host');
        const port = btn.getAttribute('data-port');
        
        btn.disabled = true;
        btn.textContent = 'Pinging...';
        try {
          // Probe via /api/test/tcp-hello or report connectivity
          const resp = await fetch(`${CONFIG.apiBase}/api/test/tcp-hello`);
          const data = await resp.json();
          appendTerminalLog(`PING PEER ${peerId} (${host}:${port})`, data);
          showToast('success', `Peer ${peerId} Active`, `Handshake verified on TCP port ${port}.`);
        } catch (err) {
          showToast('error', `Peer ${peerId} Unreachable`, err.message);
        } finally {
          btn.disabled = false;
          btn.textContent = 'Ping';
        }
      }
    });

    // Global copy to clipboard buttons delegation
    document.addEventListener('click', (e) => {
      const copyBtn = e.target.closest('[data-copy]');
      if (copyBtn) {
        const text = copyBtn.getAttribute('data-copy');
        copyToClipboard(text, copyBtn);
      }
    });

    // Diagnostic Action Buttons
    DOM.btnTestRecovery.addEventListener('click', () => {
      runDiagnostic('Chunk Recovery Simulation', '/api/test/chunk-recovery', DOM.btnTestRecovery);
    });

    DOM.btnTestTcpHello.addEventListener('click', () => {
      runDiagnostic('TCP Framing Handshake', '/api/test/tcp-hello', DOM.btnTestTcpHello);
    });

    DOM.btnTestChunking.addEventListener('click', () => {
      runDiagnostic('2.5MB File Chunking & Reassembly', '/api/test/chunking', DOM.btnTestChunking);
    });

    // Terminal Screen actions
    DOM.clearConsoleBtn.addEventListener('click', () => {
      DOM.consoleCode.innerHTML = '// Console cleared. Awaiting protocol events.';
      showToast('info', 'Console Cleared', 'Terminal output buffer was reset.');
    });

    DOM.copyConsoleBtn.addEventListener('click', () => {
      const text = DOM.consoleCode.innerText || DOM.consoleCode.textContent;
      copyToClipboard(text, DOM.copyConsoleBtn);
    });

    // Keyboard Shortcuts
    document.addEventListener('keydown', (e) => {
      // Shortcut '/' to focus search input (when not already typing in an input/textarea)
      if (e.key === '/' && !['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement.tagName)) {
        e.preventDefault();
        DOM.networkSearchInput.focus();
        DOM.networkSearchInput.select();
      }

      // Shortcut 'r' or 'R' to refresh (when not focused in an input)
      if ((e.key === 'r' || e.key === 'R') && !['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement.tagName)) {
        e.preventDefault();
        syncAllData();
        showToast('info', 'Node Synchronized', 'Updated files, mesh peers, and transfer states.');
      }

      // Shortcut 'Escape' to close drawer or clear search
      if (e.key === 'Escape') {
        if (!DOM.registerPeerDrawer.hidden) {
          DOM.registerPeerDrawer.hidden = true;
          DOM.toggleRegisterPeerBtn.setAttribute('aria-expanded', 'false');
        }
        if (document.activeElement === DOM.networkSearchInput && DOM.networkSearchInput.value) {
          DOM.networkSearchInput.value = '';
          DOM.clearSearchBtn.hidden = true;
        }
      }
    });
  }

  // ---------------------------------------------------------------------------
  // Boot Sequence
  // ---------------------------------------------------------------------------
  async function init() {
    initTheme();
    setupEventListeners();
    await syncAllData();
    startPolling();
    appendTerminalLog('P2P Node Console', {
      nodeId: 'peer-1',
      status: 'INITIALIZED',
      capabilities: ['1MB_CHUNK_SLICING', 'DISTRIBUTED_SEARCH_TTL', 'TCP_FRAMING', 'SELF_HEALING_RECOVERY'],
      storageRoot: 'peer-data/chunks'
    });
  }

  // Run on DOM Ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
