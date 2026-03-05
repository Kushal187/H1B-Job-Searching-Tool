const els = {
  apiBase: document.getElementById('apiBase'),
  profileId: document.getElementById('profileId'),
  captureBtn: document.getElementById('captureBtn'),
  generateBtn: document.getElementById('generateBtn'),
  downloadBtn: document.getElementById('downloadBtn'),
  jdText: document.getElementById('jdText'),
  warnings: document.getElementById('warnings'),
  status: document.getElementById('status'),
  meta: document.getElementById('meta'),
};

let captured = null;
let generated = null;

function setStatus(text) {
  els.status.textContent = text;
}

function renderWarnings(list) {
  const deduped = [...new Set((list || []).map((x) => String(x).trim()).filter(Boolean))];
  els.warnings.innerHTML = deduped.map((w) => `<div>• ${w}</div>`).join('');
}

async function getActiveTab() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  return tab;
}

async function ensureContentScript(tabId) {
  try {
    await chrome.tabs.sendMessage(tabId, { type: 'PING' });
  } catch {
    await chrome.scripting.executeScript({
      target: { tabId },
      files: ['content.js'],
    });
  }
}

async function captureFromPage() {
  const tab = await getActiveTab();
  if (!tab || typeof tab.id !== 'number') {
    throw new Error('No active tab found.');
  }

  await ensureContentScript(tab.id);

  const response = await chrome.tabs.sendMessage(tab.id, { type: 'CAPTURE_JD' });
  if (!response?.ok) {
    throw new Error(response?.error || 'JD extraction failed.');
  }

  captured = response.payload;
  els.jdText.value = captured.jd_text || '';
  els.meta.textContent = JSON.stringify(
    {
      url: captured.jd_url,
      title: captured.page_title,
      role: captured.role_title,
      company: captured.company_hint,
      extracted_at: captured.extracted_at,
    },
    null,
    2
  );
  renderWarnings(captured.warnings || []);
}

async function loadSettings() {
  const stored = await chrome.storage.local.get(['apiBase', 'profileId']);
  els.apiBase.value = stored.apiBase || 'http://localhost:8000';
  els.profileId.value = stored.profileId || '1';
}

async function saveSettings() {
  await chrome.storage.local.set({
    apiBase: els.apiBase.value.trim(),
    profileId: els.profileId.value.trim(),
  });
}

async function generateResume() {
  await saveSettings();

  const apiBase = els.apiBase.value.trim().replace(/\/$/, '');
  const profileId = Number(els.profileId.value || '0');
  const jdText = els.jdText.value.trim();

  if (!apiBase) throw new Error('API base URL is required.');
  if (!profileId) throw new Error('Profile ID is required.');
  if (jdText.length < 20) throw new Error('JD text is too short.');

  const payload = {
    jd_text: jdText,
    jd_url: captured?.jd_url || '',
    page_title: captured?.page_title || '',
    profile_id: profileId,
    target_role: captured?.role_title || '',
    strictness: 'balanced',
    return_pdf_base64: true,
  };

  const resp = await fetch(`${apiBase}/api/resume/generate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (!resp.ok || data.error) {
    throw new Error(data.error || 'Generation failed');
  }

  generated = data.result;
  renderWarnings([...(generated.warnings || []), ...(generated.validation?.warnings || [])]);
  els.meta.textContent = JSON.stringify(
    {
      trace_id: generated.trace_id,
      keyword_coverage: generated.keyword_coverage,
      missing_keywords: generated.missing_keywords,
      unsupported_claims: generated.validation?.unsupported_claims || [],
      duration_ms: generated.duration_ms,
      model_route: generated.model_route,
    },
    null,
    2
  );
  els.downloadBtn.disabled = !generated.pdf_base64;
}

function downloadPdf() {
  if (!generated?.pdf_base64) return;
  const byteChars = atob(generated.pdf_base64);
  const arr = new Uint8Array(byteChars.length);
  for (let i = 0; i < byteChars.length; i += 1) {
    arr[i] = byteChars.charCodeAt(i);
  }
  const blob = new Blob([arr], { type: 'application/pdf' });
  const url = URL.createObjectURL(blob);
  chrome.downloads.download({
    url,
    filename: 'Kushal_Pendekanti_Resume_SWE.pdf',
    saveAs: true,
  });
}

els.captureBtn.addEventListener('click', async () => {
  setStatus('Capturing JD...');
  try {
    await captureFromPage();
    setStatus('Captured');
  } catch (e) {
    renderWarnings([String(e)]);
    setStatus('Capture failed');
  }
});

els.generateBtn.addEventListener('click', async () => {
  setStatus('Generating...');
  els.generateBtn.disabled = true;
  try {
    await generateResume();
    setStatus('Generated');
  } catch (e) {
    renderWarnings([String(e)]);
    setStatus('Generation failed');
  } finally {
    els.generateBtn.disabled = false;
  }
});

els.downloadBtn.addEventListener('click', downloadPdf);

(async () => {
  await loadSettings();
})();
