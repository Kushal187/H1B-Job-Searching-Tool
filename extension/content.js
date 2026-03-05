function cleanText(text) {
  return (text || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .replace(/[ \t]{2,}/g, ' ')
    .trim();
}

function elementText(selector) {
  const el = document.querySelector(selector);
  return el ? cleanText(el.innerText || el.textContent || '') : '';
}

const END_MARKERS = [
  'apply for this job',
  'submit application',
  'submit your application',
  'create a job alert',
  'voluntary self-identification',
  'equal employment opportunity',
  'form cc-305',
  'omb control number',
  'public burden statement',
  'privacy notice',
  'our privacy practices',
  'autofill with',
  'first name*',
  'resume/cv*',
  'attach\nattach',
  'accepted file types',
  'indicates a required field',
];

const CONTAINER_SELECTORS = [
  'main',
  'article',
  '[role="main"]',
  '#content',
  '#app_body',
  '.content-wrapper',
  '.posting',
  '[data-testid="job-posting"]',
  '[data-automation-id="jobPostingDescription"]',
  '.job-description',
  '.job-post',
  '.job-details',
  '#job-content',
];

function trimAfterEndMarker(text) {
  const lower = text.toLowerCase();
  let cutIdx = text.length;
  for (const marker of END_MARKERS) {
    const idx = lower.indexOf(marker);
    if (idx !== -1 && idx < cutIdx) {
      cutIdx = idx;
    }
  }
  return text.slice(0, cutIdx).trim();
}

function findMainContainer() {
  for (const sel of CONTAINER_SELECTORS) {
    const el = document.querySelector(sel);
    if (el) return el;
  }
  return document.body;
}

function findGreenhouseBoardTokens() {
  const tokens = new Set();

  const iframes = document.querySelectorAll('iframe[src]');
  for (const iframe of iframes) {
    const src = iframe.src || '';
    const boardMatch = src.match(/greenhouse\.io\/(\w+)/);
    if (boardMatch && boardMatch[1] !== 'embed') tokens.add(boardMatch[1]);
    const forMatch = src.match(/for=(\w+)/);
    if (forMatch && src.includes('greenhouse.io')) tokens.add(forMatch[1]);
  }

  const html = document.documentElement.innerHTML;
  const ghUrlPattern = /greenhouse\.io\/(?:embed\/job_board\?for=|embed\/job_app\?.*?for=)(\w+)/g;
  let m;
  while ((m = ghUrlPattern.exec(html)) !== null) tokens.add(m[1]);

  const boardPathPattern = /(?:boards|job-boards)\.greenhouse\.io\/(\w+)/g;
  while ((m = boardPathPattern.exec(html)) !== null) {
    if (m[1] !== 'embed') tokens.add(m[1]);
  }

  const scriptSrcs = document.querySelectorAll('script[src*="greenhouse"]');
  for (const script of scriptSrcs) {
    const sm = (script.src || '').match(/for=(\w+)/);
    if (sm) tokens.add(sm[1]);
  }

  return [...tokens];
}

function detectGreenhouseJobId() {
  const url = window.location.href;

  const directMatch = url.match(/boards\.greenhouse\.io\/(\w+)\/jobs\/(\d+)/);
  if (directMatch) return { boards: [directMatch[1]], jobId: directMatch[2] };

  const ghParam = new URLSearchParams(window.location.search).get('gh_jid');
  const jobId = ghParam || url.match(/gh_jid=(\d+)/)?.[1];

  if (jobId) {
    const tokens = findGreenhouseBoardTokens();
    if (tokens.length > 0) return { boards: tokens, jobId };

    const host = window.location.hostname.replace(/^www\./, '');
    const boardGuess = host.split('.')[0].replace(/[^a-z0-9]/gi, '');
    if (boardGuess) return { boards: [boardGuess], jobId };
  }

  const iframes = document.querySelectorAll('iframe[src]');
  for (const iframe of iframes) {
    const src = iframe.src || '';
    const im = src.match(/greenhouse\.io\/(\w+)\/jobs\/(\d+)/);
    if (im) return { boards: [im[1]], jobId: im[2] };
    const embedMatch = src.match(/greenhouse\.io\/embed\/job_app\?.*?for=(\w+).*?token=(\d+)/);
    if (embedMatch) return { boards: [embedMatch[1]], jobId: embedMatch[2] };
  }

  return null;
}

function detectLeverCompanyAndId() {
  const url = window.location.href;
  const directMatch = url.match(/jobs\.lever\.co\/([^/]+)\/([0-9a-f-]{36})/);
  if (directMatch) return { company: directMatch[1], postingId: directMatch[2] };

  const iframes = document.querySelectorAll('iframe[src]');
  for (const iframe of iframes) {
    const src = iframe.src || '';
    const m = src.match(/jobs\.lever\.co\/([^/]+)\/([0-9a-f-]{36})/);
    if (m) return { company: m[1], postingId: m[2] };
  }

  return null;
}

function htmlToPlainText(html) {
  const doc = new DOMParser().parseFromString(html, 'text/html');
  const body = doc.body;

  const blocks = new Set([
    'P','DIV','H1','H2','H3','H4','H5','H6',
    'LI','TR','BR','HR','BLOCKQUOTE','SECTION','ARTICLE','HEADER','FOOTER',
  ]);

  function walk(node) {
    if (node.nodeType === Node.TEXT_NODE) return node.textContent;
    if (node.nodeType !== Node.ELEMENT_NODE) return '';
    const parts = [];
    for (const child of node.childNodes) parts.push(walk(child));
    let inner = parts.join('');
    if (blocks.has(node.tagName)) inner = '\n' + inner + '\n';
    return inner;
  }

  let text = walk(body);
  text = text.replace(/<[^>]*>/g, ' ');
  return cleanText(text);
}

function bgFetch(url) {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage({ type: 'FETCH_JSON', url }, (resp) => {
      if (chrome.runtime.lastError || !resp?.ok) resolve(null);
      else resolve(resp.data);
    });
  });
}

async function fetchGreenhouseJD(boards, jobId) {
  for (const board of boards) {
    const data = await bgFetch(
      `https://boards-api.greenhouse.io/v1/boards/${board}/jobs/${jobId}?content=true`
    );
    if (!data) continue;
    const parts = [data.title || ''];
    if (data.location?.name) parts.push(data.location.name);
    if (data.content) parts.push(htmlToPlainText(data.content));
    return { text: parts.join('\n\n'), title: data.title || '' };
  }
  return null;
}

async function fetchLeverJD(company, postingId) {
  const data = await bgFetch(
    `https://api.lever.co/v0/postings/${company}/${postingId}`
  );
  if (!data) return null;
  const parts = [data.text || ''];
  if (data.categories?.location) parts.push(data.categories.location);
  if (data.descriptionPlain) parts.push(data.descriptionPlain);
  if (data.lists) {
    for (const list of data.lists) {
      if (list.text) parts.push(list.text);
      if (list.content) parts.push(htmlToPlainText(list.content));
    }
  }
  if (data.additional) parts.push(htmlToPlainText(data.additional));
  return { text: parts.join('\n\n'), title: data.text || '' };
}

function tryIframeContent() {
  const iframes = document.querySelectorAll('iframe');
  for (const iframe of iframes) {
    try {
      const doc = iframe.contentDocument || iframe.contentWindow?.document;
      if (!doc) continue;
      const text = cleanText(doc.body?.innerText || '');
      if (text.length > 400) return text;
    } catch {
      // Cross-origin — can't access, skip
    }
  }
  return null;
}

async function captureJobPage() {
  const title = cleanText(document.title || '');
  const roleTitle =
    elementText('h1') ||
    elementText('[data-testid*="title"]') ||
    elementText('h2') ||
    title;

  const companyHint =
    elementText('[data-testid*="company"]') ||
    elementText('[class*="company"]') ||
    cleanText(
      document.querySelector('meta[property="og:site_name"]')?.getAttribute('content') ||
      ''
    );

  const mainContainer = findMainContainer();
  const rawText = cleanText(mainContainer?.innerText || '');

  const trimmed = trimAfterEndMarker(rawText);
  let jdText = trimmed.length >= 200 ? trimmed : rawText;

  const warnings = [];
  if (mainContainer === document.body) {
    warnings.push('Used document.body fallback — extraction may include extra content.');
  }

  if (jdText.length < 400) {
    const gh = detectGreenhouseJobId();
    if (gh) {
      try {
        const result = await fetchGreenhouseJD(gh.boards, gh.jobId);
        if (result && result.text.length > jdText.length) {
          jdText = result.text;
          warnings.push('Fetched JD from Greenhouse API (embedded iframe detected).');
        }
      } catch { /* API call failed, continue with what we have */ }
    }

    if (jdText.length < 400) {
      const lever = detectLeverCompanyAndId();
      if (lever) {
        try {
          const result = await fetchLeverJD(lever.company, lever.postingId);
          if (result && result.text.length > jdText.length) {
            jdText = result.text;
            warnings.push('Fetched JD from Lever API (embedded iframe detected).');
          }
        } catch { /* API call failed, continue with what we have */ }
      }
    }

    if (jdText.length < 400) {
      const iframeText = tryIframeContent();
      if (iframeText && iframeText.length > jdText.length) {
        const iframeTrimmed = trimAfterEndMarker(iframeText);
        jdText = iframeTrimmed.length >= 200 ? iframeTrimmed : iframeText;
        warnings.push('Extracted JD from same-origin iframe.');
      }
    }
  }

  if (jdText.length < 400) {
    warnings.push('Low-confidence extraction: detected less than 400 characters.');
  }

  return {
    jd_text: jdText,
    jd_url: window.location.href,
    page_title: title,
    role_title: roleTitle,
    company_hint: companyHint,
    extracted_at: new Date().toISOString(),
    warnings,
  };
}

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (!message || message.type !== 'CAPTURE_JD') return;

  captureJobPage()
    .then((payload) => sendResponse({ ok: true, payload }))
    .catch((error) => sendResponse({ ok: false, error: String(error) }));

  return true;
});
