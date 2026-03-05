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

function captureJobPage() {
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

  const mainContainer =
    document.querySelector('main') ||
    document.querySelector('article') ||
    document.querySelector('[role="main"]') ||
    document.body;

  const jdText = cleanText(mainContainer?.innerText || '');
  const warnings = [];
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

  try {
    const payload = captureJobPage();
    sendResponse({ ok: true, payload });
  } catch (error) {
    sendResponse({ ok: false, error: String(error) });
  }

  return true;
});
