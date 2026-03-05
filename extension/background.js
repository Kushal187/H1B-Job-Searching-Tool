chrome.runtime.onInstalled.addListener(async () => {
  try {
    await chrome.sidePanel.setPanelBehavior({ openPanelOnActionClick: true });
  } catch (_) {
    // Older Chrome versions may not support this API.
  }
});

async function openFallbackTab() {
  const url = chrome.runtime.getURL('sidepanel.html');
  await chrome.tabs.create({ url });
}

chrome.action.onClicked.addListener(async (tab) => {
  if (!tab || typeof tab.id !== 'number') {
    await openFallbackTab();
    return;
  }
  try {
    await chrome.sidePanel.open({ tabId: tab.id });
  } catch (_) {
    await openFallbackTab();
  }
});

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (message?.type === 'FETCH_JSON') {
    fetch(message.url)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data) => sendResponse({ ok: true, data }))
      .catch((err) => sendResponse({ ok: false, error: String(err) }));
    return true;
  }
});
