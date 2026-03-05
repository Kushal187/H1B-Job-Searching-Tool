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
