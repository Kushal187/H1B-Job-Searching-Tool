(function () {
  const THEME_KEY = 'theme';

  function preferredTheme() {
    return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches
      ? 'dark'
      : 'light';
  }

  function applyTheme(theme) {
    const dark = theme === 'dark';
    document.documentElement.classList.toggle('dark', dark);

    const button = document.getElementById('theme-toggle');
    if (button) {
      button.setAttribute('aria-pressed', String(dark));
      button.setAttribute('title', dark ? 'Switch to light mode' : 'Switch to dark mode');
    }
  }

  function initThemeToggle() {
    let savedTheme = null;
    try {
      savedTheme = localStorage.getItem(THEME_KEY);
    } catch (_) {}

    const initialTheme = savedTheme === 'dark' || savedTheme === 'light'
      ? savedTheme
      : preferredTheme();
    applyTheme(initialTheme);

    const button = document.getElementById('theme-toggle');
    if (!button) return;

    button.addEventListener('click', () => {
      const nextTheme = document.documentElement.classList.contains('dark') ? 'light' : 'dark';
      try {
        localStorage.setItem(THEME_KEY, nextTheme);
      } catch (_) {}
      applyTheme(nextTheme);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initThemeToggle);
  } else {
    initThemeToggle();
  }
})();
