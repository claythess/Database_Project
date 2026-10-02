(() => {
  const key = 'movie-app-theme';
  const allowed = ['system', 'light', 'dark'];
  const readPreference = () => {
    try {
      const saved = localStorage.getItem(key);
      return allowed.includes(saved) ? saved : 'system';
    } catch (_) {
      return 'system';
    }
  };
  let preference = readPreference();

  const apply = () => {
    if (preference === 'system') {
      document.documentElement.removeAttribute('data-theme');
    } else {
      document.documentElement.setAttribute('data-theme', preference);
    }
  };

  const init = () => {
    apply();
    const toggle = document.createElement('button');
    toggle.type = 'button';
    toggle.className = 'theme-toggle';
    toggle.setAttribute('aria-label', 'Change color theme');
    const updateLabel = () => {
      const mode = preference[0].toUpperCase() + preference.slice(1);
      toggle.textContent = `Theme: ${mode}`;
      toggle.title = `Color theme: ${mode}. Click to change.`;
    };
    updateLabel();
    toggle.addEventListener('click', () => {
      preference = allowed[(allowed.indexOf(preference) + 1) % allowed.length];
      try {
        if (preference === 'system') localStorage.removeItem(key);
        else localStorage.setItem(key, preference);
      } catch (_) {
        // The current tab can still use the selected mode if storage is unavailable.
      }
      apply();
      updateLabel();
    });
    document.body.appendChild(toggle);
  };

  apply();
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
