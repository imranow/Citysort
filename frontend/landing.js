/* CitySort AI — Landing Page JS */
(function () {
  "use strict";

  const AUTH_TOKEN_KEY = "citysort_access_token";
  const CONFIG = window.__CITYSORT_CONFIG__ || {};

  /* --- DOM refs --- */
  const loginOverlay = document.getElementById("login-modal");
  const signupOverlay = document.getElementById("signup-modal");
  const loginForm = document.getElementById("login-form");
  const signupForm = document.getElementById("signup-form");
  const loginError = document.getElementById("login-error");
  const signupError = document.getElementById("signup-error");
  const signupTokenInput = document.getElementById("signup-token");
  const toggleMonthly = document.getElementById("toggle-monthly");
  const toggleLifetime = document.getElementById("toggle-lifetime");
  const urlParams = new URLSearchParams(window.location.search);

  if (urlParams.get("logout") === "1") {
    localStorage.removeItem(AUTH_TOKEN_KEY);
    const cleanParams = new URLSearchParams(urlParams);
    cleanParams.delete("logout");
    const cleanedSearch = cleanParams.toString();
    const cleanedUrl =
      window.location.pathname
      + (cleanedSearch ? "?" + cleanedSearch : "")
      + window.location.hash;
    window.history.replaceState({}, "", cleanedUrl);
  }

  function decodeTokenPayload(token) {
    const rawPayload = String(token || "").split(".")[0] || "";
    const normalized = rawPayload.replace(/-/g, "+").replace(/_/g, "/");
    const padding = "=".repeat((4 - (normalized.length % 4)) % 4);
    return JSON.parse(atob(normalized + padding));
  }

  /* --- Redirect if already logged in --- */
  const existingToken = localStorage.getItem(AUTH_TOKEN_KEY);
  if (existingToken) {
    try {
      // CitySort token format is payload.signature (payload is base64url segment 0)
      const payload = decodeTokenPayload(existingToken);
      if (payload.exp && payload.exp > Date.now() / 1000) {
        window.location.href = "/app";
        return;
      }
    } catch (_) {
      localStorage.removeItem(AUTH_TOKEN_KEY);
    }
  }

  /* --- Pre-populate invite token --- */
  if (CONFIG.invite_token && signupTokenInput) {
    signupTokenInput.value = CONFIG.invite_token;
    openModal("signup");
  }
  if (urlParams.get("login") === "1") {
    openModal("login");
  } else if (urlParams.get("signup") === "1") {
    openModal("signup");
  }

  /* --- Modal controls --- */
  function openModal(type) {
    if (type === "login" && loginOverlay) {
      loginOverlay.classList.add("active");
      loginOverlay.querySelector("input")?.focus();
    } else if (type === "signup" && signupOverlay) {
      signupOverlay.classList.add("active");
      signupOverlay.querySelector("input")?.focus();
    }
  }

  function closeModal(type) {
    if (type === "login" && loginOverlay) loginOverlay.classList.remove("active");
    if (type === "signup" && signupOverlay) signupOverlay.classList.remove("active");
  }

  // Close on overlay click
  [loginOverlay, signupOverlay].forEach(function (overlay) {
    if (!overlay) return;
    overlay.addEventListener("click", function (e) {
      if (e.target === overlay) overlay.classList.remove("active");
    });
  });

  // Close buttons
  document.querySelectorAll(".modal-close").forEach(function (btn) {
    btn.addEventListener("click", function () {
      loginOverlay?.classList.remove("active");
      signupOverlay?.classList.remove("active");
    });
  });

  // Nav buttons
  document.querySelectorAll("[data-action='open-login']").forEach(function (btn) {
    btn.addEventListener("click", function (e) {
      e.preventDefault();
      openModal("login");
    });
  });
  document.querySelectorAll("[data-action='open-signup']").forEach(function (btn) {
    btn.addEventListener("click", function (e) {
      e.preventDefault();
      openModal("signup");
    });
  });

  // Switch between login/signup
  document.querySelectorAll("[data-action='switch-to-signup']").forEach(function (a) {
    a.addEventListener("click", function (e) {
      e.preventDefault();
      closeModal("login");
      openModal("signup");
    });
  });
  document.querySelectorAll("[data-action='switch-to-login']").forEach(function (a) {
    a.addEventListener("click", function (e) {
      e.preventDefault();
      closeModal("signup");
      openModal("login");
    });
  });

  /* --- Login --- */
  if (loginForm) {
    loginForm.addEventListener("submit", async function (e) {
      e.preventDefault();
      loginError.classList.remove("visible");
      loginError.textContent = "";
      const email = loginForm.querySelector("[name='email']").value.trim();
      const password = loginForm.querySelector("[name='password']").value;
      if (!email || !password) { showError(loginError, "Email and password are required."); return; }

      const btn = loginForm.querySelector("button[type='submit']");
      btn.disabled = true;
      btn.textContent = "Signing in...";

      try {
        const res = await fetch("/api/auth/login", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email, password }),
        });
        const data = await res.json();
        if (!res.ok) { showError(loginError, data.detail || "Login failed."); return; }
        localStorage.setItem(AUTH_TOKEN_KEY, data.access_token);
        window.location.href = "/app";
      } catch (err) {
        showError(loginError, "Network error. Please try again.");
      } finally {
        btn.disabled = false;
        btn.textContent = "Sign In";
      }
    });
  }

  /* --- Signup --- */
  if (signupForm) {
    signupForm.addEventListener("submit", async function (e) {
      e.preventDefault();
      signupError.classList.remove("visible");
      signupError.textContent = "";
      const email = signupForm.querySelector("[name='email']").value.trim();
      const password = signupForm.querySelector("[name='password']").value;
      const fullName = signupForm.querySelector("[name='full_name']").value.trim();
      const token = signupForm.querySelector("[name='invitation_token']").value.trim();
      if (!email || !password || !token) {
        showError(signupError, "All fields are required, including the invitation token.");
        return;
      }
      if (password.length < 8) { showError(signupError, "Password must be at least 8 characters."); return; }

      const btn = signupForm.querySelector("button[type='submit']");
      btn.disabled = true;
      btn.textContent = "Creating account...";

      try {
        const res = await fetch("/api/auth/signup", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email, password, full_name: fullName, invitation_token: token }),
        });
        const data = await res.json();
        if (!res.ok) { showError(signupError, data.detail || "Signup failed."); return; }
        localStorage.setItem(AUTH_TOKEN_KEY, data.access_token);
        window.location.href = "/app";
      } catch (err) {
        showError(signupError, "Network error. Please try again.");
      } finally {
        btn.disabled = false;
        btn.textContent = "Create Account";
      }
    });
  }

  /* --- Pricing toggle --- */
  let billingMode = "monthly";

  function updatePricing() {
    document.querySelectorAll("[data-monthly-price]").forEach(function (el) {
      if (billingMode === "monthly") {
        el.textContent = el.getAttribute("data-monthly-price");
      } else {
        el.textContent = el.getAttribute("data-lifetime-price");
      }
    });
    document.querySelectorAll("[data-monthly-period]").forEach(function (el) {
      el.textContent = billingMode === "monthly"
        ? el.getAttribute("data-monthly-period")
        : el.getAttribute("data-lifetime-period");
    });
    if (toggleMonthly) toggleMonthly.classList.toggle("active", billingMode === "monthly");
    if (toggleLifetime) toggleLifetime.classList.toggle("active", billingMode === "lifetime");
  }

  if (toggleMonthly) {
    toggleMonthly.addEventListener("click", function () { billingMode = "monthly"; updatePricing(); });
  }
  if (toggleLifetime) {
    toggleLifetime.addEventListener("click", function () { billingMode = "lifetime"; updatePricing(); });
  }
  updatePricing();

  /* --- Pricing CTA clicks --- */
  document.querySelectorAll("[data-plan]").forEach(function (btn) {
    btn.addEventListener("click", async function () {
      const plan = btn.getAttribute("data-plan");
      if (plan === "free") {
        openModal("signup");
        return;
      }

      // Check if user is logged in
      const token = localStorage.getItem(AUTH_TOKEN_KEY);
      if (!token) {
        openModal("login");
        return;
      }

      // Redirect to Stripe Checkout
      btn.disabled = true;
      btn.textContent = "Redirecting...";
      try {
        const res = await fetch("/api/billing/checkout", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + token,
          },
          body: JSON.stringify({ plan_tier: plan, billing_type: billingMode }),
        });
        const data = await res.json();
        if (!res.ok) {
          alert(data.detail || "Failed to start checkout.");
          return;
        }
        window.location.href = data.checkout_url;
      } catch (err) {
        alert("Network error. Please try again.");
      } finally {
        btn.disabled = false;
        btn.textContent = plan === "pro" ? "Upgrade to Pro" : "Contact Sales";
      }
    });
  });

  /* --- Smooth scroll for nav links --- */
  document.querySelectorAll('a[href^="#"]').forEach(function (link) {
    link.addEventListener("click", function (e) {
      e.preventDefault();
      const target = document.querySelector(link.getAttribute("href"));
      if (target) target.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  });

  /* --- Theme toggle --- */
  const themeBtn = document.getElementById("theme-toggle");
  if (themeBtn) {
    themeBtn.addEventListener("click", function () {
      const current = document.documentElement.getAttribute("data-theme");
      const next = current === "dark" ? "light" : "dark";
      document.documentElement.setAttribute("data-theme", next);
      localStorage.setItem("citysort_theme", next);
      themeBtn.textContent = next === "dark" ? "\u2600\uFE0F" : "\uD83C\uDF19";
    });
  }

  /* --- Helper --- */
  function showError(el, msg) {
    el.textContent = msg;
    el.classList.add("visible");
  }
})();
