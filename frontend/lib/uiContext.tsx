"use client";

/* eslint-disable react-hooks/set-state-in-effect */

import React, { createContext, useCallback, useContext, useMemo, useState } from "react";

type UiContextValue = {
  isSyncOpen: boolean;
  openSync: () => void;
  closeSync: () => void;
  powerbiAccessToken: string | null;
  setPowerbiAccessToken: (token: string | null) => void;
};

const UiContext = createContext<UiContextValue | null>(null);

const TOKEN_STORAGE_KEY = "pbi_delegated_access_token";

export function UiProvider({ children }: { children: React.ReactNode }) {
  const [isSyncOpen, setIsSyncOpen] = useState(false);
  const [powerbiAccessToken, setPowerbiAccessTokenState] = useState<string | null>(null);
  const [hydrated, setHydrated] = useState(false);

  // Hydration-safe: server + first client render are identical (token null),
  // then we load localStorage after mount.
  React.useEffect(() => {
    setHydrated(true);
    try {
      const stored = window.localStorage.getItem(TOKEN_STORAGE_KEY);
      if (stored) setPowerbiAccessTokenState(stored);
    } catch {
      // ignore
    }
  }, []);

  const setPowerbiAccessToken = useCallback((token: string | null) => {
    setPowerbiAccessTokenState(token);
    try {
      if (!token) window.localStorage.removeItem(TOKEN_STORAGE_KEY);
      else window.localStorage.setItem(TOKEN_STORAGE_KEY, token);
    } catch {
      // ignore
    }
  }, []);

  const openSync = useCallback(() => setIsSyncOpen(true), []);
  const closeSync = useCallback(() => setIsSyncOpen(false), []);

  const value = useMemo(
    () => ({
      isSyncOpen,
      openSync,
      closeSync,
      powerbiAccessToken: hydrated ? powerbiAccessToken : null,
      setPowerbiAccessToken,
    }),
    [isSyncOpen, openSync, closeSync, powerbiAccessToken, hydrated, setPowerbiAccessToken]
  );

  return <UiContext.Provider value={value}>{children}</UiContext.Provider>;
}

export function useUi(): UiContextValue {
  const ctx = useContext(UiContext);
  if (!ctx) throw new Error("useUi must be used within UiProvider");
  return ctx;
}
