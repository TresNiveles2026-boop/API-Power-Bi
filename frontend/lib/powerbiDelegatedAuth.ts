"use client";

// Minimal delegated token acquisition (optional):
// If the tenant/app is not configured for SPA auth, the app can still work
// by injecting `powerbiAccessToken` from elsewhere (e.g. server session).
//
// This module is best-effort and guarded by env vars.

export async function acquirePowerBiDelegatedToken(): Promise<string> {
  const clientId = process.env.NEXT_PUBLIC_AZURE_CLIENT_ID;
  const tenantId = process.env.NEXT_PUBLIC_AZURE_TENANT_ID;

  if (!clientId || !tenantId) {
    throw new Error(
      "Microsoft login no configurado. Define NEXT_PUBLIC_AZURE_CLIENT_ID y NEXT_PUBLIC_AZURE_TENANT_ID."
    );
  }

  const { PublicClientApplication } = await import("@azure/msal-browser");

  const msal = new PublicClientApplication({
    auth: {
      clientId,
      authority: `https://login.microsoftonline.com/${tenantId}`,
      redirectUri: typeof window !== "undefined" ? window.location.origin : undefined,
    },
    cache: {
      cacheLocation: "localStorage",
      storeAuthStateInCookie: false,
    },
  });

  await msal.initialize();

  const scopes = [
    "https://analysis.windows.net/powerbi/api/Report.Read.All",
    "https://analysis.windows.net/powerbi/api/Dataset.Read.All",
    "https://analysis.windows.net/powerbi/api/Workspace.Read.All"
  ];

  const accounts = msal.getAllAccounts();
  const active = accounts[0];

  if (!active) {
    await msal.loginPopup({ scopes, prompt: "select_account" });
  }

  const account = msal.getAllAccounts()[0];
  if (!account) throw new Error("No se pudo iniciar sesión con Microsoft.");

  const result: unknown = await msal.acquireTokenSilent({ account, scopes }).catch(async () => {
    return msal.acquireTokenPopup({ account, scopes });
  });

  const token =
    typeof result === "object" &&
      result !== null &&
      "accessToken" in result &&
      typeof (result as { accessToken?: unknown }).accessToken === "string"
      ? (result as { accessToken: string }).accessToken
      : null;
  if (!token) throw new Error("No se pudo obtener el access token de Power BI.");
  return token;
}
