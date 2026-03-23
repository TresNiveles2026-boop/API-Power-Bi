import { NextRequest } from "next/server";

export const maxDuration = 240;

const BACKEND_BASE_URL =
  process.env.BACKEND_URL ||
  process.env.NEXT_PUBLIC_API_URL ||
  "http://localhost:8002";

export async function POST(request: NextRequest) {
  try {
    const incoming = await request.formData();

    const backendUrl = `${BACKEND_BASE_URL}/api/v1/upload-pbit`;
    const form = new FormData();

    const file = incoming.get("file");
    const reportId = incoming.get("report_id");
    const tenantId = incoming.get("tenant_id");

    if (reportId) form.append("report_id", String(reportId));
    if (tenantId) form.append("tenant_id", String(tenantId));
    if (file instanceof File) {
      form.append("file", file, file.name);
    }

    const backendResponse = await fetch(backendUrl, {
      method: "POST",
      headers: {
        ...(request.headers.get("x-api-key")
          ? { "X-API-Key": request.headers.get("x-api-key") as string }
          : {}),
      },
      body: form,
      cache: "no-store",
    });

    const responseBody = await backendResponse.text();
    return new Response(responseBody, {
      status: backendResponse.status,
      headers: {
        "Content-Type":
          backendResponse.headers.get("content-type") || "application/json",
      },
    });
  } catch {
    return Response.json(
      {
        detail: "Error de conexión entre frontend y backend.",
        error_type: "PROXY_CONNECTION_ERROR",
      },
      { status: 502 }
    );
  }
}
