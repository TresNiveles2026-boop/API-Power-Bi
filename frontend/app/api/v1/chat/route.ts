import { NextRequest } from "next/server";

export const maxDuration = 240;

const BACKEND_BASE_URL =
  process.env.BACKEND_URL ||
  process.env.NEXT_PUBLIC_API_URL ||
  "http://localhost:8002";

const CHAT_TIMEOUT_MS = 240_000;

export async function POST(request: NextRequest) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), CHAT_TIMEOUT_MS);

  try {
    const body = await request.text();
    const backendUrl = `${BACKEND_BASE_URL}/api/v1/chat`;

    const backendResponse = await fetch(backendUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(request.headers.get("x-api-key")
          ? { "X-API-Key": request.headers.get("x-api-key") as string }
          : {}),
      },
      body,
      signal: controller.signal,
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
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      return Response.json(
        {
          detail:
            "Timeout en proxy de chat (240s). El backend no respondió a tiempo.",
          error_type: "PROXY_TIMEOUT",
        },
        { status: 504 }
      );
    }

    return Response.json(
      {
        detail: "Error de conexión entre frontend y backend.",
        error_type: "PROXY_CONNECTION_ERROR",
      },
      { status: 502 }
    );
  } finally {
    clearTimeout(timeout);
  }
}
