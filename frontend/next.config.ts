import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // WHY: Permitir que el frontend de Next.js se comunique con
  // el backend FastAPI sin problemas de CORS durante desarrollo.
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: "http://localhost:8002/api/:path*",
      },
      {
        source: "/health",
        destination: "http://localhost:8002/health",
      },
    ];
  },
};

export default nextConfig;
