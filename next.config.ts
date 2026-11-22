import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Docker image ships .next/standalone (see Dockerfile runner stage)
  output: "standalone",
  images: {
    unoptimized: false,
  },
  experimental: {
    serverActions: {
      bodySizeLimit: "4mb",
    },
  },
};

export default nextConfig;
