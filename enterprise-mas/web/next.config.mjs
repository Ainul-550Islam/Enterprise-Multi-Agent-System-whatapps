/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: false,
  experimental: {
    appRouter: true,
  },
  env: {
    MAS_API_URL: process.env.MAS_API_URL,
    OIDC_ISSUER: process.env.OIDC_ISSUER,
    OIDC_CLIENT_ID: process.env.OIDC_CLIENT_ID,
  },
}
export default nextConfig
