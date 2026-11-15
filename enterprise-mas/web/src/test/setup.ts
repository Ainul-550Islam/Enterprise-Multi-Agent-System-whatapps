import '@testing-library/jest-dom/vitest'

// Setup environment variables for testing
process.env.NEXT_PUBLIC_APP_URL = 'http://localhost:3000'
process.env.OIDC_CLIENT_ID = 'test-client'
process.env.OIDC_ISSUER = 'https://accounts.example.com'
process.env.MAS_API_URL = 'http://localhost:8080'
process.env.NEXT_PUBLIC_DISABLE_ANALYTICS = 'true'
