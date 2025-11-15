import type { Metadata } from 'next'
import { Inter } from 'next/font/google'
import './globals.css'
import { Providers } from '@/components/Providers'
import { Toaster } from 'react-hot-toast'

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  title: 'Arol AI Chatbot - Intelligent Conversational Assistant',
  description: 'Production-grade AI chatbot with RAG, real-time streaming, and advanced analytics',
  keywords: ['AI', 'chatbot', 'RAG', 'LLaMA', 'conversational AI'],
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className={inter.className}>
        <Providers>
          {children}
          <Toaster
            position="top-right"
            toastOptions={{
              duration: 3000,
              style: {
                background: '#363636',
                color: '#fff',
              },
            }}
          />
        </Providers>
      </body>
    </html>
  )
}
