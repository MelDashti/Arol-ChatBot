'use client'

import { useState } from 'react'
import { ChatInterface } from '@/components/ChatInterface'
import { Sidebar } from '@/components/Sidebar'
import { Header } from '@/components/Header'
import { useAuthStore } from '@/lib/store'
import { LoginForm } from '@/components/LoginForm'

export default function Home() {
  const [sidebarOpen, setSidebarOpen] = useState(true)
  const { user, isAuthenticated } = useAuthStore()

  if (!isAuthenticated) {
    return <LoginForm />
  }

  return (
    <div className="flex h-screen bg-background">
      {/* Sidebar */}
      <Sidebar isOpen={sidebarOpen} onClose={() => setSidebarOpen(false)} />

      {/* Main Content */}
      <div className="flex flex-col flex-1 overflow-hidden">
        <Header onMenuClick={() => setSidebarOpen(!sidebarOpen)} />

        <main className="flex-1 overflow-hidden">
          <ChatInterface />
        </main>
      </div>
    </div>
  )
}
