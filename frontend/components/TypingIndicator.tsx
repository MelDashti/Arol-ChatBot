'use client'

import { Bot } from 'lucide-react'

export function TypingIndicator() {
  return (
    <div className="flex gap-3 animate-fade-in">
      <div className="flex-shrink-0 w-8 h-8 rounded-full bg-secondary flex items-center justify-center">
        <Bot className="w-5 h-5 text-secondary-foreground" />
      </div>

      <div className="rounded-lg px-4 py-3 bg-muted">
        <div className="flex gap-1">
          <div className="w-2 h-2 rounded-full bg-foreground/40 animate-pulse-dot" style={{ animationDelay: '0ms' }} />
          <div className="w-2 h-2 rounded-full bg-foreground/40 animate-pulse-dot" style={{ animationDelay: '200ms' }} />
          <div className="w-2 h-2 rounded-full bg-foreground/40 animate-pulse-dot" style={{ animationDelay: '400ms' }} />
        </div>
      </div>
    </div>
  )
}
