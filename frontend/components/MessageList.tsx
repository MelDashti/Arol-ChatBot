'use client'

import { useEffect, useRef } from 'react'
import { MessageBubble } from './MessageBubble'
import { TypingIndicator } from './TypingIndicator'
import type { Message } from '@/lib/types'

interface MessageListProps {
  messages: Message[]
  isLoading: boolean
}

export function MessageList({ messages, isLoading }: MessageListProps) {
  const messagesEndRef = useRef<HTMLDivElement>(null)

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  useEffect(() => {
    scrollToBottom()
  }, [messages, isLoading])

  if (messages.length === 0) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center max-w-md px-4">
          <h2 className="text-2xl font-bold mb-2">Welcome to Arol AI</h2>
          <p className="text-muted-foreground mb-6">
            I'm your intelligent assistant for all AROL-related questions. Ask me anything!
          </p>
          <div className="grid grid-cols-1 gap-2 text-sm">
            <button className="p-3 rounded-lg border border-border hover:bg-muted text-left transition-colors">
              What products does AROL offer?
            </button>
            <button className="p-3 rounded-lg border border-border hover:bg-muted text-left transition-colors">
              Tell me about capping systems
            </button>
            <button className="p-3 rounded-lg border border-border hover:bg-muted text-left transition-colors">
              How can I contact AROL support?
            </button>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="max-w-4xl mx-auto px-4 py-6 space-y-4">
      {messages.map((message) => (
        <MessageBubble key={message.id} message={message} />
      ))}

      {isLoading && <TypingIndicator />}

      <div ref={messagesEndRef} />
    </div>
  )
}
