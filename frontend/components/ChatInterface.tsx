'use client'

import { useState, useRef, useEffect } from 'react'
import { Send, Paperclip, Loader2 } from 'lucide-react'
import { MessageList } from './MessageList'
import { useChatStore } from '@/lib/store'
import { sendMessage } from '@/lib/api'
import toast from 'react-hot-toast'

export function ChatInterface() {
  const [input, setInput] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  const { currentConversation, messages, addMessage } = useChatStore()

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!input.trim() || isLoading) return

    const userMessage = input.trim()
    setInput('')
    setIsLoading(true)

    // Add user message immediately
    addMessage({
      role: 'user',
      content: userMessage,
      id: Date.now().toString(),
    })

    try {
      const response = await sendMessage({
        message: userMessage,
        conversation_id: currentConversation?.id,
      })

      // Add AI response
      addMessage({
        role: 'assistant',
        content: response.message,
        id: (Date.now() + 1).toString(),
        processing_time_ms: response.processing_time_ms,
      })
    } catch (error: any) {
      toast.error(error.message || 'Failed to send message')
      console.error('Error sending message:', error)
    } finally {
      setIsLoading(false)
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSubmit(e)
    }
  }

  // Auto-resize textarea
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto'
      textareaRef.current.style.height = `${textareaRef.current.scrollHeight}px`
    }
  }, [input])

  return (
    <div className="flex flex-col h-full">
      {/* Messages */}
      <div className="flex-1 overflow-y-auto">
        <MessageList messages={messages} isLoading={isLoading} />
      </div>

      {/* Input Area */}
      <div className="border-t border-border bg-background p-4">
        <form onSubmit={handleSubmit} className="max-w-4xl mx-auto">
          <div className="flex items-end gap-2">
            <button
              type="button"
              className="p-2 rounded-lg hover:bg-muted transition-colors"
              aria-label="Attach file"
            >
              <Paperclip className="w-5 h-5 text-muted-foreground" />
            </button>

            <div className="flex-1 relative">
              <textarea
                ref={textareaRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Ask me anything about AROL..."
                className="w-full resize-none rounded-lg border border-input bg-background px-4 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring max-h-32 scrollbar-hide"
                rows={1}
                disabled={isLoading}
              />
            </div>

            <button
              type="submit"
              disabled={!input.trim() || isLoading}
              className="p-3 rounded-lg bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-all"
              aria-label="Send message"
            >
              {isLoading ? (
                <Loader2 className="w-5 h-5 animate-spin" />
              ) : (
                <Send className="w-5 h-5" />
              )}
            </button>
          </div>

          <div className="mt-2 text-xs text-muted-foreground text-center">
            Press Enter to send, Shift+Enter for new line
          </div>
        </form>
      </div>
    </div>
  )
}
