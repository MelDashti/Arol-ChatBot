'use client'

import { Bot, User, ThumbsUp, ThumbsDown, Copy, Check } from 'lucide-react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism'
import { useState } from 'react'
import type { Message } from '@/lib/types'

interface MessageBubbleProps {
  message: Message
}

export function MessageBubble({ message }: MessageBubbleProps) {
  const [copied, setCopied] = useState(false)
  const isUser = message.role === 'user'

  const copyToClipboard = () => {
    navigator.clipboard.writeText(message.content)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div className={`flex gap-3 ${isUser ? 'flex-row-reverse' : 'flex-row'} animate-fade-in`}>
      {/* Avatar */}
      <div
        className={`flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center ${
          isUser ? 'bg-primary' : 'bg-secondary'
        }`}
      >
        {isUser ? (
          <User className="w-5 h-5 text-primary-foreground" />
        ) : (
          <Bot className="w-5 h-5 text-secondary-foreground" />
        )}
      </div>

      {/* Message Content */}
      <div className={`flex-1 ${isUser ? 'flex flex-col items-end' : ''}`}>
        <div
          className={`rounded-lg px-4 py-3 max-w-[85%] ${
            isUser
              ? 'bg-primary text-primary-foreground'
              : 'bg-muted text-foreground'
          }`}
        >
          {isUser ? (
            <p className="whitespace-pre-wrap">{message.content}</p>
          ) : (
            <div className="markdown-body">
              <ReactMarkdown
                remarkPlugins={[remarkGfm]}
                components={{
                  code({ node, inline, className, children, ...props }) {
                    const match = /language-(\w+)/.exec(className || '')
                    return !inline && match ? (
                      <SyntaxHighlighter
                        {...props}
                        style={oneDark}
                        language={match[1]}
                        PreTag="div"
                      >
                        {String(children).replace(/\n$/, '')}
                      </SyntaxHighlighter>
                    ) : (
                      <code {...props} className={className}>
                        {children}
                      </code>
                    )
                  },
                }}
              >
                {message.content}
              </ReactMarkdown>
            </div>
          )}
        </div>

        {/* Message Actions (for AI messages) */}
        {!isUser && (
          <div className="flex items-center gap-2 mt-2 text-xs text-muted-foreground">
            {message.processing_time_ms && (
              <span>{message.processing_time_ms}ms</span>
            )}

            <button
              onClick={copyToClipboard}
              className="p-1 hover:text-foreground transition-colors"
              title="Copy message"
            >
              {copied ? <Check className="w-4 h-4" /> : <Copy className="w-4 h-4" />}
            </button>

            <button
              className="p-1 hover:text-foreground transition-colors"
              title="Helpful"
            >
              <ThumbsUp className="w-4 h-4" />
            </button>

            <button
              className="p-1 hover:text-foreground transition-colors"
              title="Not helpful"
            >
              <ThumbsDown className="w-4 h-4" />
            </button>
          </div>
        )}
      </div>
    </div>
  )
}
