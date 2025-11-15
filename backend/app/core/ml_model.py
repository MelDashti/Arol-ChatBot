"""
ML Model Management with RAG pipeline
Handles model loading, inference, and streaming responses
"""

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline, TextIteratorStreamer
from peft import PeftModel
from langchain_huggingface import HuggingFacePipeline
from langchain_huggingface.embeddings import HuggingFaceEmbeddings
from langchain_pinecone import PineconeVectorStore
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain.chains.retrieval import create_retrieval_chain
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.messages import HumanMessage, AIMessage
from pinecone import Pinecone as PC
from threading import Thread
from typing import AsyncGenerator, Optional
import logging
import asyncio

from app.core.config import settings
from app.core.cache import cache_manager

logger = logging.getLogger(__name__)


class MLModelManager:
    """Manages ML models and RAG pipeline"""

    def __init__(self):
        self.model = None
        self.tokenizer = None
        self.llm = None
        self.rag_chain = None
        self.vector_store = None
        self.embeddings = None
        self.loaded = False

    async def load_models(self):
        """Load all ML models and initialize RAG pipeline"""
        if self.loaded:
            logger.info("Models already loaded")
            return

        try:
            logger.info("Loading embeddings model...")
            self.embeddings = HuggingFaceEmbeddings(
                model_name=settings.EMBEDDING_MODEL
            )

            logger.info("Connecting to Pinecone...")
            pc = PC(api_key=settings.PINECONE_API_KEY)
            index = pc.Index(settings.PINECONE_INDEX)
            self.vector_store = PineconeVectorStore(
                index,
                self.embeddings,
                "content"
            )

            logger.info(f"Loading base model: {settings.BASE_MODEL}")
            base_model = AutoModelForCausalLM.from_pretrained(
                settings.BASE_MODEL,
                torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
                device_map="auto" if torch.cuda.is_available() else None
            )

            logger.info(f"Loading tokenizer...")
            self.tokenizer = AutoTokenizer.from_pretrained(settings.BASE_MODEL)

            logger.info(f"Loading fine-tuned model: {settings.MODEL_NAME}")
            self.model = PeftModel.from_pretrained(base_model, settings.MODEL_NAME)
            self.model = self.model.merge_and_unload()

            # Create pipeline
            logger.info("Creating text generation pipeline...")
            generator = pipeline(
                "text-generation",
                model=self.model,
                tokenizer=self.tokenizer,
                max_new_tokens=settings.MAX_NEW_TOKENS,
                temperature=settings.TEMPERATURE,
            )

            self.llm = HuggingFacePipeline(pipeline=generator)

            # Create RAG chain
            logger.info("Setting up RAG chain...")
            retriever = self.vector_store.as_retriever(
                search_kwargs={"k": settings.TOP_K_RETRIEVAL}
            )

            prompt_template = ChatPromptTemplate.from_messages([
                HumanMessage("""
You are an assistant for AROL company. You need to be professional and helpful in your responses to users.
Your tone should be formal and respectful.
If you are unsure about the answer, you can kindly answer them with "I am not sure about that, please contact our support team for more information."
Please provide the most accurate and helpful response to the user's question.
Do not provide any personal information or any information that is not related to the question.
Do not include anything else but the answer to user question or request if they are related to AROL company and its services.
Given the following context, answer the question as accurately as possible.
Do not repeat the context or the question in the response. Start the response directly.
The response should contain only the answer to the question.
                """),
                HumanMessage(
                    "Context: AROL company is a software company that provides software solutions for businesses. Question: Hi, Who are you?"
                ),
                AIMessage("Hello there, I am a customer care assistant for AROL company"),
                ('human', "Context: {context}\n\nQuestion: {input}"),
            ])

            qa_chain = create_stuff_documents_chain(
                llm=self.llm,
                prompt=prompt_template
            )
            self.rag_chain = create_retrieval_chain(
                retriever=retriever,
                combine_docs_chain=qa_chain
            )

            self.loaded = True
            logger.info("All models loaded successfully!")

        except Exception as e:
            logger.error(f"Failed to load models: {e}", exc_info=True)
            raise

    async def get_response(
        self,
        message: str,
        conversation_id: Optional[str] = None,
        use_cache: bool = True
    ) -> str:
        """Get response from RAG chain with optional caching"""

        if not self.loaded:
            await self.load_models()

        # Check cache first
        if use_cache:
            cache_key = f"chat:{message[:100]}"  # Use first 100 chars as key
            cached = await cache_manager.get(cache_key)
            if cached:
                logger.info("Returning cached response")
                return cached

        try:
            # Run inference in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.rag_chain.invoke({"input": message})
            )

            # Extract answer
            if isinstance(response, dict):
                answer = response.get("answer", "Sorry, I couldn't generate an answer.")
            else:
                answer = str(response)

            # Clean up response
            if "Answer: " in answer:
                answer = answer.split("Answer: ")[1]
            if "</s" in answer:
                answer = answer.split("</s")[0]
            answer = answer.strip()

            # Cache the response
            if use_cache:
                await cache_manager.set(cache_key, answer, ttl=settings.CACHE_TTL)

            return answer

        except Exception as e:
            logger.error(f"Error generating response: {e}", exc_info=True)
            return f"An error occurred while processing your request. Please try again."

    async def stream_response(
        self,
        message: str,
        conversation_id: Optional[str] = None
    ) -> AsyncGenerator[str, None]:
        """Stream response word by word for real-time UI"""

        if not self.loaded:
            await self.load_models()

        try:
            # Get full response first (streaming with RAG chain is complex)
            response = await self.get_response(message, conversation_id, use_cache=False)

            # Simulate streaming word by word
            words = response.split()
            for i, word in enumerate(words):
                # Add space except for last word
                chunk = word if i == len(words) - 1 else word + " "
                yield chunk
                await asyncio.sleep(0.05)  # Small delay for streaming effect

        except Exception as e:
            logger.error(f"Error streaming response: {e}", exc_info=True)
            yield f"Error: {str(e)}"

    async def cleanup(self):
        """Cleanup models and free memory"""
        logger.info("Cleaning up ML models...")
        if self.model is not None:
            del self.model
        if self.tokenizer is not None:
            del self.tokenizer
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        self.loaded = False
        logger.info("Cleanup complete")


# Global instance
model_manager = MLModelManager()
