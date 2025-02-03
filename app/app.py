import os

import gradio as gr
import spaces
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain.chains.retrieval import create_retrieval_chain
from langchain_core.messages import HumanMessage, AIMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_huggingface import HuggingFacePipeline
from langchain_huggingface.embeddings import HuggingFaceEmbeddings
from langchain_pinecone import PineconeVectorStore
from peft import PeftModel
from pinecone import Pinecone as PC
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline

# Initialize Pinecone
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX = "arolchatbot"  # e.g., "us-west1-gcp-free"
pc = PC(api_key=PINECONE_API_KEY)
index = pc.Index(PINECONE_INDEX)
# Connect to Pinecone


embeddings = HuggingFaceEmbeddings(model_name="thenlper/gte-large")
vector_store = PineconeVectorStore(index, embeddings, "content")

# Model and Tokenizer
model_name = "Meldashti/chatbot"
base_model = AutoModelForCausalLM.from_pretrained("unsloth/Llama-3.2-3B")
tokenizer = AutoTokenizer.from_pretrained("unsloth/Llama-3.2-3B")

# Merge PEFT weights with base model
model = PeftModel.from_pretrained(base_model, model_name)
model = model.merge_and_unload()

# Simplified pipeline with minimal parameters
generator = pipeline(
    "text-generation",
    model=model,
    tokenizer=tokenizer,
    max_new_tokens=150,
    temperature=0.7,
)

# LLM wrapper
llm = HuggingFacePipeline(pipeline=generator)

retriever = vector_store.as_retriever(search_kwargs={"k": 5})
prompt = """

Context: {context}

Question: {input}
"""
# prompt = hub.pull("rlm/rag-prompt")
prompt_template = ChatPromptTemplate.from_messages(
    [
        HumanMessage(""""
        You are an assistant for AROL company. You need to be professional and helpful in your responses to users.
Your tone should be formal and respectful.
If you are unsure about the answer, you can kindly answer them with "I am not sure about that, please contact our support team for more information."
Please provide the most accurate and helpful response to the user's question.
Do not provide any personal information or any information that is not related to the question.
Do not include anything else but the answer to user question or request if they are related to AROL company and its services.
Given the following context, answer the question as accurately as possible.
Do not repeat the context or the question in the response. Start the response directly.
The response should contain only the answer to the question. For example: Q: Hi, Who are you? A: Hello there, I am an assistant for AROL company.
        """),
        HumanMessage(
            "Context: AROL company is a software company that provides software solutions for businesses.,  Question: Hi, Who are you?"),
        AIMessage(
            "Hello there, I am an customer care assistant for AROL company"),
        ('human', prompt),

    ]
)

# Retrieval QA Chain
qa_chain = create_stuff_documents_chain(llm=llm, prompt=prompt_template)
rag_chain = create_retrieval_chain(retriever=retriever, combine_docs_chain=qa_chain)


@spaces.GPU
def chat(message, history):
    # Chat function with extensive logging
    print(f"Received message: {message}")
    try:
        response = rag_chain.invoke({"input": message})
        print(response)
        if isinstance(response, str):
            # Clean up the output to remove unnecessary text
            response = response.split("Answer: ")[1].split("</s")[0]  # Get everything after "Answer:"
            response = response.strip()  # Remove extra spaces or newline characters
            return response
        elif isinstance(response, dict):
            response = response.get("answer", "Sorry, I couldn't generate an answer.")
            response = response.split("Answer: ")[1].split("</s")[0]
            response = response.strip()
            return response
        else:
            return "Sorry, I couldn't generate an answer."
    except Exception as e:
        print(f"Error generating response: {type(e)}, {e}")
        return f"An error occurred: {str(e)}"


# Gradio interface
demo = gr.ChatInterface(chat, type="messages", autofocus=False)

# Launch
if __name__ == "__main__":
    demo.launch(debug=True)
