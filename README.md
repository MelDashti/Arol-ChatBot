# AROL Group Smart Chatbot

## 🚀 Overview
The AROL Group Smart Chatbot is an AI-powered assistant designed to provide accurate, domain-specific responses about AROL Group's products and services. This project includes web crawling, data preprocessing, model fine-tuning, RAG (Retrieval-Augmented Generation), and deployment on Hugging Face Spaces.

The data was scraped from [AROL Group Website](https://www.arol.com/arol-group-canelli).

## 📦 Features
- **Web Crawling & Data Scraping:** Extracted structured data from AROL Group websites using Scrapy and BeautifulSoup.
- **Data Preprocessing:** Organized data into a Q&A format for model training and validation.
- **Model Fine-Tuning:** Fine-tuned LLaMA 3.2 with LoRA (Low-Rank Adaptation) for efficient training.
- **Retrieval-Augmented Generation (RAG):** Integrated with Pinecone for vector-based retrieval to enhance chatbot performance.
- **Deployment:** Deployed on Hugging Face Spaces using Gradio with real-time interaction capabilities.

## ⚙️ Installation
1. **Clone the repository:**
   ```bash
   git clone https://github.com/MelDashti/Arol-ChatBot.git
   cd Arol-ChatBot
   ```
2. **Install dependencies for deployment:**
   ```bash
   pip install -r app/requirements.txt
   ```

## 💬 Usage
### 🌐 **Interact with the Chatbot on Hugging Face Spaces:**  
[Chatbot Demo](https://huggingface.co/spaces/Meldashti/unsloth-llama-3-8b-bnb-4bit?logs=container)

### 🖥️ **Local Deployment (Optional):**
1. **Set environment variables:**
   ```bash
   export PINECONE_API_KEY="your_pinecone_api_key"
   export PINECONE_INDEX="arolchatbot"
   ```

2. **Modify `app.py` (Comment out the Hugging Face Spaces-specific line):**
   ```python
   # @spaces.GPU  # Comment this line when running locally
   def chat(message, history):
       ...
   ```

3. **Run the application locally:**
   ```bash
   cd app
   python app.py
   ```

> **Note:** Local deployment requires an active Pinecone account and internet access to load models from Hugging Face.

## 🤖 Model Details
- **Model:** LLaMA 3.2 (3B parameters)
- **Fine-Tuning:** LoRA with PEFT for efficient parameter adaptation
- **Retrieval-Augmented Generation (RAG):** Integrated with Pinecone for dynamic knowledge retrieval

## 📊 Model Fine-Tuning
The fine-tuning process was performed using a Jupyter notebook on Google Colab. This notebook covers:
- Data loading and preprocessing
- Prompt formatting for instruction-based learning
- Fine-tuning with LoRA (Low-Rank Adaptation)
- Model evaluation and deployment on Hugging Face Hub

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/MelDashti/Smart-Chatbot/blob/master/AIChatbot.ipynb)

## 🔗 Links
- **Project Repository:** [GitHub Repo](https://github.com/MelDashti/Arol-ChatBot)
- **Fine-Tuned Model:** [Hugging Face Model](https://huggingface.co/Meldashti/chatbot/tree/main)
- **Chatbot Demo:** [Hugging Face Spaces](https://huggingface.co/spaces/Meldashti/unsloth-llama-3-8b-bnb-4bit?logs=container)

## 🖼️ Screenshot
![photo_2025-02-04_00-33-25](https://github.com/user-attachments/assets/41fa1313-dc89-4909-b882-0476a56c18bc)

## 📊 Experiment Tracking
Experiment performance and model metrics were tracked using **Weights & Biases (wandb)**. Logs are available under the `model/wandb/` directory.

## 👨‍💻 Authors
- **[Meelad Dashti](https://github.com/MelDashti)**
- **[Mohammad Hakimi](https://github.com/mohammad-hakimi)**
- **[Federico La Macchia](https://github.com/FedeLM1999)**


