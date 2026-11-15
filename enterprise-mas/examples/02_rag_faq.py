# 02_rag_faq.py

A RAG (Retrieval-Augmented Generation) FAQ agent that demonstrates:

- Memory store integration
- Vector search via pgvector
- Agent with knowledge retrieval

## Usage

```bash
# Bootstrap setup
mas admin bootstrap   --organization org-1234   --tenant tenant-5678   --project project-9012   --owner owner-3456

# Create a RAG-enabled agent
mas agent create   --name "rag-faq"   --scope tenant   --capabilities "retrieval,chat"

# Submit execution with knowledge base
mas execution submit   --project project-9012   --agent rag-faq   --input '{"question": "What is the return policy?"}'
```
