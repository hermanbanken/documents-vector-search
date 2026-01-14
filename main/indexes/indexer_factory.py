import os
from .indexers.faiss_indexer import FaissIndexer
from .embeddings.sentence_embeder import SentenceEmbedder

def create_indexer(indexer_name):
    if indexer_name == "indexer_FAISS_IndexFlatL2__embeddings_all-MiniLM-L6-v2":
        return FaissIndexer(indexer_name, SentenceEmbedder(model_name="sentence-transformers/all-MiniLM-L6-v2"))
    
    if indexer_name == "indexer_FAISS_IndexFlatL2__embeddings_all-mpnet-base-v2":
        return FaissIndexer(indexer_name, SentenceEmbedder(model_name="sentence-transformers/all-mpnet-base-v2"))
    
    if indexer_name == "indexer_FAISS_IndexFlatL2__embeddings_multi-qa-distilbert-cos-v1":
        return FaissIndexer(indexer_name, SentenceEmbedder(model_name="sentence-transformers/multi-qa-distilbert-cos-v1"))

    raise ValueError(f"Unknown indexer name: {indexer_name}")

def load_indexer(indexer_name, collection_name, persister, use_memory_map=True, read_only=True):
    """
    Load indexer with optional memory-mapped FAISS index for reduced memory usage.
    
    Args:
        indexer_name: Name of the indexer
        collection_name: Name of the collection
        persister: DiskPersister instance
        use_memory_map: If True, try to use memory-mapped index file (read-only, OS-managed memory)
        read_only: If True, can use memory-mapped read-only index. If False, must load into memory for writes.
    """
    index_base_path = f"{collection_name}/indexes/{indexer_name}"
    index_file_path = os.path.join(persister.base_path, f"{index_base_path}/indexer.faiss")
    
    # Determine model name based on indexer name
    if indexer_name == "indexer_FAISS_IndexFlatL2__embeddings_all-MiniLM-L6-v2":
        model_name = "sentence-transformers/all-MiniLM-L6-v2"
    elif indexer_name == "indexer_FAISS_IndexFlatL2__embeddings_all-mpnet-base-v2":
        model_name = "sentence-transformers/all-mpnet-base-v2"
    elif indexer_name == "indexer_FAISS_IndexFlatL2__embeddings_multi-qa-distilbert-cos-v1":
        model_name = "sentence-transformers/multi-qa-distilbert-cos-v1"
    else:
        raise ValueError(f"Unknown indexer name: {indexer_name}")
    
    embedder = SentenceEmbedder(model_name=model_name)
    
    # Try memory-mapped loading first (more memory efficient, but read-only)
    if use_memory_map and read_only and os.path.exists(index_file_path):
        return FaissIndexer(indexer_name, embedder, index_file_path=index_file_path, use_memory_map=True)
    
    # Load into memory (for writes or if memory-mapped file doesn't exist)
    serialized_index = persister.read_bin_file(f"{index_base_path}/indexer")
    return FaissIndexer(indexer_name, embedder, serialized_index=serialized_index, use_memory_map=False)