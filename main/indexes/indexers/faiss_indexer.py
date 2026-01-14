import faiss
import numpy as np
import os


class FaissIndexer:
    def __init__(self, name, embedder, serialized_index=None, index_file_path=None, use_memory_map=True):
        """
        Initialize FAISS indexer.
        
        Args:
            name: Indexer name
            embedder: SentenceEmbedder instance
            serialized_index: Serialized index data (for backward compatibility)
            index_file_path: Path to FAISS index file (for memory-mapped loading)
            use_memory_map: If True and index_file_path provided, use memory-mapped index
        """
        self.name = name
        self.embedder = embedder
        self._use_memory_map = use_memory_map
        self._index_file_path = index_file_path
        
        if index_file_path and use_memory_map and os.path.exists(index_file_path):
            # Load using FAISS file format (FAISS may use memory mapping automatically for large indexes)
            # This is more memory-efficient than deserializing the full index
            try:
                self.faiss_index = faiss.read_index(index_file_path)
            except Exception as e:
                # Fallback to deserializing if file format loading fails
                if serialized_index is not None:
                    self.faiss_index = faiss.deserialize_index(serialized_index)
                else:
                    raise e
        elif serialized_index is not None:
            # Fallback to deserializing (loads full index into memory)
            self.faiss_index = faiss.deserialize_index(serialized_index)
        else:
            # Create new index
            self.faiss_index = faiss.IndexIDMap(faiss.IndexFlatL2(embedder.get_number_of_dimensions()))

    def get_name(self):
        return self.name

    def index_texts(self, ids, texts):
        """Add texts to index. Note: This requires a writable index."""
        if self._use_memory_map and hasattr(self.faiss_index, 'is_trained') and not self.faiss_index.is_trained:
            raise RuntimeError("Cannot write to memory-mapped read-only index")
        self.faiss_index.add_with_ids(self.embedder.embed(texts), ids)

    def remove_ids(self, ids):
        """Remove IDs from index. Note: This requires a writable index."""
        if self._use_memory_map:
            raise RuntimeError("Cannot modify memory-mapped read-only index")
        self.faiss_index.remove_ids(ids)

    def serialize(self):
        """Serialize index to bytes. For memory-mapped indexes, this loads into memory."""
        return faiss.serialize_index(self.faiss_index)

    def save_to_file(self, file_path):
        """Save index to file for memory-mapped loading."""
        faiss.write_index(self.faiss_index, file_path)

    def search(self, text, number_of_results=10):
        return self.faiss_index.search(np.expand_dims(self.embedder.embed(text), axis=0), number_of_results)
    
    def get_size(self):
        return self.faiss_index.ntotal