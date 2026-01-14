from sentence_transformers import SentenceTransformer
import threading

# Global model cache to share models across instances
_model_cache = {}
_cache_lock = threading.Lock()

def _get_or_load_model(model_name):
    """Get a cached model or load it if not in cache. Thread-safe."""
    if model_name not in _model_cache:
        with _cache_lock:
            # Double-check pattern to avoid race conditions
            if model_name not in _model_cache:
                _model_cache[model_name] = SentenceTransformer(model_name)
    return _model_cache[model_name]

class SentenceEmbedder:
    def __init__(self, model_name="sentence-transformers/all-MiniLM-L6-v2"):
        self.model_name = model_name
        self._model = None  # Lazy-loaded
        self._dimensions = None  # Cached dimensions

    @property
    def model(self):
        """Lazy-load the model on first access."""
        if self._model is None:
            self._model = _get_or_load_model(self.model_name)
        return self._model

    def embed(self, text):
        return self.model.encode(text)
    
    def get_number_of_dimensions(self):
        """Get embedding dimensions, caching the result."""
        if self._dimensions is None:
            self._dimensions = self.model.get_sentence_embedding_dimension()
        return self._dimensions
