"""
Cache module containing classes for caching different types of content.
"""
from cache.document_cache import DocumentCache
from cache.web_content_cache import ContentCache

__all__ = [
    'DocumentCache',
    'ContentCache',
]