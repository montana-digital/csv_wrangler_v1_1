"""
Cache management utilities for CSV Wrangler.

Provides functions for invalidating Streamlit cache when data changes.
"""
from typing import Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def invalidate_dataset_cache(dataset_id: int) -> None:
    """
    Invalidate all cached data for a specific dataset.
    
    Clears Streamlit cache entries related to:
    - Dataset DataFrame loading
    - Dataset statistics
    - Dataset row counts
    
    Args:
        dataset_id: Dataset ID to invalidate cache for
    """
    try:
        # Import streamlit only when needed (may not be available in all contexts)
        import streamlit as st
        
        # Increment cache version for this dataset
        cache_version_key = f"dataset_cache_version_{dataset_id}"
        current_version = st.session_state.get(cache_version_key, 0)
        st.session_state[cache_version_key] = current_version + 1
        
        logger.info(f"Invalidated cache for dataset {dataset_id} (version: {current_version + 1})")
        
    except ImportError:
        # Streamlit not available (e.g., in tests or non-Streamlit contexts)
        logger.debug("Streamlit not available, skipping cache invalidation")
    except Exception as e:
        logger.warning(f"Failed to invalidate cache for dataset {dataset_id}: {e}")


def invalidate_enriched_dataset_cache(enriched_dataset_id: int) -> None:
    """
    Invalidate all cached data for a specific enriched dataset.
    
    Args:
        enriched_dataset_id: Enriched dataset ID to invalidate cache for
    """
    try:
        import streamlit as st
        
        cache_version_key = f"enriched_dataset_cache_version_{enriched_dataset_id}"
        current_version = st.session_state.get(cache_version_key, 0)
        st.session_state[cache_version_key] = current_version + 1
        
        logger.info(f"Invalidated cache for enriched dataset {enriched_dataset_id} (version: {current_version + 1})")
        
    except ImportError:
        logger.debug("Streamlit not available, skipping cache invalidation")
    except Exception as e:
        logger.warning(f"Failed to invalidate cache for enriched dataset {enriched_dataset_id}: {e}")


def get_cache_version(dataset_id: Optional[int] = None, enriched_dataset_id: Optional[int] = None) -> int:
    """
    Get current cache version for a dataset.
    
    Used as part of cache key to force invalidation.
    
    Args:
        dataset_id: Dataset ID
        enriched_dataset_id: Enriched dataset ID
        
    Returns:
        Cache version number (0 if not set)
    """
    try:
        import streamlit as st
        
        if dataset_id:
            cache_version_key = f"dataset_cache_version_{dataset_id}"
            return st.session_state.get(cache_version_key, 0)
        elif enriched_dataset_id:
            cache_version_key = f"enriched_dataset_cache_version_{enriched_dataset_id}"
            return st.session_state.get(cache_version_key, 0)
    except ImportError:
        # Streamlit not available, return 0 (no cache version)
        pass
    except Exception:
        # If anything fails, return 0 (no cache version)
        pass
    
    return 0


def clear_all_cache() -> None:
    """
    Clear all Streamlit cache (use with caution).
    
    This clears ALL cached data, not just dataset-related.
    Only use in exceptional circumstances.
    """
    try:
        import streamlit as st
        
        # Streamlit doesn't have a direct way to clear all cache,
        # but we can clear session state cache versions
        keys_to_remove = [
            key for key in st.session_state.keys()
            if key.startswith("dataset_cache_version_") or key.startswith("enriched_dataset_cache_version_")
        ]
        for key in keys_to_remove:
            del st.session_state[key]
        
        logger.info("Cleared all cache version tracking")
        
    except ImportError:
        logger.debug("Streamlit not available, skipping cache clear")
    except Exception as e:
        logger.warning(f"Failed to clear cache: {e}")

