"""
Knowledge Base Search page for CSV Wrangler.

Provides fast search across Knowledge Tables and enriched datasets.
Two-phase approach: presence flags first, detailed retrieval on drill-down.
"""
import streamlit as st

from src.database.connection import get_session
from src.ui.components.knowledge_search import render_knowledge_search
from src.ui.components.sidebar import render_sidebar

# Render uniform sidebar
render_sidebar()

st.title("🔍 Knowledge Base Search")
st.markdown(
    "**Fast search across all Knowledge Tables and enriched datasets**\n\n"
    "Search for phone numbers, emails, or web domains to find which Knowledge Tables and "
    "enriched datasets contain matching values. View detailed data on demand."
)
st.markdown("---")

with get_session() as session:
    render_knowledge_search(session)

