"""
Dataset #3 page for CSV Wrangler.
"""
from src.pages._dataset_page_template import render_dataset_page
from src.ui.components.sidebar import render_sidebar

# Render uniform sidebar
render_sidebar()

# Render dataset page for slot 3
render_dataset_page(slot_number=3)
