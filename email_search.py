# email_search_app.py
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timedelta
import re
import os
from dotenv import load_dotenv

# Load environment variables (for local development)
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Email Search Tool",
    page_icon="📧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize BigQuery client (cached to avoid recreating)
@st.cache_resource
def get_bigquery_client():
    # Try to use Streamlit secrets first (for deployment), fallback to local auth
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(credentials=credentials)
    except (KeyError, FileNotFoundError):
        # Local development - use default credentials
        return bigquery.Client()

client = get_bigquery_client()

# Configuration - try Streamlit secrets first, fallback to environment variables
PROJECT_ID = st.secrets.get("PROJECT_ID", os.getenv("PROJECT_ID"))
DATASET = st.secrets.get("DATASET", os.getenv("DATASET"))
TABLE = st.secrets.get("TABLE", os.getenv("TABLE"))

# Custom CSS for better styling
st.markdown("""
<style>
    .stTextInput > div > div > input {
        font-size: 16px;
    }
    .result-card {
        padding: 15px;
        border-radius: 5px;
        border: 1px solid #ddd;
        margin-bottom: 10px;
        background-color: #f9f9f9;
    }
    .highlight {
        background-color: yellow;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Main title
st.title("Email Search Interface")
st.markdown("Search through email summaries and metadata")

# Sidebar for filters
with st.sidebar:
    st.header("Search Filters")
    
    # Results limit
    limit = st.slider("Max results", 10, 500, 100, 10)

# Main search area
col1, col2 = st.columns([4, 1])

with col1:
    search_query = st.text_input(
        "Search terms:",
        placeholder="Enter keywords to search...",
        key="search_input",
        help="Use spaces to search multiple keywords"
    )

with col2:
    st.write("")  # Spacing
    st.write("")  # Spacing
    search_button = st.button("🔍 Search", type="primary", use_container_width=True)

# Search function with caching
@st.cache_data(ttl=300)  # Cache for 5 minutes
def search_emails(query, limit):
    """Execute BigQuery search with filters"""
    
    if not query:
        return pd.DataFrame()
    
    # Build WHERE clause - simple keyword search in summary and subject
    query_params = []
    
    # Split into keywords and search in both Body and Subject
    keywords = query.split()
    keyword_conditions = []
    for i, keyword in enumerate(keywords):
        condition = f"(LOWER(Body) LIKE LOWER(@keyword_{i}) OR LOWER(Subject) LIKE LOWER(@keyword_{i}))"
        keyword_conditions.append(condition)
        query_params.append(bigquery.ScalarQueryParameter(f"keyword_{i}", "STRING", f"%{keyword}%"))
    
    where_clause = " AND ".join(keyword_conditions)
    
    # Simple query - just search and sort by date
    sql_query = f"""
    SELECT 
        email_id,
        Body,
        Subject,
        `From` as sender,
        `To` as recipient,
        Date_Sent as date,
        filename
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {where_clause}
    ORDER BY Date_Sent DESC
    LIMIT @limit
    """
    
    query_params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    
    # Execute query
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    
    try:
        query_job = client.query(sql_query, job_config=job_config)
        df = query_job.to_dataframe()
        return df
    except Exception as e:
        st.error(f"Query error: {str(e)}")
        return pd.DataFrame()

# Highlight function
def highlight_text(text, query_terms, case_sensitive=False):
    """Highlight search terms in text"""
    if not text or not query_terms:
        return text
    
    terms = query_terms.split()
    highlighted = text
    
    for term in terms:
        pattern = re.compile(re.escape(term), re.IGNORECASE if not case_sensitive else 0)
        highlighted = pattern.sub(lambda m: f'<span class="highlight">{m.group()}</span>', highlighted)
    
    return highlighted

# Execute search
if search_button or search_query:
    if search_query:
        with st.spinner("🔍 Searching emails..."):
            results_df = search_emails(search_query, limit)
            
            # Store in session state for export
            st.session_state.results_df = results_df
        
        # Display results
        if not results_df.empty:
            st.success(f"✅ Found {len(results_df)} results")
            
            # Summary statistics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Results", len(results_df))
            with col2:
                st.metric("Unique Senders", results_df['sender'].nunique())
            with col3:
                st.metric("Unique Recipients", results_df['recipient'].nunique())
            with col4:
                date_range = f"{results_df['date'].min()} to {results_df['date'].max()}"
                st.metric("Date Range", "")
                st.caption(date_range)
            
            st.markdown("---")
            
            # Display each result
            for idx, row in results_df.iterrows():
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.markdown(f"### {row['Subject']}")
                    with col2:
                        st.markdown(f"**Date:** {row['date']}")
                    
                    st.markdown(f"**From:** {row['sender']}")
                    st.markdown(f"**To:** {row['recipient']}")
                    
                    # Summary without highlighting for simplicity
                    st.markdown(f"**Body:** {row['Body'][:500]}...")  # Show first 500 chars
                    
                    st.caption(f"File: {row['filename']}")
                    
                    # Action buttons
                    col1, col2, col3 = st.columns([1, 1, 4])
                    with col1:
                        if st.button("📋 Copy ID", key=f"copy_{idx}"):
                            st.code(row['email_id'])
                    with col2:
                        view_full = st.button("🔗 View Full", key=f"view_{idx}")
                    
                    # Show full body if button clicked
                    if view_full:
                        with st.expander("Full Email Body", expanded=True):
                            st.text(row['Body'])
                    
                    st.markdown("---")
        else:
            st.warning("⚠️ No results found. Try different search terms or filters.")
    else:
        st.info("👆 Enter search terms above to begin")
else:
    # Show some helpful information
    st.info("💡 **Tips:**")
    st.markdown("""
    - Use multiple keywords to narrow results
    - Use filters in the sidebar for more precise searches
    - Click 'Advanced Search Options' for regex and exclusion patterns
    - Export results to CSV for further analysis
    """)
    
    # Show sample queries
    with st.expander("📝 Example Searches"):
        st.markdown("""
        - `meeting schedule` - Find emails about meetings
        - `contract payment` - Find financial discussions
        - `urgent deadline` - Find time-sensitive emails
        - Use date filters to focus on specific time periods
        - Use sender/recipient filters to track communications with specific people
        """)

# Footer
st.markdown("---")
st.caption("Email Search Tool • AFP DataViz")