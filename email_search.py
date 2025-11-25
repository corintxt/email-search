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

# Password protection
def check_password():
    """Returns `True` if the user has entered the correct password."""
    
    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets.get("APP_PASSWORD", os.getenv("APP_PASSWORD", "password123")):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.info("Please enter the password to access this application.")
        return False
    elif not st.session_state["password_correct"]:
        # Password incorrect, show input + error
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct
        return True

# Check password before showing app
if not check_password():
    st.stop()  # Don't continue if password is not correct

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
SUMMARY = st.secrets.get("SUMMARY", os.getenv("SUMMARY"))

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
st.title("Email Search Tool")
st.markdown(f"Target dataset: `{DATASET}.{TABLE}`")

# Sidebar for filters
with st.sidebar:
    st.header("Search Filters")
    
    # Results limit
    limit = st.slider("Max results", 10, 500, 100, 10)

    # Search type
    search_type = st.radio(
        "Search in:",
        ["All fields", "Subject", "Body"]  # "Summary" - not available in current table
    )
    
    # Category filter - fetch categories if summary table exists
    category_filter = None
    if SUMMARY:
        try:
            # Try to fetch unique categories from summary table
            categories_query = f"SELECT DISTINCT category FROM `{PROJECT_ID}.{DATASET}.{SUMMARY}` WHERE category IS NOT NULL ORDER BY category"
            categories_job = client.query(categories_query)
            categories_df = categories_job.to_dataframe()
            
            if not categories_df.empty:
                categories_list = ["All categories"] + categories_df['category'].tolist()
                selected_category = st.selectbox("Filter by category:", categories_list)
                if selected_category != "All categories":
                    category_filter = selected_category
        except Exception as e:
            # If query fails, don't show category filter
            pass
    
    # Sender/Recipient filters
    st.subheader("Email Filters")
    sender_filter = st.text_input("From (sender contains):", "")
    recipient_filter = st.text_input("To (recipient contains):", "")
    
    # Date range filter
    st.subheader("Date Range")
    use_date_filter = st.checkbox("Filter by date range")
    
    if use_date_filter:
        col1, col2 = st.columns(2)
        with col1:
            date_from = st.date_input("From")
        with col2:
            date_to = st.date_input("To")
    else:
        date_from = None
        date_to = None

    # Summary display option
    st.subheader("Display Options")
    show_summaries = st.checkbox("Show summary")
    
    # Check if summary table exists when toggled
    summary_table_exists = False
    if show_summaries:
        if not SUMMARY:
            st.warning("⚠️ Summary table name not configured")
            show_summaries = False
        else:
            try:
                # Check if summary table exists
                summary_check_query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET}.{SUMMARY}` LIMIT 1"
                check_job = client.query(summary_check_query)
                check_job.result()  # Wait for query to complete
                summary_table_exists = True
            except Exception as e:
                st.warning(f"⚠️ Summary table does not exist: `{DATASET}.{SUMMARY}`")
                show_summaries = False

    # Export option
    st.subheader("Export")
    if st.button("📥 Export Results to CSV"):
        if 'results_df' in st.session_state and not st.session_state.results_df.empty:
            csv = st.session_state.results_df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"email_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No results to export. Run a search first.")

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
def search_emails(query, limit, search_type, date_from, date_to, sender_filter, recipient_filter, show_summaries, summary_table, category_filter):
    """Execute BigQuery search with filters"""
    
    # Build WHERE clause based on search type
    query_params = []
    where_conditions = []
    
    # Table prefix for joins (use alias when joining)
    table_prefix = "e." if (show_summaries and summary_table) else ""
    
    # Add keyword search only if query is provided
    if query:
        # Determine which fields to search
        if search_type == "Subject":
            search_fields = [f"{table_prefix}Subject"]
        elif search_type == "Body":
            search_fields = [f"{table_prefix}Body"]
        # elif search_type == "Summary":  # Not available in current table
        #     search_fields = ["Summary"]
        else:  # All fields
            search_fields = [f"{table_prefix}Subject", f"{table_prefix}Body"]  # "Summary" - not available in current table
        
        # Split into keywords and search in selected fields
        keywords = query.split()
        keyword_conditions = []
        for i, keyword in enumerate(keywords):
            field_conditions = " OR ".join([
                f"LOWER({field}) LIKE LOWER(@keyword_{i})" for field in search_fields
            ])
            condition = f"({field_conditions})"
            keyword_conditions.append(condition)
            query_params.append(bigquery.ScalarQueryParameter(f"keyword_{i}", "STRING", f"%{keyword}%"))
        
        where_conditions.append(" AND ".join(keyword_conditions))
    
    # Sender filter
    if sender_filter:
        where_conditions.append(f"LOWER({table_prefix}`From`) LIKE LOWER(@sender)")
        query_params.append(bigquery.ScalarQueryParameter("sender", "STRING", f"%{sender_filter}%"))
    
    # Recipient filter
    if recipient_filter:
        where_conditions.append(f"LOWER({table_prefix}`To`) LIKE LOWER(@recipient)")
        query_params.append(bigquery.ScalarQueryParameter("recipient", "STRING", f"%{recipient_filter}%"))
    
    # Date filters
    if date_from:
        where_conditions.append(f"{table_prefix}Date_Sent >= @date_from")
        query_params.append(bigquery.ScalarQueryParameter("date_from", "DATE", date_from))
    
    if date_to:
        where_conditions.append(f"{table_prefix}Date_Sent <= @date_to")
        query_params.append(bigquery.ScalarQueryParameter("date_to", "DATE", date_to))
    
    # Category filter (only applies when joining with summary table)
    if category_filter and show_summaries and summary_table:
        where_conditions.append("s.category = @category")
        query_params.append(bigquery.ScalarQueryParameter("category", "STRING", category_filter))
    
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
    
    # Build query with optional summary join
    if show_summaries and summary_table:
        sql_query = f"""
        SELECT 
            e.id,
            e.Body,
            e.Subject,
            e.`From` as sender,
            e.`To` as recipient,
            e.Date_Sent as date,
            e.filename,
            s.summary,
            s.category
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}` e
        LEFT JOIN `{PROJECT_ID}.{DATASET}.{summary_table}` s
        ON e.id = s.id
        WHERE {where_clause}
        ORDER BY e.Date_Sent DESC
        LIMIT @limit
        """
    else:
        sql_query = f"""
        SELECT 
            id,
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
if search_button or search_query is not None:
    with st.spinner("🔍 Searching emails..."):
        results_df = search_emails(search_query, limit, search_type, date_from, date_to, sender_filter, recipient_filter, show_summaries and summary_table_exists, SUMMARY if show_summaries and summary_table_exists else None, category_filter)
        
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
                        # Highlight search terms in subject
                        highlighted_subject = highlight_text(row['Subject'], search_query if search_query else "")
                        st.markdown(f"##### {highlighted_subject}", unsafe_allow_html=True)
                    with col2:
                        st.markdown(f"**Date:** {row['date']}")
                    
                    st.markdown(f"**From:** {row['sender']}")
                    st.markdown(f"**To:** {row['recipient']}")
                    
                    # Compact view when showing summaries
                    if show_summaries and summary_table_exists and 'summary' in row and pd.notna(row['summary']) and row['summary']:
                        highlighted_summary = highlight_text(str(row['summary']), search_query if search_query else "")
                        st.markdown(f"*{highlighted_summary}*", unsafe_allow_html=True)
                    else:
                        # Show body preview with highlighted search terms only if not showing summaries
                        body_preview = row['Body'][:500] if len(row['Body']) > 500 else row['Body']
                        highlighted_body = highlight_text(body_preview, search_query if search_query else "")
                        st.markdown(f"**Body:** {highlighted_body}{'...' if len(row['Body']) > 500 else ''}", unsafe_allow_html=True)
                    
                    # Caption with category badge
                    # Add category badge if available (as first item)
                    if show_summaries and summary_table_exists and 'category' in row and pd.notna(row['category']) and row['category']:
                        category_html = f'<span style="background-color: #e8f4f8; color: #0066cc; padding: 3px 8px; border-radius: 3px; font-size: 0.85em; font-weight: 500;">{row["category"]}</span>'
                        caption_text = f"{category_html} • ID: {row['id']} • Source file: {row['filename']}"
                        st.caption(caption_text, unsafe_allow_html=True)
                    else:
                        st.caption(f"ID: {row['id']} • Source file: {row['filename']}")
                    
                    # Action button
                    view_full = st.button("🔗 View Full", key=f"view_{idx}")
                    
                    # Show full body if button clicked
                    if view_full:
                        with st.expander("Full Email Body", expanded=True):
                            highlighted_full_body = highlight_text(row['Body'], search_query if search_query else "")
                            st.markdown(highlighted_full_body, unsafe_allow_html=True)
                    
                    st.markdown("---")
                
    else:
        st.warning("⚠️ No results found. Try different search terms or filters.")
else:
    # Show some helpful information
    st.info("**Tips:**")
    st.markdown("""
    - Use multiple keywords to narrow results, e.g. `girls island`
    - Use filters in the sidebar for more precise searches
    - Export results to CSV for further analysis
    """)

# Footer
st.markdown("---")
st.caption("Email Search Tool • An AFP DataViz project")