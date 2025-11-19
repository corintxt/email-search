# email_search_app.py
import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import re

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
    return bigquery.Client()

client = get_bigquery_client()

# Configuration
PROJECT_ID = "your-project"
DATASET = "your-dataset"
TABLE = "emails"

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
st.title("📧 Email Search Interface")
st.markdown("Search through email summaries and metadata")

# Sidebar for filters
with st.sidebar:
    st.header("Search Filters")
    
    # Search type
    search_type = st.radio(
        "Search in:",
        ["Summary", "Subject", "Both", "All fields"]
    )
    
    # Date range
    st.subheader("Date Range")
    date_option = st.selectbox(
        "Quick select:",
        ["All time", "Last 7 days", "Last 30 days", "Last 90 days", "Custom"]
    )
    
    if date_option == "Custom":
        date_from = st.date_input("From", value=None)
        date_to = st.date_input("To", value=None)
    elif date_option != "All time":
        days = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90}[date_option]
        date_from = datetime.now().date() - timedelta(days=days)
        date_to = datetime.now().date()
    else:
        date_from = None
        date_to = None
    
    # Sender filter
    sender_filter = st.text_input("Sender email contains:", "")
    
    # Recipient filter
    recipient_filter = st.text_input("Recipient email contains:", "")
    
    # Case sensitivity
    case_sensitive = st.checkbox("Case sensitive search")
    
    # Results limit
    limit = st.slider("Max results", 10, 500, 100, 10)
    
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

# Advanced search toggle
with st.expander("🔧 Advanced Search Options"):
    col1, col2 = st.columns(2)
    with col1:
        exact_phrase = st.checkbox("Exact phrase match")
        exclude_terms = st.text_input("Exclude terms (comma-separated):", "")
    with col2:
        use_regex = st.checkbox("Use regex pattern")
        sort_order = st.selectbox("Sort by:", ["Date (newest)", "Date (oldest)", "Relevance"])

# Search function with caching
@st.cache_data(ttl=300)  # Cache for 5 minutes
def search_emails(query, search_type, date_from, date_to, sender, recipient, 
                  case_sensitive, limit, exact_phrase, exclude_terms):
    """Execute BigQuery search with filters"""
    
    if not query:
        return pd.DataFrame()
    
    # Build WHERE clause
    where_conditions = []
    query_params = []
    
    # Search field logic
    if search_type == "Summary":
        search_fields = ["summary"]
    elif search_type == "Subject":
        search_fields = ["subject"]
    elif search_type == "Both":
        search_fields = ["summary", "subject"]
    else:  # All fields
        search_fields = ["summary", "subject", "sender", "recipient"]
    
    # Build search condition
    if exact_phrase:
        search_condition = " OR ".join([
            f"{'LOWER(' if not case_sensitive else ''}{field}{')}' if not case_sensitive else ''} LIKE {'LOWER(' if not case_sensitive else ''}@search_term{')' if not case_sensitive else ''}"
            for field in search_fields
        ])
        query_params.append(bigquery.ScalarQueryParameter("search_term", "STRING", f"%{query}%"))
    else:
        # Split into keywords
        keywords = query.split()
        keyword_conditions = []
        for i, keyword in enumerate(keywords):
            field_conditions = " OR ".join([
                f"{'LOWER(' if not case_sensitive else ''}{field}{')}' if not case_sensitive else ''} LIKE {'LOWER(' if not case_sensitive else ''}@keyword_{i}{')' if not case_sensitive else ''}"
                for field in search_fields
            ])
            keyword_conditions.append(f"({field_conditions})")
            query_params.append(bigquery.ScalarQueryParameter(f"keyword_{i}", "STRING", f"%{keyword}%"))
        
        search_condition = " AND ".join(keyword_conditions)
    
    where_conditions.append(f"({search_condition})")
    
    # Exclude terms
    if exclude_terms:
        excluded = [term.strip() for term in exclude_terms.split(",")]
        for i, term in enumerate(excluded):
            exclude_condition = " AND ".join([
                f"{'LOWER(' if not case_sensitive else ''}{field}{')}' if not case_sensitive else ''} NOT LIKE {'LOWER(' if not case_sensitive else ''}@exclude_{i}{')' if not case_sensitive else ''}"
                for field in search_fields
            ])
            where_conditions.append(f"({exclude_condition})")
            query_params.append(bigquery.ScalarQueryParameter(f"exclude_{i}", "STRING", f"%{term}%"))
    
    # Date filters
    if date_from:
        where_conditions.append("date >= @date_from")
        query_params.append(bigquery.ScalarQueryParameter("date_from", "DATE", date_from))
    
    if date_to:
        where_conditions.append("date <= @date_to")
        query_params.append(bigquery.ScalarQueryParameter("date_to", "DATE", date_to))
    
    # Sender filter
    if sender:
        where_conditions.append("LOWER(sender) LIKE LOWER(@sender)")
        query_params.append(bigquery.ScalarQueryParameter("sender", "STRING", f"%{sender}%"))
    
    # Recipient filter
    if recipient:
        where_conditions.append("LOWER(recipient) LIKE LOWER(@recipient)")
        query_params.append(bigquery.ScalarQueryParameter("recipient", "STRING", f"%{recipient}%"))
    
    # Build full query
    sql_query = f"""
    SELECT 
        email_id,
        summary,
        subject,
        sender,
        recipient,
        date,
        -- Add relevance scoring
        (
            CASE WHEN LOWER(subject) LIKE LOWER(@search_term) THEN 3 ELSE 0 END +
            CASE WHEN LOWER(summary) LIKE LOWER(@search_term) THEN 2 ELSE 0 END
        ) as relevance_score
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE {' AND '.join(where_conditions)}
    ORDER BY {"relevance_score DESC, " if sort_order == "Relevance" else ""}
             date {"DESC" if "newest" in sort_order else "ASC"}
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
            results_df = search_emails(
                search_query, 
                search_type, 
                date_from, 
                date_to,
                sender_filter,
                recipient_filter,
                case_sensitive,
                limit,
                exact_phrase,
                exclude_terms
            )
            
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
                        st.markdown(f"### {row['subject']}")
                    with col2:
                        st.markdown(f"**Date:** {row['date']}")
                    
                    st.markdown(f"**From:** {row['sender']}")
                    st.markdown(f"**To:** {row['recipient']}")
                    
                    # Highlighted summary
                    highlighted_summary = highlight_text(
                        row['summary'], 
                        search_query, 
                        case_sensitive
                    )
                    st.markdown(f"**Summary:** {highlighted_summary}", unsafe_allow_html=True)
                    
                    st.caption(f"Email ID: {row['email_id']}")
                    
                    # Action buttons
                    col1, col2, col3 = st.columns([1, 1, 4])
                    with col1:
                        if st.button("📋 Copy ID", key=f"copy_{idx}"):
                            st.code(row['email_id'])
                    with col2:
                        if st.button("🔗 View Full", key=f"view_{idx}"):
                            st.info(f"Open email {row['email_id']} in your email system")
                    
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
st.caption("Email Search Tool • Built with Streamlit")