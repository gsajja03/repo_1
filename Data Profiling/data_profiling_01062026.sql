import streamlit as st
import pandas as pd
import snowflake.snowpark as sp
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Data Profiling Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def get_session():
    return get_active_session()

session = get_session()



st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .profile-section {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
        border: 1px solid #dee2e6;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">📊 Data Profiling Dashboard</div>', unsafe_allow_html=True)
st.markdown("---")


@st.cache_data(ttl=300)
def get_available_roles():
    """Fetch roles available to current user"""
    try:
        query = "SELECT VALUE AS ROLE_NAME FROM TABLE(SPLIT_TO_TABLE('CDP_TST_DEVELOPER_FR,CDP_DEV_DEVELOPER_FR',','));"
        result = session.sql(query).collect()
        # Filter to roles the user can actually use
        roles = [row['ROLE_NAME'] for row in result]
        return sorted(roles)
    except Exception as e:
        st.error(f"Error fetching roles: {str(e)}")
        return []

def get_current_role():
    """Get the currently active role"""
    try:
        result = session.sql("SELECT CURRENT_ROLE()").collect()
        return result[0][0] if result else None
    except Exception as e:
        st.error(f"Error getting current role: {str(e)}")
        return None

def switch_role(role_name):
    """Switch to a different role"""
    try:
        session.sql(f"USE ROLE {role_name}").collect()
        # Clear all caches when role changes
        st.cache_data.clear()
        return True, f"Successfully switched to role: {role_name}"
    except Exception as e:
        return False, f"Error switching role: {str(e)}"

    
@st.cache_data(ttl=300)
def get_databases():
    """Fetch available databases"""
    try:
        query = """
            SELECT database_name 
            FROM INFORMATION_SCHEMA.DATABASES
            ORDER BY database_name
        """
        result = session.sql(query).collect()
        return [row['DATABASE_NAME'] for row in result]
    except Exception as e:
        st.error(f"Error fetching databases: {str(e)}")
        return []

@st.cache_data(ttl=300)
def get_schemas(database):
    """Fetch schemas for selected database"""
    try:
        query = f"""
            SELECT schema_name 
            FROM {database}.INFORMATION_SCHEMA.SCHEMATA
            WHERE schema_name NOT IN ('INFORMATION_SCHEMA')
            ORDER BY schema_name
        """
        result = session.sql(query).collect()
        return [row['SCHEMA_NAME'] for row in result]
    except Exception as e:
        st.error(f"Error fetching schemas: {str(e)}")
        return []

@st.cache_data(ttl=300)
def get_tables(database, schema):
    query = f"""
        SELECT table_name 
        FROM {database}.INFORMATION_SCHEMA.TABLES 
        WHERE table_schema = '{schema}' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """
    result = session.sql(query).collect()
    return [row['TABLE_NAME'] for row in result]

@st.cache_data(ttl=60)
def check_profile_table_exists(database, schema):
    query = f"""
        SELECT COUNT(*) as cnt
        FROM TST_DATAMART_DB.STREAMLIT.COLUMN_PROFILE_RESULT
        WHERE DATABASE_NAME = '{database}' AND SCHEMA_NAME = '{schema}'
        
    """
    try:
        result = session.sql(query).collect()
        return result[0]['CNT'] > 0
    except:
        return False

@st.cache_data(ttl=60)
def get_profile_runs(database, schema):
    query = f"""
        SELECT DISTINCT 
            PROFILE_RUN_ID,
            MAX(PROFILE_TIMESTAMP) as RUN_TIMESTAMP,
            LISTAGG(DISTINCT TABLE_NAME, ', ') WITHIN GROUP (ORDER BY TABLE_NAME) as TABLES_PROFILED,
            COUNT(DISTINCT TABLE_NAME) as TABLE_COUNT
        FROM TST_DATAMART_DB.STREAMLIT.COLUMN_PROFILE_RESULT
        GROUP BY PROFILE_RUN_ID
        ORDER BY RUN_TIMESTAMP DESC
    """
    result = session.sql(query).collect()
    return [(row['PROFILE_RUN_ID'], row['RUN_TIMESTAMP'], row['TABLE_COUNT'], row['TABLES_PROFILED']) for row in result]

@st.cache_data(ttl=60)
def get_profile_data(database, schema, run_id, table_filter=None):
    where_clause = f"AND TABLE_NAME = '{table_filter}'" if table_filter else ""

    query = f"""
        SELECT *
        FROM TST_DATAMART_DB.STREAMLIT.COLUMN_PROFILE_RESULT
        WHERE PROFILE_RUN_ID = '{run_id}'
        {where_clause}
        ORDER BY TABLE_NAME, COLUMN_NAME
    """
    result = session.sql(query).collect()
    return pd.DataFrame([row.as_dict() for row in result])

def run_profiling_all_tables(database, schema):
    query = f"CALL TST_DATAMART_DB.STREAMLIT.PROFILE_SCHEMA_COLUMNS_PERSISTENT_SP('{schema}', '{database}')"
    result = session.sql(query).collect()
    return result[0][0]

def run_profiling_single_table(database, schema, table_name):
    run_id = session.sql("SELECT UUID_STRING()").collect()[0][0]

    query = f"""
        SELECT column_name, data_type
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """
    columns = session.sql(query).collect()

    
    columns_processed = 0
    for col in columns:
        column_name = col['COLUMN_NAME']
        data_type = col['DATA_TYPE']

        profile_sql = f"""
            INSERT INTO TST_DATAMART_DB.STREAMLIT.COLUMN_PROFILE_RESULT
            SELECT 
                 '{run_id}' AS PROFILE_RUN_ID,
                 '{database}' AS DATABASE_NAME,
                 '{schema}' AS SCHEMA_NAME,
                 '{table_name}' AS TABLE_NAME,
                '{column_name}' AS COLUMN_NAME,
                '{data_type}' AS DATA_TYPE,
                COUNT(*) AS TOTAL_ROWS,
                COUNT_IF("{column_name}" IS NULL) AS NULL_COUNT,
                COUNT_IF("{column_name}" IS NOT NULL) AS NOT_NULL_COUNT,
                ROUND((COUNT_IF("{column_name}" IS NULL) / NULLIF(COUNT(*), 0)) * 100, 2) AS NULL_PERCENTAGE,
                COUNT(DISTINCT "{column_name}") AS DISTINCT_COUNT,
                ROUND((COUNT(DISTINCT "{column_name}") / NULLIF(COUNT_IF("{column_name}" IS NOT NULL), 0)) * 100, 2) AS DISTINCT_PERCENTAGE,
                TO_VARCHAR(MIN("{column_name}")) AS MIN_VALUE,
                TO_VARCHAR(MAX("{column_name}")) AS MAX_VALUE,
                CURRENT_TIMESTAMP() AS PROFILE_TIMESTAMP
            FROM {database}.{schema}.{table_name}
        """
        session.sql(profile_sql).collect()
        columns_processed += 1

    return f"Profiling completed for table '{table_name}'. Run ID: {run_id}. Columns processed: {columns_processed}"
    

def check_permissions(database, schema):
    try:
        query = f"""
            SELECT COUNT(*) as cnt
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE table_schema = '{schema}'
            LIMIT 1
        """
        session.sql(query).collect()
        return True, "Permissions verified"
    except Exception as e:
        return False, f"Permission error: {str(e)}"
######################################################

with st.sidebar:
    st.header("🔍 Configuration")

    # ============================================
    # ROLE SELECTION SECTION
    # ============================================
    st.subheader("👤 Role Selection")

    current_role = get_current_role()

    if current_role:
        st.info(f"Current Role: **{current_role}**")

    available_roles = get_available_roles()

    if available_roles:
        # Find index of current role, default to 0 if not found
        try:
            current_role_index = available_roles.index(current_role) if current_role in available_roles else 0
        except ValueError:
            current_role_index = 0

        selected_role = st.selectbox(
            "Select Role",
            options=available_roles,
            index=current_role_index,
            key="role_select",
            help="Select a role to use for database operations"
        )

        # Show role switch button if different from current
        if selected_role != current_role:
            if st.button("🔄 Switch Role", type="primary", key="switch_role_btn"):
                with st.spinner(f"Switching to role {selected_role}..."):
                    success, message = switch_role(selected_role)
                    if success:
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)

        # Show role information
        with st.expander("ℹ️ Role Information"):
            st.markdown(f"""
            **Selected Role:** `{selected_role}`

            **Note:** Changing roles will:
            - Clear all cached data
            - Refresh available databases and schemas
            - May affect permissions for profiling operations
            """)
    else:
        st.warning("No roles available or unable to fetch roles")

    st.markdown("---")

    # ============================================
    # DATABASE & SCHEMA SELECTION
    # ============================================
    st.subheader("🗄️ Database & Schema")
      
  
    try:
        
        databases = get_databases()
        if databases:
            selected_database = st.selectbox(
                "Select Database",
                options=databases,
                index=0,
                key="db_select"
            )
        else:
            selected_database = None
            st.warning("No databases accessible")
    except Exception as e:
        selected_database = None
        st.error(f"Error fetching databases: {str(e)}")

    if selected_database:
        try:
            schemas = get_schemas(selected_database)
            if schemas:
                selected_schema = st.selectbox(
                    "Select Schema",
                    options=schemas,
                    index=0,
                    key="schema_select"
                )
            else:
                selected_schema = None
                st.warning("No schemas accessible in selected database")
        except Exception as e:
            selected_schema = None
            st.error(f"Error fetching schemas: {str(e)}")
    else:
        selected_schema = None

    profile_exists = False
    if selected_database and selected_schema:
        profile_exists = check_profile_table_exists(selected_database, selected_schema)

    st.markdown("---")

    st.subheader("⚙️ Run Profiling")

    if selected_database and selected_schema:
        has_permissions, _ = check_permissions(selected_database, selected_schema)

        if has_permissions:
            profile_scope = st.radio(
                "Profile Scope",
                options=["All Tables in Schema", "Single Table"],
                key="profile_scope"
            )

            selected_profile_table = None
            if profile_scope == "Single Table":
                try:
                    available_tables = get_tables(selected_database, selected_schema)
                    if available_tables:
                        selected_profile_table = st.selectbox(
                            "Select Table to Profile",
                            options=available_tables,
                            key="profile_table_select"
                        )
                    else:
                        st.warning("No tables found in selected schema")
                except Exception as e:
                    st.error(f"Error fetching tables: {str(e)}")

            st.markdown("---")

            if profile_scope == "All Tables in Schema":
                button_label = "🔄 Profile All Tables"
                button_help = "Profile all tables in the selected schema"
            else:
                button_label = f"🔄 Profile '{selected_profile_table}'" if selected_profile_table else "🔄 Profile Table"
                button_help = f"Profile only the selected table: {selected_profile_table}" if selected_profile_table else "Select a table first"

            run_button_disabled = not (selected_database and selected_schema)
            if profile_scope == "Single Table":
                run_button_disabled = run_button_disabled or not selected_profile_table

            if st.button(
                button_label,
                type="primary",
                disabled=run_button_disabled,
                help=button_help,
                key="run_profile_btn"
            ):
                with st.spinner(f"Running profiling... This may take a few minutes."):
                    try:
                        if profile_scope == "All Tables in Schema":
                            result = run_profiling_all_tables(selected_database, selected_schema)
                        else:
                            result = run_profiling_single_table(selected_database, selected_schema, selected_profile_table)

                        st.success(f"✅ {result}")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
                        st.exception(e)
        else:
            st.error("⚠️ Insufficient permissions.")
    else:
        st.info("Select database and schema to enable profiling")

    if not profile_exists:
        st.info("ℹ️ No profile data found. Run profiling first.")

    st.markdown("---")

    if profile_exists and selected_database and selected_schema:
        st.subheader("📅 Profile Run")
        try:
            profile_runs = get_profile_runs(selected_database, selected_schema)

            if profile_runs:
                run_options = [
                    f"{run[1].strftime('%Y-%m-%d %H:%M:%S')} ({run[2]} table{'s' if run[2] > 1 else ''})" 
                    for run in profile_runs
                ]

                selected_run_idx = st.selectbox(
                    "Select Profile Run",
                    options=range(len(run_options)),
                    format_func=lambda x: run_options[x],
                    index=0,
                    key="run_select"
                )
                selected_run_id = profile_runs[selected_run_idx][0]

                with st.expander("📋 Tables in this run"):
                    tables_profiled = profile_runs[selected_run_idx][3]
                    if len(tables_profiled) > 100:
                        st.text(tables_profiled[:100] + "...")
                    else:
                        st.text(tables_profiled)
            else:
                selected_run_id = None
                st.warning("No profile runs found")
        except Exception as e:
            selected_run_id = None
            st.error(f"Error fetching profile runs: {str(e)}")
    else:
        selected_run_id = None

    if selected_database and selected_schema and selected_run_id:
        st.markdown("---")
        st.subheader("🗂️ View Filter")

        try:
            profile_data_preview = get_profile_data(selected_database, selected_schema, selected_run_id)
            available_tables = sorted(profile_data_preview['TABLE_NAME'].unique().tolist())

            table_filter_options = ["All Tables"] + available_tables
            selected_table = st.selectbox(
                "Filter by Table",
                options=table_filter_options,
                index=0,
                key="table_filter"
            )

            if selected_table == "All Tables":
                selected_table = None
        except Exception as e:
            selected_table = None
            st.error(f"Error loading table filter: {str(e)}")

if not (selected_database and selected_schema and profile_exists and selected_run_id):
    st.info("👈 Please select a database and schema from the sidebar, then run profiling or select an existing profile run.")

    with st.expander("📖 Getting Started Guide", expanded=True):
        st.markdown("""
        ### How to Use This Dashboard

        **Steps:**

        1. **Select Database & Schema**
           - Choose from available databases
           - Select target schema

        2. **Choose Profiling Scope**
           - **All Tables in Schema**: Profile every table
           - **Single Table**: Profile specific table

        3. **Run Profiling**
           - Click the profile button
           - Wait for completion

        4. **View Results**
           - Select a profile run
           - Explore different tabs
           - Filter by table if needed

        ### What Gets Profiled?

        For each column:
        - ✅ Total row count
        - ✅ Null and non-null counts
        - ✅ Null percentage
        - ✅ Distinct value count
        - ✅ Distinct percentage
        - ✅ Minimum value
        - ✅ Maximum value

        ### Permissions Required

        Your role needs:
        - `USAGE` on database and schema
        - `SELECT` on tables to profile
        - `INSERT` to save profile data

        ### Tips

        - 💡 Profile single tables for quick analysis
        - 💡 Profile all tables for schema overview
        - 💡 Compare runs to track changes
        """)

    st.stop()

try:
    df = get_profile_data(selected_database, selected_schema, selected_run_id, selected_table)

    if df.empty:
        st.warning("No profile data available for the selected filters.")
        st.stop()
except Exception as e:
    st.error(f"Error loading profile data: {str(e)}")
    st.stop()

st.header("📈 Key Metrics")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    total_tables = df['TABLE_NAME'].nunique()
    st.metric("Total Tables", total_tables)

with col2:
    total_columns = len(df)
    st.metric("Total Columns", total_columns)

with col3:
    avg_null_pct = df['NULL_PERCENTAGE'].mean()
    st.metric("Avg Null %", f"{avg_null_pct:.2f}%")

with col4:
    high_null_cols = len(df[df['NULL_PERCENTAGE'] > 50])
    st.metric("High Null Columns", high_null_cols, delta=f"{(high_null_cols/total_columns*100):.1f}%")

with col5:
    low_cardinality = len(df[df['DISTINCT_PERCENTAGE'] < 5])
    st.metric("Low Cardinality", low_cardinality, delta=f"{(low_cardinality/total_columns*100):.1f}%")

st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🔍 Detailed Analysis", "📋 Data Quality", "📑 Raw Data"])

with tab1:
    st.subheader("Overview Dashboard")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Null Percentage Distribution")
        fig_null_dist = px.histogram(
            df,
            x='NULL_PERCENTAGE',
            nbins=20,
            title='Distribution of Null Percentages',
            labels={'NULL_PERCENTAGE': 'Null Percentage', 'count': 'Number of Columns'},
            color_discrete_sequence=['#1f77b4']
        )
        fig_null_dist.update_layout(height=400)
        st.plotly_chart(fig_null_dist, use_container_width=True)

    with col2:
        st.markdown("#### Data Type Distribution")
        data_type_counts = df['DATA_TYPE'].value_counts().reset_index()
        data_type_counts.columns = ['DATA_TYPE', 'COUNT']

        fig_data_types = px.pie(
            data_type_counts,
            values='COUNT',
            names='DATA_TYPE',
            title='Column Data Types',
            hole=0.4
        )
        fig_data_types.update_layout(height=400)
        st.plotly_chart(fig_data_types, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Top Tables by Column Count")
        table_col_counts = df.groupby('TABLE_NAME').size().reset_index(name='COLUMN_COUNT')
        table_col_counts = table_col_counts.sort_values('COLUMN_COUNT', ascending=False).head(10)

        fig_table_cols = px.bar(
            table_col_counts,
            x='COLUMN_COUNT',
            y='TABLE_NAME',
            orientation='h',
            title='Tables with Most Columns (Top 10)',
            labels={'COLUMN_COUNT': 'Number of Columns', 'TABLE_NAME': 'Table Name'},
            color='COLUMN_COUNT',
            color_continuous_scale='Blues'
        )
        fig_table_cols.update_layout(height=400)
        st.plotly_chart(fig_table_cols, use_container_width=True)

    with col2:
        st.markdown("#### Distinct Value Distribution")
        fig_distinct = px.histogram(
            df,
            x='DISTINCT_PERCENTAGE',
            nbins=20,
            title='Distribution of Distinct Value Percentages',
            labels={'DISTINCT_PERCENTAGE': 'Distinct %', 'count': 'Number of Columns'},
            color_discrete_sequence=['#2ca02c']
        )
        fig_distinct.update_layout(height=400)
        st.plotly_chart(fig_distinct, use_container_width=True)

with tab2:
    st.subheader("Detailed Column Analysis")

    if selected_table is None:
        detail_tables = df['TABLE_NAME'].unique()
        detail_table = st.selectbox("Select Table for Detailed View", options=detail_tables, key="detail_table")
    else:
        detail_table = selected_table

    table_df = df[df['TABLE_NAME'] == detail_table].copy()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Columns", len(table_df))
    with col2:
        st.metric("Avg Null %", f"{table_df['NULL_PERCENTAGE'].mean():.2f}%")
    with col3:
        st.metric("Total Rows", f"{table_df['TOTAL_ROWS'].iloc[0]:,}" if len(table_df) > 0 else "N/A")
    with col4:
        st.metric("Avg Distinct %", f"{table_df['DISTINCT_PERCENTAGE'].mean():.2f}%")

    st.markdown("---")

    st.markdown("#### Column Quality Heatmap")

    heatmap_data = table_df[['COLUMN_NAME', 'NULL_PERCENTAGE', 'DISTINCT_PERCENTAGE']].copy()
    heatmap_data['COMPLETENESS'] = 100 - heatmap_data['NULL_PERCENTAGE']

    fig_heatmap = go.Figure(data=go.Heatmap(
        z=[heatmap_data['COMPLETENESS'].values, heatmap_data['DISTINCT_PERCENTAGE'].values],
        x=heatmap_data['COLUMN_NAME'].values,
        y=['Completeness %', 'Distinct %'],
        colorscale='RdYlGn',
        text=[[f"{v:.1f}%" for v in heatmap_data['COMPLETENESS'].values],
              [f"{v:.1f}%" for v in heatmap_data['DISTINCT_PERCENTAGE'].values]],
        texttemplate='%{text}',
        textfont={"size": 10},
        colorbar=dict(title="Percentage")
    ))

    fig_heatmap.update_layout(
        title=f'Data Quality Metrics for {detail_table}',
        xaxis_title='Column Name',
        height=300,
        xaxis={'tickangle': -45}
    )

    st.plotly_chart(fig_heatmap, use_container_width=True)

    st.markdown("#### Column Details")

    display_df = table_df[[
        'COLUMN_NAME', 'DATA_TYPE', 'TOTAL_ROWS', 'NULL_COUNT', 
        'NULL_PERCENTAGE', 'DISTINCT_COUNT', 'DISTINCT_PERCENTAGE',
        'MIN_VALUE', 'MAX_VALUE'
    ]].copy()

    display_df['NULL_PERCENTAGE'] = display_df['NULL_PERCENTAGE'].apply(lambda x: f"{x:.2f}%")
    display_df['DISTINCT_PERCENTAGE'] = display_df['DISTINCT_PERCENTAGE'].apply(lambda x: f"{x:.2f}%")

    st.dataframe(
        display_df,
        use_container_width=True,
        height=400,
        hide_index=True
    )

with tab3:
    st.subheader("Data Quality Issues")

    col1, col2 = st.columns(2)
    with col1:
        null_threshold = st.slider("Null Percentage Threshold", 0, 100, 50, 5, key="null_threshold")
    with col2:
        distinct_threshold = st.slider("Low Distinct Percentage Threshold", 0, 100, 5, 1, key="distinct_threshold")

    st.markdown("---")

    st.markdown(f"#### ⚠️ Columns with >{null_threshold}% Nulls")
    high_null_df = df[df['NULL_PERCENTAGE'] > null_threshold][
        ['TABLE_NAME', 'COLUMN_NAME', 'DATA_TYPE', 'NULL_PERCENTAGE', 'TOTAL_ROWS']
    ].sort_values('NULL_PERCENTAGE', ascending=False)

    if not high_null_df.empty:
        st.dataframe(high_null_df, use_container_width=True, hide_index=True)

        fig_high_null = px.bar(
            high_null_df.head(20),
            x='NULL_PERCENTAGE',
            y='COLUMN_NAME',
            color='TABLE_NAME',
            orientation='h',
            title=f'Top 20 Columns with Highest Null Percentage (>{null_threshold}%)',
            labels={'NULL_PERCENTAGE': 'Null %', 'COLUMN_NAME': 'Column'}
        )
        fig_high_null.update_layout(height=500)
        st.plotly_chart(fig_high_null, use_container_width=True)
    else:
        st.success(f"✅ No columns found with >{null_threshold}% nulls")

    st.markdown("---")

    st.markdown(f"#### 🔍 Low Cardinality Columns (<{distinct_threshold}% Distinct)")
    low_card_df = df[
        (df['DISTINCT_PERCENTAGE'] < distinct_threshold) & 
        (df['TOTAL_ROWS'] > 100)
    ][
        ['TABLE_NAME', 'COLUMN_NAME', 'DATA_TYPE', 'DISTINCT_COUNT', 'DISTINCT_PERCENTAGE', 'TOTAL_ROWS']
    ].sort_values('DISTINCT_PERCENTAGE')

    if not low_card_df.empty:
        st.dataframe(low_card_df, use_container_width=True, hide_index=True)

        st.info("💡 Low cardinality columns may be good candidates for clustering keys or optimization")
    else:
        st.success(f"✅ No low cardinality columns found (<{distinct_threshold}% distinct)")

    st.markdown("---")

    st.markdown("#### ❌ Completely Null Columns (100% Null)")
    completely_null_df = df[df['NULL_PERCENTAGE'] == 100][
        ['TABLE_NAME', 'COLUMN_NAME', 'DATA_TYPE', 'TOTAL_ROWS']
    ]

    if not completely_null_df.empty:
        st.dataframe(completely_null_df, use_container_width=True, hide_index=True)
        st.warning("⚠️ Consider removing these columns or investigating why they contain no data")
    else:
        st.success("✅ No completely null columns found")

with tab4:
    st.subheader("Raw Profile Data")

    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Download Profile Data as CSV",
        data=csv,
        file_name=f"profile_data_{selected_database}_{selected_schema}_{selected_run_id[:8]}.csv",
        mime="text/csv",
        key="download_csv"
    )

    st.markdown("---")

    st.markdown("#### Filter and Search")

    col1, col2, col3 = st.columns(3)

    with col1:
        search_column = st.text_input("Search Column Name", "", key="search_col")
    with col2:
        filter_table = st.multiselect(
            "Filter by Table",
            options=df['TABLE_NAME'].unique(),
            default=None,
            key="filter_table_multi"
        )
    with col3:
        filter_data_type = st.multiselect(
            "Filter by Data Type",
            options=df['DATA_TYPE'].unique(),
            default=None,
            key="filter_dtype"
        )

    filtered_df = df.copy()

    if search_column:
        filtered_df = filtered_df[filtered_df['COLUMN_NAME'].str.contains(search_column, case=False, na=False)]

    if filter_table:
        filtered_df = filtered_df[filtered_df['TABLE_NAME'].isin(filter_table)]

    if filter_data_type:
        filtered_df = filtered_df[filtered_df['DATA_TYPE'].isin(filter_data_type)]

    st.markdown(f"**Showing {len(filtered_df)} of {len(df)} rows**")

    st.dataframe(
        filtered_df,
        use_container_width=True,
        height=600,
        hide_index=True
    )

st.markdown("---")
st.markdown(f"""
    <div style='text-align: center; color: #666; padding: 1rem;'>
        <p>Data Profiling Dashboard | Powered by Snowflake & Streamlit</p>
        <p style='font-size: 0.8rem;'>Database: {selected_database} | Schema: {selected_schema} | Run ID: {selected_run_id[:8] if selected_run_id else 'N/A'}</p>
    </div>
""", unsafe_allow_html=True)