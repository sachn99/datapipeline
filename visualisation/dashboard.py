import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine
import datetime

# Connect to PostgreSQL
engine = create_engine('postgresql://sachin:sachin@localhost:5432/noora')

app = dash.Dash(__name__)

app.layout = html.Div(children=[
    html.H1(children='Noora Health WhatsApp Engagement Dashboard', style={'textAlign': 'center'}),

    html.Div(children='''Analysis of active and engaged users, and message status distribution.''', style={'textAlign': 'center', 'marginBottom': '20px'}),

    html.Div([
        html.Div([
            html.H3('Total Active Users'),
            html.Div(id='total-active-users', style={'fontSize': '24px', 'fontWeight': 'bold'}),
        ], style={'width': '48%', 'display': 'inline-block', 'padding': '20px', 'textAlign': 'center', 'border': '1px solid #ddd', 'borderRadius': '10px'}),

        html.Div([
            html.H3('Total Engaged Users'),
            html.Div(id='total-engaged-users', style={'fontSize': '24px', 'fontWeight': 'bold'}),
        ], style={'width': '48%', 'display': 'inline-block', 'padding': '20px', 'textAlign': 'center', 'border': '1px solid #ddd', 'borderRadius': '10px'}),
    ], style={'display': 'flex', 'justifyContent': 'space-between', 'marginBottom': '20px'}),

    html.Div([
        html.Div([
            html.Label('Select Date Range for Active Users Analysis:'),
            dcc.DatePickerRange(
                id='active-users-date-picker',
                start_date=datetime.datetime.now() - datetime.timedelta(days=30),
                end_date=datetime.datetime.now(),
                display_format='YYYY-MM-DD'
            ),
            dcc.Graph(id='active-users'),
        ], style={'width': '48%', 'display': 'inline-block', 'padding': '20px'}),

        html.Div([
            html.Label('Select Date Range for Engaged Users Analysis:'),
            dcc.DatePickerRange(
                id='engaged-users-date-picker',
                start_date=datetime.datetime.now() - datetime.timedelta(days=30),
                end_date=datetime.datetime.now(),
                display_format='YYYY-MM-DD'
            ),
            dcc.Graph(id='engaged-users'),
        ], style={'width': '48%', 'display': 'inline-block', 'padding': '20px'}),
    ], style={'display': 'flex', 'justifyContent': 'space-between'}),

    html.Div([
        html.Label('Select Date Range for Status Summary Analysis:'),
        dcc.DatePickerRange(
            id='status-summary-date-picker',
            start_date=datetime.datetime.now() - datetime.timedelta(days=7),
            end_date=datetime.datetime.now(),
            display_format='YYYY-MM-DD'
        ),
        dcc.Graph(id='status-summary'),
    ], style={'padding': '20px'}),

    html.Div([
        html.Label('Enter User ID and Select Date Range for User Messages Analysis:'),
        dcc.Input(id='user-id-input', type='text', placeholder='Enter User ID', style={'marginRight': '10px'}),
        dcc.DatePickerRange(
            id='user-messages-date-picker',
            start_date=datetime.datetime.now() - datetime.timedelta(days=30),
            end_date=datetime.datetime.now(),
            display_format='YYYY-MM-DD'
        ),
        dcc.Graph(id='user-messages-status'),
    ], style={'padding': '20px'}),
])

@app.callback(
    [Output('total-active-users', 'children'),
     Output('total-engaged-users', 'children'),
     Output('active-users', 'figure'),
     Output('engaged-users', 'figure'),
     Output('status-summary', 'figure'),
     Output('user-messages-status', 'figure')],
    [Input('active-users-date-picker', 'start_date'),
     Input('active-users-date-picker', 'end_date'),
     Input('engaged-users-date-picker', 'start_date'),
     Input('engaged-users-date-picker', 'end_date'),
     Input('status-summary-date-picker', 'start_date'),
     Input('status-summary-date-picker', 'end_date'),
     Input('user-id-input', 'value'),
     Input('user-messages-date-picker', 'start_date'),
     Input('user-messages-date-picker', 'end_date')]
)
def update_graphs(active_start_date, active_end_date,
                  engaged_start_date, engaged_end_date,
                  status_start_date, status_end_date,
                  user_id, user_start_date, user_end_date):
    
    # Total Active Users Query
    total_active_users_query = """
    SELECT 
        COUNT(DISTINCT masked_from_addr) AS total_active_users
    FROM public.dim_messages
    WHERE direction = 'inbound';
    """
    total_active_users_df = pd.read_sql(total_active_users_query, engine)
    total_active_users = total_active_users_df['total_active_users'].iloc[0]

    # Total Engaged Users Query
    total_engaged_users_query = """
    SELECT 
        COUNT(DISTINCT masked_addressees) AS total_engaged_users
    FROM public.dim_messages
    WHERE last_status = 'read'
    AND last_status_timestamp >= inserted_at 
    AND last_status_timestamp < inserted_at + INTERVAL '7 days';
    """
    total_engaged_users_df = pd.read_sql(total_engaged_users_query, engine)
    total_engaged_users = total_engaged_users_df['total_engaged_users'].iloc[0]

    # Active Users Query
    active_users_query = """
    SELECT 
        DATE_TRUNC('day', inserted_at) AS day, 
        COUNT(DISTINCT masked_from_addr) AS active_users
    FROM public.dim_messages
    WHERE direction = 'inbound'
      AND inserted_at >= %s
      AND inserted_at < %s
    GROUP BY day
    ORDER BY day;
    """
    active_users_df = pd.read_sql(active_users_query, engine, params=(active_start_date, active_end_date))
    active_users_fig = px.line(active_users_df, x='day', y='active_users', title='Active Users')

    # Engaged Users Query
    engaged_users_query = """
    SELECT 
        DATE_TRUNC('day', last_status_timestamp) AS day, 
        COUNT(DISTINCT masked_addressees) AS engaged_users
    FROM public.dim_messages
    WHERE last_status = 'read' 
      AND last_status_timestamp >= inserted_at 
      AND last_status_timestamp < inserted_at + INTERVAL '7 days'
      AND last_status_timestamp >= %s
      AND last_status_timestamp < %s
    GROUP BY day
    ORDER BY day;
    """
    engaged_users_df = pd.read_sql(engaged_users_query, engine, params=(engaged_start_date, engaged_end_date))
    engaged_users_fig = px.line(engaged_users_df, x='day', y='engaged_users', title='Engaged Users')

    # Status Summary Query
    status_summary_query = """
    SELECT 
        last_status, 
        COUNT(*) AS count
    FROM public.dim_messages
    WHERE inserted_at >= %s
      AND inserted_at < %s
    GROUP BY last_status
    ORDER BY last_status;
    """
    status_summary_df = pd.read_sql(status_summary_query, engine, params=(status_start_date, status_end_date))
    status_summary_fig = px.bar(status_summary_df, x='last_status', y='count', title='Status Distribution of Messages')

    # User Messages Query
    if user_id:
        user_messages_query = """
        SELECT 
            m.id,
            m.message_type,
            m.content,
            m.direction,
            m.external_timestamp,
            m.masked_from_addr,
            m.is_deleted,
            m.last_status,
            m.last_status_timestamp,
            m.inserted_at,
            m.updated_at
        FROM public.dim_messages m
        WHERE (masked_from_addr = %s OR masked_addressees = %s)
          AND inserted_at >= %s
          AND inserted_at < %s
        ORDER BY inserted_at;
        """
        user_messages_df = pd.read_sql(user_messages_query, engine, params=(user_id, user_id, user_start_date, user_end_date))
        user_messages_fig = px.scatter(user_messages_df, x='inserted_at', y='last_status', color='direction', 
                                       title=f'Messages for User {user_id}', hover_data=['content', 'message_type'])
    else:
        user_messages_fig = px.scatter(title='No User ID Provided')

    return total_active_users, total_engaged_users, active_users_fig, engaged_users_fig, status_summary_fig, user_messages_fig

if __name__ == '__main__':
    app.run_server(debug=True)
